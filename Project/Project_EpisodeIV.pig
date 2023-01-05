-- Load data from HDFS
inputFile = LOAD 'hdfs:///user/ksandeep4/inputs/episodeIV_dialouges.txt' USING PigStorage('\t') AS ( name:chararray,line:chararray);
-- Filter out first 2 lines
inputFileRanked = RANK inputFile;
onlyDialouges = FILTER inputFileRanked BY ( rank_inputFile > 2);
-- Group by Name
groupByName = GROUP onlyDialouges BY name;
-- Count the no. of lines for each character
names =  FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
nameOrdered = ORDER names BY no_of_lines DESC;
-- Store Result in HDFS
STORE nameOrdered INTO 'hdfs:///user/ksandeep4/outputs/episodeIVoutput' USING PigStorage('\t');