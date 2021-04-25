DROP TABLE IF EXISTS makrdown;
CREATE TABLE markdown (id UInt32, name String, array Array(Int8)) ENGINE = Memory;
INSERT INTO markdown VALUES (1, 'name1', [1,2,3]), (2, 'name2', [4,5,6]), (3, 'name3', [7,8,9]);

SELECT * FROM markdown FORMAT Markdown;
DROP TABLE IF EXISTS markdown
