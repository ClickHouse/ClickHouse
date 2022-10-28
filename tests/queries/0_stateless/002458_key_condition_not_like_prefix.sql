CREATE TABLE data (str String) ENGINE=MergeTree ORDER BY str;
INSERT INTO data (str) SELECT 'aa' FROM numbers(100000);
INSERT INTO data (str) SELECT 'ba' FROM numbers(100000);
INSERT INTO data (str) SELECT 'ca' FROM numbers(100000);
SELECT count()FROM data WHERE str NOT LIKE 'a%' SETTINGS force_primary_key=1;
