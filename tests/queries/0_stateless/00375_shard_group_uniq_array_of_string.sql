-- Tags: shard, long

DROP TABLE IF EXISTS group_uniq_str;
CREATE TABLE group_uniq_str ENGINE = Memory AS SELECT number % 10 as id, toString(intDiv((number%10000), 10)) as v FROM system.numbers LIMIT 10000000;

INSERT INTO group_uniq_str SELECT 2 as id, toString(number % 100) as v FROM system.numbers LIMIT 1000000;
INSERT INTO group_uniq_str SELECT 5 as id, toString(number % 100) as v FROM system.numbers LIMIT 10000000;

SELECT length(groupUniqArray(v)) FROM group_uniq_str GROUP BY id ORDER BY id;
SELECT length(groupUniqArray(v)) FROM remote('127.0.0.{2,3,4,5}', currentDatabase(), 'group_uniq_str') GROUP BY id ORDER BY id SETTINGS max_rows_to_read = '100M';
SELECT length(groupUniqArray(10)(v)) FROM group_uniq_str GROUP BY id ORDER BY id;
SELECT length(groupUniqArray(10000)(v)) FROM group_uniq_str GROUP BY id ORDER BY id;

DROP TABLE IF EXISTS group_uniq_str;
