SET enable_analyzer = 1;

SELECT
    arrayJoin(arrayMap(tag -> (map('a', 'value1', 'b', 'value2')[tag]), ['a', 'b'])) AS path,
    path IN ['Uncategorized'] AS in_category
FROM remote('127.0.0.{1,2}', system.one);
