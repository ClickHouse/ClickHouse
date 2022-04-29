DROP TABLE IF EXISTS test_distruted;
DROP TABLE IF EXISTS test_local;

CREATE TABLE test_local (text String, text2 String) ENGINE = MergeTree() ORDER BY text;
CREATE TABLE test_distruted (text String, text2 String) ENGINE =  Distributed('test_shard_localhost', currentDatabase(), test_local);
INSERT INTO test_distruted SELECT randomString(100) AS text, randomString(100) AS text2 FROM system.numbers LIMIT 1;

SET joined_subquery_requires_alias = 0;

SELECT COUNT() AS count
FROM test_distruted
INNER JOIN
(
    SELECT text
    FROM test_distruted
    WHERE (text ILIKE '%text-for-search%') AND (text2 ILIKE '%text-for-search%')
) USING (text)
WHERE (text ILIKE '%text-for-search%') AND (text2 ILIKE '%text-for-search%')
;

DROP TABLE IF EXISTS test_distruted;
DROP TABLE IF EXISTS test_local;
