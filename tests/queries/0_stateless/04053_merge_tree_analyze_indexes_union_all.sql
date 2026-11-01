-- Regression test: mergeTreeAnalyzeIndexes with UNION ALL subquery referencing
-- the same table caused "Column identifier key is already registered" exception.

DROP TABLE IF EXISTS data;

CREATE TABLE data
(
    `key` UInt64
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO data SELECT * FROM numbers(100);

SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, (key NOT IN (
    SELECT key
    FROM data
    UNION ALL
    SELECT key
    FROM data
)));

DROP TABLE data;
