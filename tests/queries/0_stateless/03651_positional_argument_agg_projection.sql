DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY a;

ALTER TABLE test
    ADD PROJECTION test_projection
    (
        SELECT
            0 AS bug,
            max(a)
        GROUP BY bug
    );

DROP TABLE test;
