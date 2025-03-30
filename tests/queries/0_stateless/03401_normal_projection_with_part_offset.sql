-- { echo ON }

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `a` Int32,
    `b` Int32,
    PROJECTION p
    (
        SELECT
            a,
            b,
            _part_offset
        ORDER BY b
    )
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 1;

INSERT INTO test SELECT number * 3, rand() FROM numbers(10);
INSERT INTO test SELECT number * 3 + 1, rand() FROM numbers(10);
INSERT INTO test SELECT number * 3 + 2, rand() FROM numbers(10);
SELECT a, l._part_offset = r._part_offset FROM test l JOIN mergeTreeProjection(currentDatabase(), test, p) r USING (a) SETTINGS max_threads = 1;

OPTIMIZE TABLE test FINAL;
SELECT a, l._part_offset = r._part_offset FROM test l JOIN mergeTreeProjection(currentDatabase(), test, p) r USING (a) SETTINGS max_threads = 1;

DROP TABLE test;
