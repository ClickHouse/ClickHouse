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
ORDER BY ()
SETTINGS index_granularity_bytes = 10485760, index_granularity = 8192;

-- Insert enough rows so that future projection materialization test will trigger level 1 merge
INSERT INTO test SELECT number * 3, rand() FROM numbers(360000);
INSERT INTO test SELECT number * 3 + 1, rand() FROM numbers(360000);
INSERT INTO test SELECT number * 3 + 2, rand() FROM numbers(360000);
SELECT sum(l._part_offset = r._parent_part_offset) FROM test l JOIN mergeTreeProjection(currentDatabase(), test, p) r USING (a) SETTINGS enable_analyzer = 1;

OPTIMIZE TABLE test FINAL;
SELECT sum(l._part_offset = r._parent_part_offset) FROM test l JOIN mergeTreeProjection(currentDatabase(), test, p) r USING (a) SETTINGS enable_analyzer = 1;

ALTER TABLE test ADD PROJECTION p2 (SELECT a, b, _part_offset ORDER BY b);
ALTER TABLE test MATERIALIZE PROJECTION p2 SETTINGS mutations_sync = 2;
SELECT sum(l._part_offset = r._parent_part_offset) FROM test l JOIN mergeTreeProjection(currentDatabase(), test, p2) r USING (a) SETTINGS enable_analyzer = 1;

DROP TABLE test;
