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
ENGINE = ReplacingMergeTree
ORDER BY a
SETTINGS index_granularity_bytes = 10485760, index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO test SELECT number * 3, rand() FROM numbers(100000);
INSERT INTO test SELECT number * 3 + 1, rand() FROM numbers(100000);
SELECT sum(l._part_offset = r._parent_part_offset) FROM test l JOIN mergeTreeProjection(currentDatabase(), test, p) r USING (a) SETTINGS enable_analyzer = 1;

OPTIMIZE TABLE test FINAL;

SELECT sum(l._part_offset = r._parent_part_offset) FROM test l JOIN mergeTreeProjection(currentDatabase(), test, p) r USING (a) SETTINGS enable_analyzer = 1;

DROP TABLE test;
