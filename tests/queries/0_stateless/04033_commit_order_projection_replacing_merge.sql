-- Tags: no-parallel-replicas

set enable_analyzer = 1;

DROP TABLE IF EXISTS mt_replacing_test SYNC;

CREATE TABLE mt_replacing_test(
    a UInt64,
    ver UInt64,
    PROJECTION _commit_order (
        SELECT *, _block_number, _block_offset
        ORDER BY _block_number, _block_offset
    )
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY a
SETTINGS enable_block_number_column=1, enable_block_offset_column=1, allow_commit_order_projection=1, deduplicate_merge_projection_mode='rebuild';

INSERT INTO mt_replacing_test VALUES (1, 1), (2, 1), (3, 1);
INSERT INTO mt_replacing_test VALUES (2, 2), (3, 2);  -- newer versions for a=2, a=3
OPTIMIZE TABLE mt_replacing_test FINAL;

-- After merge, only latest versions survive. Their _block_number should be from the insert that wrote them.
SELECT 'replacing merge tree after merge';
SELECT a, ver, _block_number, _block_offset FROM mt_replacing_test ORDER BY a;

-- Projection should have same data
SELECT 'replacing - projection data';
SELECT a, ver, _block_number, _block_offset
FROM mergeTreeProjection(currentDatabase(), 'mt_replacing_test', '_commit_order')
settings max_threads=1;

DROP TABLE mt_replacing_test SYNC;
