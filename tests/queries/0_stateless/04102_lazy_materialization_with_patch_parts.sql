-- Tags: no-shared-merge-tree
-- Test reproduces regression where for error
-- Code: 10. DB::Exception: Received from localhost:9000. DB::Exception: Not found column _block_number in block. There are only columns: : While executing MergeTreeSelect(pool: ReadPoolInOrder, algorithm: InOrder). (NOT_FOUND_COLUMN_IN_BLOCK)

SET enable_lightweight_update = 1;

CREATE TABLE t_lazy_patch (id UInt64, filt UInt64, lazy String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lazy_patch SELECT number, number % 10, repeat('a', number) FROM numbers(100);
INSERT INTO t_lazy_patch SELECT number + 100, number % 10, repeat('b', number + 100) FROM numbers(100);
UPDATE t_lazy_patch SET lazy = 'patched' WHERE filt < 3;

OPTIMIZE TABLE t_lazy_patch FINAL;

SELECT lazy FROM t_lazy_patch WHERE filt > 0 ORDER BY id LIMIT 5;
