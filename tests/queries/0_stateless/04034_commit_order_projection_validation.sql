-- Tags: no-random-merge-tree-settings, no-parallel-replicas

set enable_analyzer = 1;

-- Missing allow_commit_order_projection
CREATE TABLE mt_no_allow(a UInt64,
    PROJECTION p (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column=1, enable_block_offset_column=1; -- { serverError BAD_ARGUMENTS }

-- Missing enable_block_number_column
CREATE TABLE mt_no_bn(a UInt64,
    PROJECTION p (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_offset_column=1, allow_commit_order_projection=1; -- { serverError BAD_ARGUMENTS }

-- Missing enable_block_offset_column
CREATE TABLE mt_no_bo(a UInt64,
    PROJECTION p (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column=1, allow_commit_order_projection=1; -- { serverError BAD_ARGUMENTS }

SELECT 'ok';
