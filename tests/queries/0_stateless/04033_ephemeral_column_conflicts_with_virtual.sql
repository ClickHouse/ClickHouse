-- User EPHEMERAL columns must not shadow virtual columns of the same name,
-- because EPHEMERAL columns have no physical storage and cannot provide data on read.
-- This would cause a type mismatch between the Block header and the actual column data.

CREATE TABLE test_ephemeral_virtual_conflict
(
    `key` UInt32,
    `_part_offset` EPHEMERAL 0
)
ENGINE = MergeTree
ORDER BY key; -- { serverError ILLEGAL_COLUMN }

-- DEFAULT columns with the same name as virtual columns should still work
-- (they have physical storage and properly shadow the virtual column).
CREATE TABLE test_default_virtual_override
(
    `key` UInt32,
    `_part_offset` DEFAULT 0
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO test_default_virtual_override (key) SELECT number FROM numbers(10);

SELECT key, _part_offset FROM test_default_virtual_override ORDER BY key;

DROP TABLE test_default_virtual_override;
