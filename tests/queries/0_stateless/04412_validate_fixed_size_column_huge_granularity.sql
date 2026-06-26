-- Tags: no-replicated-database, no-shared-merge-tree

-- Regression test for an AST fuzzer finding (STID 1367-1ab3).
-- A non-adaptive MergeTree (`index_granularity_bytes = 0`) created with an absurd
-- `index_granularity` (close to the unsigned 64-bit range; the setting is only
-- rejected when < 1) used to abort the server in debug builds with
-- `Logical error: Too large size (...) passed to allocator`.
-- The debug-only validator `MergeTreeDataPartWriterWide::validateColumnOfFixedSize`
-- re-reads each granule via `deserializeBinaryBulk` using the per-mark granularity
-- as the row count. For such a table that count is the huge `index_granularity`,
-- so the column was eagerly resized to ~2^63 elements and tripped the allocator
-- overflow guard before reaching EOF.

DROP TABLE IF EXISTS t_validate_huge_granularity;

-- min_rows_for_wide_part / min_bytes_for_wide_part must be 0 because tables with
-- non-adaptive granularity can only store wide parts; otherwise the server emits
-- a startup warning that turns the test red on the CI test runner.
CREATE TABLE t_validate_huge_granularity (x UInt8)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS index_granularity_bytes = 0, index_granularity = 9223372036854775933, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_validate_huge_granularity SELECT number FROM numbers(100000);

SELECT count(), sum(x) FROM t_validate_huge_granularity;

DROP TABLE t_validate_huge_granularity;
