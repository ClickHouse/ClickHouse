-- Mutations that rewrite a Dynamic column (which has data-dependent substreams such as
-- `variant_discr`) interleaved with merges, on Wide parts. Regression coverage for the
-- mutation stream-accounting of dynamic-structure columns; CHECK TABLE validates that
-- every recorded substream of every part is present on disk.
-- See https://github.com/ClickHouse/ClickHouse/issues/107561

DROP TABLE IF EXISTS t_dyn_mut;
CREATE TABLE t_dyn_mut (id UInt64, s UInt64, y Dynamic(max_types=3))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_dyn_mut SELECT number, number, number::Int64 FROM numbers(1000);
INSERT INTO t_dyn_mut SELECT number, number, 's' || number FROM numbers(1000);
INSERT INTO t_dyn_mut SELECT number, number, number::Float64 FROM numbers(1000);
INSERT INTO t_dyn_mut SELECT number, number, [number] FROM numbers(1000);
OPTIMIZE TABLE t_dyn_mut FINAL;

-- Mutation that rewrites the Dynamic column.
ALTER TABLE t_dyn_mut UPDATE y = id::Decimal64(2) WHERE id % 2 = 0 SETTINGS mutations_sync = 2;
-- Mutation that does NOT touch the Dynamic column (its streams must be hardlinked intact).
ALTER TABLE t_dyn_mut UPDATE s = s + 1 WHERE id % 3 = 0 SETTINGS mutations_sync = 2;
-- Type change that re-decides the variant structure.
ALTER TABLE t_dyn_mut MODIFY COLUMN y Dynamic(max_types = 1) SETTINGS mutations_sync = 2;
INSERT INTO t_dyn_mut SELECT number, number, map(number, number) FROM numbers(1000);
OPTIMIZE TABLE t_dyn_mut FINAL;

SELECT count(), countIf(y IS NOT NULL) FROM t_dyn_mut;
SELECT dynamicType(y) AS t, count() FROM t_dyn_mut GROUP BY t ORDER BY t;
CHECK TABLE t_dyn_mut SETTINGS check_query_single_value_result = 1;

DROP TABLE t_dyn_mut;
