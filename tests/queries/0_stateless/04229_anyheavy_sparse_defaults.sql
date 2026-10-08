-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104432
-- `anyHeavy` used to ignore default values that came from the sparse-column
-- code path because the `addManyDefaults` override hardcoded length=0.

DROP TABLE IF EXISTS t_anyheavy_sparse;

CREATE TABLE t_anyheavy_sparse (x UInt64) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

-- A part dominated by the default value (0), and a smaller part with a different value.
-- The heavy hitter must be 0, not 1.
INSERT INTO t_anyheavy_sparse SELECT 0 FROM numbers(800);
INSERT INTO t_anyheavy_sparse SELECT 1 FROM numbers(200);

SELECT anyHeavy(x) FROM t_anyheavy_sparse SETTINGS max_threads = 1;

DROP TABLE t_anyheavy_sparse;
