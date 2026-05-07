-- Tags: no-random-merge-tree-settings
-- Regression test for Bad cast from ColumnVector to ColumnLowCardinality crash (STID: 4054-600e)
-- The bug was in applyFunctionChainToColumn: when the key expression contains a CAST on a
-- LowCardinality column, the FunctionCast wrapper was built with LC argument types, but the
-- column was stripped of LC before being passed to the function.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_cast_key;

-- ORDER BY CAST(val, 'UInt64') where val is LowCardinality(UInt32)
-- creates a monotonic function chain with a FunctionCast that has LC argument type.
CREATE TABLE t_lc_cast_key (val LowCardinality(UInt32))
ENGINE = MergeTree() ORDER BY CAST(val, 'UInt64');

INSERT INTO t_lc_cast_key SELECT number % 100 FROM numbers(1000);

-- These queries trigger canConstantBeWrappedByMonotonicFunctions which calls
-- applyFunctionChainToColumn with the CAST function. Before the fix, this crashed
-- with "Bad cast from type DB::ColumnVector<unsigned int> to DB::ColumnLowCardinality".
SELECT count() FROM t_lc_cast_key WHERE val = 42;
SELECT count() FROM t_lc_cast_key WHERE val > 50;
SELECT count() FROM t_lc_cast_key WHERE val BETWEEN 10 AND 20;

DROP TABLE t_lc_cast_key;
