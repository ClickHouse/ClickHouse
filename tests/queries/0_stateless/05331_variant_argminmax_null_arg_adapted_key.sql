-- argMin / argMax skip a NULL in either argument. With a Variant value ("arg", accepted natively) and a Variant
-- comparison key (adapted to Nullable(supertype)), the resolution is mixed: only the key goes through the Variant
-- adapter, and the NULL rows of the natively kept "arg" must still be skipped rather than returned.

SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_argminmax_null_arg;
CREATE TABLE t_variant_argminmax_null_arg (arg Variant(String, UInt64), key Variant(UInt8, UInt64), g UInt8) ENGINE = Memory;
INSERT INTO t_variant_argminmax_null_arg VALUES (NULL, 10, 0), ('x', 5, 0), (NULL, 1, 0), (NULL, 7, 1), (42, NULL, 1), (NULL, NULL, 2);

SELECT argMax(arg, key), argMin(arg, key) FROM t_variant_argminmax_null_arg;
SELECT g, argMax(arg, key), argMin(arg, key) FROM t_variant_argminmax_null_arg GROUP BY g ORDER BY g;
SELECT argMaxIf(arg, key, g = 0), argMinIf(arg, key, g = 0) FROM t_variant_argminmax_null_arg;
SELECT finalizeAggregation(argMaxState(arg, key)) FROM t_variant_argminmax_null_arg;
SELECT argMax(arg, key) OVER (), argMin(arg, key) OVER () FROM t_variant_argminmax_null_arg LIMIT 1;

-- With the skipping disabled, the NULL "arg" of the row with the largest key is returned, as before.
SELECT argMax(arg, key) FROM t_variant_argminmax_null_arg SETTINGS aggregate_functions_skip_variant_nulls = 0;

DROP TABLE t_variant_argminmax_null_arg;
