-- `x <=> y` must agree with `x = y` for non-NULL values, whatever wrapping the operands carry.
-- Each row prints `<=>`, `=` and `IS DISTINCT FROM`: for non-NULL operands column 2 must equal
-- column 3, and column 4 must be its negation.

SELECT '-- scalar Float64';
SELECT 'plain',                 nan <=> nan,                                                 nan = nan,                                             nan IS DISTINCT FROM nan;
SELECT 'nullable',              toNullable(nan) <=> toNullable(nan),                         toNullable(nan) = toNullable(nan),                     toNullable(nan) IS DISTINCT FROM toNullable(nan);
SELECT 'nullable/plain',        toNullable(nan) <=> nan,                                     toNullable(nan) = nan,                                 toNullable(nan) IS DISTINCT FROM nan;
SELECT 'plain/nullable',        nan <=> toNullable(nan),                                     nan = toNullable(nan),                                 nan IS DISTINCT FROM toNullable(nan);
SELECT 'materialized',          materialize(nan) <=> materialize(nan),                       materialize(nan) = materialize(nan),                   materialize(nan) IS DISTINCT FROM materialize(nan);
SELECT 'materialized nullable', materialize(toNullable(nan)) <=> materialize(toNullable(nan)), materialize(toNullable(nan)) = materialize(toNullable(nan)), materialize(toNullable(nan)) IS DISTINCT FROM materialize(toNullable(nan));

SELECT '-- narrower floats and mixed widths';
SELECT 'f32',            toFloat32('nan') <=> toFloat32('nan'),                         toFloat32('nan') = toFloat32('nan'),                         toFloat32('nan') IS DISTINCT FROM toFloat32('nan');
SELECT 'f32 nullable',   toNullable(toFloat32('nan')) <=> toNullable(toFloat32('nan')),  toNullable(toFloat32('nan')) = toNullable(toFloat32('nan')), toNullable(toFloat32('nan')) IS DISTINCT FROM toNullable(toFloat32('nan'));
SELECT 'bf16 nullable',  toNullable(toBFloat16('nan')) <=> toNullable(toBFloat16('nan')), toNullable(toBFloat16('nan')) = toNullable(toBFloat16('nan')), toNullable(toBFloat16('nan')) IS DISTINCT FROM toNullable(toBFloat16('nan'));
SELECT 'f32 vs f64',     toFloat32('nan') <=> toFloat64('nan'),                          toFloat32('nan') = toFloat64('nan'),                         toFloat32('nan') IS DISTINCT FROM toFloat64('nan');
SELECT 'f32 vs f64 nul', toNullable(toFloat32('nan')) <=> toNullable(toFloat64('nan')),  toNullable(toFloat32('nan')) = toNullable(toFloat64('nan')), toNullable(toFloat32('nan')) IS DISTINCT FROM toNullable(toFloat64('nan'));

SELECT '-- LowCardinality';
SELECT 'lc',          toLowCardinality(nan) <=> toLowCardinality(nan),                         toLowCardinality(nan) = toLowCardinality(nan),                         toLowCardinality(nan) IS DISTINCT FROM toLowCardinality(nan);
SELECT 'lc nullable', toLowCardinality(toNullable(nan)) <=> toLowCardinality(toNullable(nan)), toLowCardinality(toNullable(nan)) = toLowCardinality(toNullable(nan)), toLowCardinality(toNullable(nan)) IS DISTINCT FROM toLowCardinality(toNullable(nan));

SELECT '-- Tuple';
SELECT 'tuple',                (nan, 1) <=> (nan, 1),                                                   (nan, 1) = (nan, 1),                                                 (nan, 1) IS DISTINCT FROM (nan, 1);
SELECT 'nullable element',     (toNullable(nan), 1) <=> (toNullable(nan), 1),                            (toNullable(nan), 1) = (toNullable(nan), 1),                          (toNullable(nan), 1) IS DISTINCT FROM (toNullable(nan), 1);
SELECT 'nullable tuple',       toNullable((nan, 1)) <=> toNullable((nan, 1)),                            toNullable((nan, 1)) = toNullable((nan, 1)),                          toNullable((nan, 1)) IS DISTINCT FROM toNullable((nan, 1));
SELECT 'nullable tuple mat',   materialize(toNullable((nan, 1))) <=> materialize(toNullable((nan, 1))),  materialize(toNullable((nan, 1))) = materialize(toNullable((nan, 1))), materialize(toNullable((nan, 1))) IS DISTINCT FROM materialize(toNullable((nan, 1)));

SELECT '-- signed NaN, signed zero, infinity';
SELECT 'nan vs -nan',          nan <=> -nan,                                        nan = -nan,                                    nan IS DISTINCT FROM -nan;
SELECT 'nan vs -nan nullable', toNullable(nan) <=> toNullable(-nan),                toNullable(nan) = toNullable(-nan),            toNullable(nan) IS DISTINCT FROM toNullable(-nan);
SELECT '-0.0 vs 0.0',          toFloat64(-0.0) <=> toFloat64(0.0),                  toFloat64(-0.0) = toFloat64(0.0),              toFloat64(-0.0) IS DISTINCT FROM toFloat64(0.0);
SELECT '-0.0 vs 0.0 nullable', toNullable(toFloat64(-0.0)) <=> toNullable(toFloat64(0.0)), toNullable(toFloat64(-0.0)) = toNullable(toFloat64(0.0)), toNullable(toFloat64(-0.0)) IS DISTINCT FROM toNullable(toFloat64(0.0));
SELECT 'inf',                  inf <=> inf,                                         inf = inf,                                     inf IS DISTINCT FROM inf;
SELECT 'inf nullable',         toNullable(inf) <=> toNullable(inf),                 toNullable(inf) = toNullable(inf),             toNullable(inf) IS DISTINCT FROM toNullable(inf);
SELECT 'inf vs -inf nullable', toNullable(inf) <=> toNullable(-inf),                toNullable(inf) = toNullable(-inf),            toNullable(inf) IS DISTINCT FROM toNullable(-inf);
SELECT 'nan vs inf nullable',  toNullable(nan) <=> toNullable(inf),                 toNullable(nan) = toNullable(inf),             toNullable(nan) IS DISTINCT FROM toNullable(inf);
SELECT 'nan vs 1 nullable',    toNullable(nan) <=> toNullable(toFloat64(1)),        toNullable(nan) = toNullable(toFloat64(1)),    toNullable(nan) IS DISTINCT FROM toNullable(toFloat64(1));

SELECT '-- NULL handling is unchanged';
SELECT 'null/null',       NULL <=> NULL,                       NULL = NULL,                       NULL IS DISTINCT FROM NULL;
SELECT 'nan/null',        toNullable(nan) <=> NULL,            toNullable(nan) = NULL,            toNullable(nan) IS DISTINCT FROM NULL;
SELECT 'null/nan',        NULL <=> toNullable(nan),            NULL = toNullable(nan),            NULL IS DISTINCT FROM toNullable(nan);
SELECT 'nan/null plain',  nan <=> NULL,                        nan = NULL,                        nan IS DISTINCT FROM NULL;
SELECT 'equal nullable',  toNullable(1) <=> toNullable(1),     toNullable(1) = toNullable(1),     toNullable(1) IS DISTINCT FROM toNullable(1);
SELECT 'differ nullable', toNullable(1) <=> toNullable(2),     toNullable(1) = toNullable(2),     toNullable(1) IS DISTINCT FROM toNullable(2);
SELECT 'int/null',        toNullable(1) <=> NULL,              toNullable(1) = NULL,              toNullable(1) IS DISTINCT FROM NULL;
SELECT 'string nullable', toNullable('a') <=> toNullable('a'), toNullable('a') = toNullable('a'), toNullable('a') IS DISTINCT FROM toNullable('a');

SELECT '-- Nullable operands with no least common supertype are unchanged';
SELECT 'u64/i64 equal',  toNullable(toUInt64(1)) <=> toNullable(toInt64(1));
SELECT 'u64/i64 differ', toNullable(toUInt64(18446744073709551615)) <=> toNullable(toInt64(-1));
SELECT 'u64/i64 null',   toNullable(toUInt64(1)) <=> CAST(NULL AS Nullable(Int64));

SELECT '-- Array and Map track `=`, which reports NaN as equal for containers';
SELECT 'array', [nan] <=> [nan],                 [nan] = [nan],                 [nan] IS DISTINCT FROM [nan];
SELECT 'map',   map('a', nan) <=> map('a', nan), map('a', nan) = map('a', nan), map('a', nan) IS DISTINCT FROM map('a', nan);

SELECT '-- table columns';
DROP TABLE IF EXISTS t_null_safe_nan;
CREATE TABLE t_null_safe_nan (key UInt8, a Float64, b Nullable(Float64)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_null_safe_nan VALUES (1, nan, nan), (2, 1, 1), (3, nan, NULL);

SELECT key, a <=> a, b <=> b, a <=> b, a = a, b = b, a = b FROM t_null_safe_nan ORDER BY key;
SELECT 'rows matching a <=> nan', count() FROM t_null_safe_nan WHERE a <=> nan;
SELECT 'rows matching b <=> nan', count() FROM t_null_safe_nan WHERE b <=> nan;
SELECT 'rows matching b <=> NULL', count() FROM t_null_safe_nan WHERE b <=> NULL;

DROP TABLE t_null_safe_nan;
