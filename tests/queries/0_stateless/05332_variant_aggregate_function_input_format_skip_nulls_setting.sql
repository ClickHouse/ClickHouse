-- The states built from raw values (`aggregate_function_input_format` = 'value' / 'array') for an
-- `AggregateFunction(f, Variant(...))` column aggregate fresh data, so they must follow the current
-- `aggregate_functions_skip_variant_nulls` setting, although the declared column type is resolved independently of it.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_input_format;
CREATE TABLE t_variant_input_format
(
    c AggregateFunction(count, Variant(UInt64, String)),
    a AggregateFunction(groupArray, Variant(UInt64, String)),
    arr Array(AggregateFunction(count, Variant(UInt64, String)))
) ENGINE = Memory;

SET aggregate_function_input_format = 'value';

SET aggregate_functions_skip_variant_nulls = 0;
INSERT INTO t_variant_input_format FORMAT Values (NULL, NULL, [NULL, 1]), (1, 1, [NULL]), (NULL, 'a', []);
SELECT 'value, skip 0', countMerge(c), arraySort(x -> toString(x), groupArrayMerge(a)) FROM t_variant_input_format;
SELECT 'value, skip 0, nested', arrayMap(x -> finalizeAggregation(x), arr) FROM t_variant_input_format ORDER BY length(arr), toString(arr);
TRUNCATE TABLE t_variant_input_format;

SET aggregate_functions_skip_variant_nulls = 1;
INSERT INTO t_variant_input_format FORMAT Values (NULL, NULL, [NULL, 1]), (1, 1, [NULL]), (NULL, 'a', []);
SELECT 'value, skip 1', countMerge(c), arraySort(x -> toString(x), groupArrayMerge(a)) FROM t_variant_input_format;
SELECT 'value, skip 1, nested', arrayMap(x -> finalizeAggregation(x), arr) FROM t_variant_input_format ORDER BY length(arr), toString(arr);
TRUNCATE TABLE t_variant_input_format;

SET aggregate_function_input_format = 'array';

SET aggregate_functions_skip_variant_nulls = 0;
INSERT INTO t_variant_input_format FORMAT Values ([NULL, 1, NULL], [NULL, 'a'], []);
SELECT 'array, skip 0', countMerge(c), arraySort(x -> toString(x), groupArrayMerge(a)) FROM t_variant_input_format;
TRUNCATE TABLE t_variant_input_format;

SET aggregate_functions_skip_variant_nulls = 1;
INSERT INTO t_variant_input_format FORMAT Values ([NULL, 1, NULL], [NULL, 'a'], []);
SELECT 'array, skip 1', countMerge(c), arraySort(x -> toString(x), groupArrayMerge(a)) FROM t_variant_input_format;

DROP TABLE t_variant_input_format;
