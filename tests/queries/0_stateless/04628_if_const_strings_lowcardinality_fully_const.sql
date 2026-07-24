SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SET optimize_if_transform_strings_to_enum = 0;

SELECT 'Fully constant expressions stay String (no LowCardinality for constants)';
SELECT if(1, 'x', 'y') AS res, toTypeName(res);
-- Note: a typed NULL is used because with a bare `NULL` the old analyzer folds `if` with a constant
-- condition into the raw `NULL` literal, so `toTypeName` would differ between analyzers.
SELECT if(0, 'x', CAST(NULL AS Nullable(String))) AS res, toTypeName(res);
SELECT multiIf(0, 'a', 1, 'b', 'c') AS res, toTypeName(res);
SELECT multiIf(0, 'a', 1, NULL, 'c') AS res, toTypeName(res);
SELECT transform('US', ['US', 'DE'], ['United States', 'Germany'], 'Unknown') AS res, toTypeName(res);
SELECT transform('US', ['US', 'DE'], ['United States', 'Germany']) AS res, toTypeName(res);

SELECT 'Non-constant condition or input still gets LowCardinality';
SELECT if(number % 2 = 0, 'x', 'y') AS res, toTypeName(res) FROM numbers(2);
SELECT multiIf(number % 2 = 0, 'a', number = 1, 'b', 'c') AS res, toTypeName(res) FROM numbers(2);
SELECT transform(toString(number), ['0', '1'], ['zero', 'one'], 'other') AS res, toTypeName(res) FROM numbers(2);

SELECT 'Constant string consumers accept fully constant if/multiIf/transform results';
SELECT arrayReduce(if(1, 'sum', 'max'), [1, 2, 3]);
SELECT arrayReduce(multiIf(0, 'sum', 1, 'max', 'min'), [1, 2, 3]);
-- Note: `transform` is not tested with `arrayReduce` because its result is never a `ColumnConst`
-- (regardless of this optimization), so `arrayReduce(transform(...), ...)` fails even without it.
SELECT tupleElement(CAST((1, 'v'), 'Tuple(a UInt8, b String)'), if(1, 'b', 'a'));

DROP TABLE IF EXISTS join_04628;
CREATE TABLE join_04628 (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join_04628 VALUES (1, 'one');
SELECT joinGet(if(1, concat(currentDatabase(), '.join_04628'), ''), 'v', toUInt64(1));
DROP TABLE join_04628;
