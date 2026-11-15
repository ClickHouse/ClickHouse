-- The LowCardinality inference is opt-in: constructing a dictionary per block can
-- be slower than returning `String` when no downstream operation reuses it.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/25272

-- The setting is randomized by the test runner, so the default is asserted through
-- `system.settings` rather than through the session value.
SELECT `default` FROM system.settings WHERE name = 'optimize_if_transform_const_strings_to_lowcardinality';

SET optimize_if_transform_const_strings_to_lowcardinality = 0;

SELECT toTypeName(if(number % 2 = 0, 'a', 'b')) FROM numbers(1);
SELECT toTypeName(multiIf(number % 2 = 0, 'a', 'b')) FROM numbers(1);
SELECT toTypeName(transform(number, [0], ['a'], 'b')) FROM numbers(1);
