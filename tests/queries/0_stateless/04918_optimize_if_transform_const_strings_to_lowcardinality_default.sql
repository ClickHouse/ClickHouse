-- The LowCardinality inference is opt-in: constructing a dictionary per block can
-- be slower than returning `String` when no downstream operation reuses it.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/25272

SELECT toTypeName(if(number % 2 = 0, 'a', 'b')) FROM numbers(1);
SELECT toTypeName(multiIf(number % 2 = 0, 'a', 'b')) FROM numbers(1);
SELECT toTypeName(transform(number, [0], ['a'], 'b')) FROM numbers(1);
