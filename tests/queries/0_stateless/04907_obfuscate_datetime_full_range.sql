-- `DateTime` uses the full `UInt32` timestamp range. Its continuity deltas therefore need
-- `Int64`: timestamps after the 2038 boundary must not turn into negative `Int32` values.
SELECT toDate(x)
FROM obfuscate(
    SELECT toDateTime('2038-01-19 03:14:08', 'UTC') + toIntervalSecond(number * 20650752) AS x
    FROM numbers(1))
LIMIT 1
SETTINGS obfuscate_seed = 'obfuscate_datetime_full_range';
