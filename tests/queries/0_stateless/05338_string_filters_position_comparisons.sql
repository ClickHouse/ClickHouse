-- A comparison of `position` with a constant may be pushed into the scan by `apply_string_filters_during_scan`
-- only if it rejects the zero result. Every query in a group must return the same result as its first line.

-- The string filters are not applied when the read columns may be written to the columns cache (randomized in tests).
DROP TABLE IF EXISTS t_string_filter_position;
SET use_columns_cache = 0;

DROP TABLE IF EXISTS t_string_filter_position;

CREATE TABLE t_string_filter_position (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_position
SELECT number, if(number % 10 = 0, 'value with needle ' || toString(number), 'ordinary value ' || toString(number))
FROM numbers(10000);

SELECT 'all rows pass';
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') != 5 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') >= 0 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') > -1 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') >= toDecimal32(0, 1) SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, '') > 0 SETTINGS apply_string_filters_during_scan = 1;

SELECT 'rows without the needle pass';
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') = 0 SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') < 5 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') <= 0 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') < 0.5 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE 5 > position(s, 'needle') SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE 0 >= position(s, 'needle') SETTINGS apply_string_filters_during_scan = 1;

SELECT 'only rows with the needle pass';
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') > 0 SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') != 0 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') >= 1 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE position(s, 'needle') > 0.5 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE 0 < position(s, 'needle') SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_position PREWHERE 1 <= position(s, 'needle') SETTINGS apply_string_filters_during_scan = 1;

DROP TABLE t_string_filter_position;
