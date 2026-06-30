SET date_time_input_format = 'basic';
SELECT a FROM format(TSV, 'a DateTime64(2, \'UTC\')',
$$1234.5
3333.77
2025.08.31
99.1
23.9
2025-08-31 13:45:30
$$) ORDER BY a;
-- A bare 4-digit integer is ambiguous with a year and is rejected in both the optimistic and fallback paths
SELECT toDateTime64('1234x', 3, 'UTC'); -- { serverError CANNOT_PARSE_DATETIME, CANNOT_PARSE_TEXT }

-- Regression: fallback path (buffer < 19 bytes from field start).
-- A decimal epoch followed by another field must not consume the neighbour value.
SELECT b, c FROM format(CSV, 'b DateTime64(2, UTC), c Bool', $$1234.5,1$$);
SELECT b, c FROM format(CSV, 'b DateTime64(2, UTC), c Bool', $$0.0,true$$);

-- Regression: plain DateTime must still reject short (1-4 digit) positive timestamps
-- when the optimistic path is active (buffer >= 19 bytes from the field).
SELECT a FROM format(TSV, 'a DateTime, b String', $$1234	this_is_long_enough_padding$$); -- { serverError CANNOT_PARSE_DATETIME }
