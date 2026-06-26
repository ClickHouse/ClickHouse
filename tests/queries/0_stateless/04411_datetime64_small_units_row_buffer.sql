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
