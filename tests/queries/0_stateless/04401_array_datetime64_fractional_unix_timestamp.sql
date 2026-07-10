-- Container (array/tuple/map) and JSON DateTime64 elements must accept the
-- unquoted fractional unix timestamp form (e.g. 1783585473.954), same as scalar columns.
-- A bare integer stays a scaled tick count (backward compatible).
SET session_timezone = 'UTC';

-- Reported bug: CSV Array element as unquoted fractional unix timestamp used to fail
-- with CANNOT_READ_ARRAY_FROM_TEXT.
SELECT 'csv_array_frac', * FROM format(CSV, 'x Array(DateTime64(3))', '"[1783585473.954,1783585473.954]"');

-- Fractional form parses to the same value as the equivalent integer-ticks form.
SELECT 'csv_array_frac_eq_ticks',
    (SELECT x FROM format(CSV, 'x Array(DateTime64(3))', '"[1783585473.954]"'))
  = (SELECT x FROM format(CSV, 'x Array(DateTime64(3))', '"[1783585473954]"'));

-- Same fix applies to JSON, TSV, Values, and Map containers.
SELECT 'json_array_frac', * FROM format(JSONEachRow, 'x Array(DateTime64(3))', '{"x":[1783585473.954]}');
SELECT 'tsv_array_frac', * FROM format(TSV, 'x Array(DateTime64(3))', '[1783585473.954]');
SELECT 'values_array_frac', [1783585473.954]::Array(DateTime64(3)) = [1783585473954]::Array(DateTime64(3));
SELECT 'map_frac', * FROM format(CSV, 'x Map(String, DateTime64(3))', '"{''k'':1783585473.954}"');

-- Nullable element.
SELECT 'nullable_frac', [1783585473.954, NULL]::Array(Nullable(DateTime64(3)));

-- Negative fractional timestamp matches the scalar representation.
SELECT 'neg_frac', [-1390214744.877]::Array(DateTime64(3));

-- Negative sub-second values whose whole part is zero must keep the sign.
-- readIntText normalises "-0" to 0, so the sign has to be restored explicitly;
-- e.g. -0.123 s is 123 ms before the epoch: 1969-12-31 23:59:59.877.
SELECT 'csv_neg_zero_frac', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-0.123]"');
SELECT 'json_neg_zero_frac', toString(x[1]) FROM format(JSONEachRow, 'x Array(DateTime64(3))', '{"x":[-0.877]}');
SELECT 'csv_neg_one_frac', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-1.123]"');

-- The scalar readDateTime64Text path shares the same sign restoration, so a scalar
-- -0.xxx must parse identically to the container element (regression for the earlier
-- scalar/container divergence). Basic input format exercises readDateTime64Text directly.
SELECT 'scalar_neg_zero_frac', toString(x) FROM format(CSV, 'x DateTime64(3)', '-0.123') SETTINGS date_time_input_format = 'basic';
SELECT 'scalar_eq_container_neg_zero',
    (SELECT x FROM format(CSV, 'x DateTime64(3)', '-0.123') SETTINGS date_time_input_format = 'basic')
  = (SELECT x[1] FROM format(CSV, 'x Array(DateTime64(3))', '"[-0.123]"'));
SELECT 'scalar_eq_container_neg_one',
    (SELECT x FROM format(CSV, 'x DateTime64(3)', '-1.123') SETTINGS date_time_input_format = 'basic')
  = (SELECT x[1] FROM format(CSV, 'x Array(DateTime64(3))', '"[-1.123]"'));

-- Bare shorthand `-.123` (sign directly followed by the decimal point, implied zero whole part):
-- the scalar path already accepts it, so the container/JSON path must too, and both must agree.
-- readIntText rejects a lone sign without digits, so the container helper special-cases it.
SELECT 'csv_neg_shorthand', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-.123]"');
SELECT 'json_neg_shorthand', toString(x[1]) FROM format(JSONEachRow, 'x Array(DateTime64(3))', '{"x":[-.877]}');
SELECT 'pos_shorthand', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[.123]"');
SELECT 'scalar_eq_container_neg_shorthand',
    (SELECT x FROM format(CSV, 'x DateTime64(3)', '-.123') SETTINGS date_time_input_format = 'basic')
  = (SELECT x[1] FROM format(CSV, 'x Array(DateTime64(3))', '"[-.123]"'));

-- Leading '+' must be rejected in the container/JSON path, matching scalar DateTime64 basic
-- parsing (which rejects it) and the pre-PR container behavior. readIntText would silently
-- accept it, so the helper filters it explicitly. Both scalar and container must reject.
SELECT 'container_plus_frac', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+1783585473.954]"'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'container_plus_zero_frac', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+0.123]"'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'container_plus_shorthand', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+.123]"'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'container_plus_bare_int', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+1783585473954]"'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'scalar_plus_frac', toString(x) FROM format(CSV, 'x DateTime64(3)', '+1783585473.954') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_DATETIME }

-- Fraction is truncated / padded to the column scale.
SELECT 'scale0', [1783585473.954]::Array(DateTime64(0));
SELECT 'scale6_extra', [1783585473.954321987]::Array(DateTime64(6));
SELECT 'scale3_short', [1783585473.95]::Array(DateTime64(3));

-- Backward compatibility: a bare integer is still a scaled tick count, not seconds.
SELECT 'bc_bare_int_ticks', [1504193808]::Array(DateTime64(3));
SELECT 'bc_neg_bare_int_ticks', [-1390214744]::Array(DateTime64(3));
