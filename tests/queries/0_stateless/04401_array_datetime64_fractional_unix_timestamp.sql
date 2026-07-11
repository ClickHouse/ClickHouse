-- Container (array/tuple/map) and JSON DateTime64 elements must accept the
-- unquoted fractional unix timestamp form (e.g. 1783585473.954), same as scalar columns.
-- A bare integer stays a scaled tick count (backward compatible).
SET session_timezone = 'UTC';
-- CI randomizes date_time_input_format; the scalar DateTime64 rows below need the
-- basic reader (readDateTime64Text). A top-level SET is required: a per-query
-- SETTINGS clause on format() inside a scalar subquery does not reach the parallel
-- parsing reader, so best_effort would reject the fractional / -0.xxx forms there.
SET date_time_input_format = 'basic';

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
SELECT 'scalar_neg_zero_frac', toString(x) FROM format(CSV, 'x DateTime64(3)', '-0.123');
SELECT 'scalar_eq_container_neg_zero',
    (SELECT x FROM format(CSV, 'x DateTime64(3)', '-0.123'))
  = (SELECT x[1] FROM format(CSV, 'x Array(DateTime64(3))', '"[-0.123]"'));
SELECT 'scalar_eq_container_neg_one',
    (SELECT x FROM format(CSV, 'x DateTime64(3)', '-1.123'))
  = (SELECT x[1] FROM format(CSV, 'x Array(DateTime64(3))', '"[-1.123]"'));

-- Bare shorthand `-.123` (sign directly followed by the decimal point, implied zero whole part):
-- the scalar path already accepts it, so the container/JSON path must too, and both must agree.
-- readIntText rejects a lone sign without digits, so the container helper special-cases it.
SELECT 'csv_neg_shorthand', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-.123]"');
SELECT 'json_neg_shorthand', toString(x[1]) FROM format(JSONEachRow, 'x Array(DateTime64(3))', '{"x":[-.877]}');
SELECT 'pos_shorthand', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[.123]"');
SELECT 'scalar_eq_container_neg_shorthand',
    (SELECT x FROM format(CSV, 'x DateTime64(3)', '-.123'))
  = (SELECT x[1] FROM format(CSV, 'x Array(DateTime64(3))', '"[-.123]"'));

-- Leading '+' must be rejected in the container/JSON path, matching scalar DateTime64 basic
-- parsing (which rejects it) and the pre-PR container behavior. readIntText would silently
-- accept it, so the helper filters it explicitly. Both scalar and container must reject.
SELECT 'container_plus_frac', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+1783585473.954]"'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'container_plus_zero_frac', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+0.123]"'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'container_plus_shorthand', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+.123]"'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'container_plus_bare_int', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+1783585473954]"'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'scalar_plus_frac', toString(x) FROM format(CSV, 'x DateTime64(3)', '+1783585473.954'); -- { serverError CANNOT_PARSE_DATETIME }

-- The dotted fractional / sign / shorthand forms are a basic-parser feature. Under best_effort
-- (and best_effort_us) scalar and quoted-nested DateTime64 route through parseDateTime64BestEffort,
-- which rejects them, so the unquoted container/JSON element must reject them too: the effective
-- parser depends on date_time_input_format, not on quoting/nesting. A bare integer tick count still
-- parses under best_effort (unchanged, backward compatible).
SELECT 'be_scalar_dotted', toString(x) FROM format(CSV, 'x DateTime64(3)', '-0.123') SETTINGS date_time_input_format = 'best_effort'; -- { serverError CANNOT_PARSE_DATETIME }
SELECT 'be_unquoted_dotted', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-0.123]"') SETTINGS date_time_input_format = 'best_effort'; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }
SELECT 'be_unquoted_dot_pos', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[.123]"') SETTINGS date_time_input_format = 'best_effort'; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }
SELECT 'be_unquoted_frac', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[1783585473.954]"') SETTINGS date_time_input_format = 'best_effort'; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }
SELECT 'beus_unquoted_dotted', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-0.123]"') SETTINGS date_time_input_format = 'best_effort_us'; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }
SELECT 'be_unquoted_bare_int', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[1783585473954]"') SETTINGS date_time_input_format = 'best_effort';

-- A leading '+' must be rejected under EVERY date_time_input_format, not just basic, so the
-- effective parser does not depend on quoting/nesting. Scalar DateTime64 rejects '+' under both
-- basic (readDateTimeTextFallback only special-cases '-') and best_effort (parseDateTime64BestEffort
-- treats it as a malformed timezone offset). The '+' guard runs before the best_effort bare-tick
-- fallback, so the unquoted container path rejects '+1783585473954' under best_effort/best_effort_us
-- too, matching scalar (the basic '+' forms are covered above by container_plus_*).
SELECT 'be_plus_bare_container', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+1783585473954]"') SETTINGS date_time_input_format = 'best_effort'; -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'beus_plus_bare_container', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[+1783585473954]"') SETTINGS date_time_input_format = 'best_effort_us'; -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'be_plus_bare_scalar', toString(x) FROM format(CSV, 'x DateTime64(3)', '+1783585473954') SETTINGS date_time_input_format = 'best_effort'; -- { serverError CANNOT_PARSE_DATETIME }

-- Minimum negative bare-integer tick count must be preserved. The sign is consumed up front
-- (chunk-boundary safety), so the magnitude is read as unsigned and negated in a well-defined
-- way: `readIntText(whole); whole = -whole` on a signed Int64 would be signed-overflow UB for
-- -9223372036854775808 (|INT64_MIN| == 2^63 does not fit a signed Int64), and would diverge
-- between the throw and try paths. This matches the pre-PR readIntText(x) container behavior.
SELECT 'int64_min_bare', reinterpretAsInt64(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-9223372036854775808]"');
SELECT 'int64_min_plus1_bare', reinterpretAsInt64(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-9223372036854775807]"');
SELECT 'int64_max_bare', reinterpretAsInt64(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[9223372036854775807]"');
-- Magnitude past |INT64_MIN| (2^63 + 1) is out of range and must be rejected, not silently wrapped.
SELECT 'below_int64_min_bare', reinterpretAsInt64(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-9223372036854775809]"'); -- { serverError CANNOT_PARSE_NUMBER }
-- JSON element exercises the same path.
SELECT 'json_int64_min_bare', reinterpretAsInt64(x[1]) FROM format(JSONEachRow, 'x Array(DateTime64(3))', '{"x":[-9223372036854775808]}');

-- Positive whole part must be overflow-checked exactly like the scalar readDateTime64Text path
-- (which uses ReadIntTextCheckOverflow::CHECK_OVERFLOW). An out-of-range positive whole seconds
-- value is rejected consistently on the container throw path, the container try path, and the
-- scalar path, instead of silently wrapping a signed time_t before the fractional logic runs.
SELECT 'pos_overflow_whole_container', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[18446744073709551615.1]"') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'pos_overflow_whole_container_json', toString(x[1]) FROM format(JSONEachRow, 'x Array(DateTime64(3))', '{"x":[18446744073709551615.1]}') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'pos_overflow_whole_scalar', toString(x) FROM format(CSV, 'x DateTime64(3)', '18446744073709551615.1') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_NUMBER }
-- An in-range positive whole.fraction must still agree between scalar and container.
SELECT 'pos_whole_scalar_eq_container', (SELECT x[1] FROM format(CSV, 'x Array(DateTime64(3, ''UTC''))', '"[999999999.999]"') SETTINGS date_time_input_format = 'basic') = (SELECT CAST('999999999.999' AS DateTime64(3, 'UTC')) SETTINGS date_time_input_format = 'basic');

-- Fraction is truncated / padded to the column scale.
SELECT 'scale0', [1783585473.954]::Array(DateTime64(0));
SELECT 'scale6_extra', [1783585473.954321987]::Array(DateTime64(6));
SELECT 'scale3_short', [1783585473.95]::Array(DateTime64(3));

-- Backward compatibility: a bare integer is still a scaled tick count, not seconds.
SELECT 'bc_bare_int_ticks', [1504193808]::Array(DateTime64(3));
SELECT 'bc_neg_bare_int_ticks', [-1390214744]::Array(DateTime64(3));

-- A lone '.' or '-.' has no digit on either side of the decimal point. Both were rejected before
-- this PR; the fractional support must not silently coerce a missing mantissa to the epoch. The
-- check lives in the shared scalar/container path so scalar and container agree.
SELECT 'empty_dot_container', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[.]"') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'empty_neg_dot_container', toString(x[1]) FROM format(CSV, 'x Array(DateTime64(3))', '"[-.]"') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'empty_dot_json', toString(x[1]) FROM format(JSONEachRow, 'x Array(DateTime64(3))', '{"x":[.]}') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'empty_neg_dot_json', toString(x[1]) FROM format(JSONEachRow, 'x Array(DateTime64(3))', '{"x":[-.]}') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_NUMBER }
SELECT 'empty_dot_scalar', toString(x) FROM format(CSV, 'x DateTime64(3)', '.') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_DATETIME }
SELECT 'empty_neg_dot_scalar', toString(x) FROM format(CSV, 'x DateTime64(3)', '-.') SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_DATETIME }
-- A digit on either side of the point is enough: `.5`, `5.`, `-.5`, `0.` all remain valid and agree
-- between the scalar and container numeric readers.
SELECT 'dot_lhs_scalar_eq_container', (SELECT x[1] FROM format(CSV, 'x Array(DateTime64(3, ''UTC''))', '"[5.]"') SETTINGS date_time_input_format = 'basic') = (SELECT x FROM format(CSV, 'x DateTime64(3, ''UTC'')', '5.') SETTINGS date_time_input_format = 'basic');
SELECT 'dot_rhs_scalar_eq_container', (SELECT x[1] FROM format(CSV, 'x Array(DateTime64(3, ''UTC''))', '"[.5]"') SETTINGS date_time_input_format = 'basic') = (SELECT x FROM format(CSV, 'x DateTime64(3, ''UTC'')', '.5') SETTINGS date_time_input_format = 'basic');
