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

-- Fraction is truncated / padded to the column scale.
SELECT 'scale0', [1783585473.954]::Array(DateTime64(0));
SELECT 'scale6_extra', [1783585473.954321987]::Array(DateTime64(6));
SELECT 'scale3_short', [1783585473.95]::Array(DateTime64(3));

-- Backward compatibility: a bare integer is still a scaled tick count, not seconds.
SELECT 'bc_bare_int_ticks', [1504193808]::Array(DateTime64(3));
SELECT 'bc_neg_bare_int_ticks', [-1390214744]::Array(DateTime64(3));
