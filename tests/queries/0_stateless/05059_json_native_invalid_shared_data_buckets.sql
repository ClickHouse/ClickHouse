-- Crafted Native blocks whose V3 JSON structure prefix carries an invalid shared-data bucket count.
-- The count sizes the per-bucket reader state directly, so it must be rejected before use.
-- The structure is passed to `format` explicitly: otherwise schema inference wraps the error into
-- CANNOT_EXTRACT_TABLE_STRUCTURE and hides the code the guard raises.

-- buckets = 0
SELECT count() FROM format('Native', 'j JSON', unhex('0101016a044a534f4e0400000000000000000100')); -- { serverError INCORRECT_DATA }
-- buckets = SIZE_MAX
SELECT count() FROM format('Native', 'j JSON', unhex('0101016a044a534f4e04000000000000000001ffffffffffffffffff01')); -- { serverError INCORRECT_DATA }
-- buckets = 257, one past the maximum
SELECT count() FROM format('Native', 'j JSON', unhex('0101016a044a534f4e040000000000000000018102')); -- { serverError INCORRECT_DATA }
-- buckets = 256, the maximum, passes the guard and fails only on the truncated payload
SELECT count() FROM format('Native', 'j JSON', unhex('0101016a044a534f4e040000000000000000018002')); -- { serverError CANNOT_READ_ALL_DATA, ATTEMPT_TO_READ_AFTER_EOF }
