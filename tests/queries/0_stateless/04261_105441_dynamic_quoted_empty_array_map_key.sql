-- Tags: no-fasttest
-- Tag no-fasttest: requires Dynamic type, experimental.

-- Regression for issue #105441 ("Empty array as map key, not serialized back").
-- A `Dynamic` value whose Values/Quoted text infers to `Map(Array(Nothing), ...)` is
-- considered incomplete, so the deserializer falls back to `String`. The fallback
-- previously did `"'" + field + "'"`, which terminated prematurely at the first
-- embedded single quote and silently truncated the stored value. The fix uses
-- `writeQuotedString` so inner single quotes and backslashes are escaped properly.

SET allow_experimental_dynamic_type = 1;

SELECT '-- empty array as map key (raw text 11 chars, must round-trip intact)';
SELECT length(d::String), d::String, dynamicType(d)
FROM format(Values, 'd Dynamic', $$({[]:'came'})$$)
FORMAT JSONEachRow;

SELECT '-- empty array key with embedded single quote (would corrupt)';
SELECT length(d::String), d::String, dynamicType(d)
FROM format(Values, 'd Dynamic', $$({[]:'with\'embedded'})$$)
FORMAT JSONEachRow;

SELECT '-- nested empty array as map key';
SELECT length(d::String), d::String, dynamicType(d)
FROM format(Values, 'd Dynamic', $$({[[]]:1})$$)
FORMAT JSONEachRow;

SELECT '-- empty array key + non-empty key (Array(Int64) is complete, stays as Map)';
SELECT length(d::String), d::String, dynamicType(d)
FROM format(Values, 'd Dynamic', $$({[]:'a', [1]:'b'})$$)
FORMAT JSONEachRow;

SELECT '-- already-single-quoted plain string is not double-wrapped';
SELECT length(d::String), d::String, dynamicType(d)
FROM format(Values, 'd Dynamic', $$('plain string')$$)
FORMAT JSONEachRow;
