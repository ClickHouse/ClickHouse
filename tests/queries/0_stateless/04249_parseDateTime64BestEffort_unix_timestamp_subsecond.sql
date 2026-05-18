-- Unix timestamps with microsecond (16 digits) and nanosecond (19 digits) precision
-- must be parsed by the best-effort parser. Previously the parser only knew about
-- 13-digit millisecond timestamps and rejected longer ones with
-- "unexpected number of decimal digits".

SELECT 'toDateTime64 cast (cast_string_to_date_time_mode = best_effort)';
SELECT toDateTime64('1779094968417585', 6, 'UTC');
SELECT toDateTime64('1779094968417585845', 9, 'UTC');

SELECT 'parseDateTime64BestEffort';
SELECT parseDateTime64BestEffort('1779094968417585', 6, 'UTC');
SELECT parseDateTime64BestEffort('1779094968417585845', 9, 'UTC');

SELECT 'scale promotion: microsecond digits, nanosecond scale';
SELECT parseDateTime64BestEffort('1779094968417585', 9, 'UTC');

SELECT 'scale truncation: nanosecond digits, microsecond scale';
SELECT parseDateTime64BestEffort('1779094968417585845', 6, 'UTC');

SELECT 'parseDateTime64BestEffortOrNull / OrZero';
SELECT parseDateTime64BestEffortOrNull('1779094968417585845', 9, 'UTC');
SELECT parseDateTime64BestEffortOrZero('1779094968417585845', 9, 'UTC');

SELECT 'non-const';
SELECT parseDateTime64BestEffort(materialize('1779094968417585845'), 9, 'UTC');

SELECT 'parseDateTime64BestEffortUS';
SELECT parseDateTime64BestEffortUS('1779094968417585', 6, 'UTC');
SELECT parseDateTime64BestEffortUS('1779094968417585845', 9, 'UTC');
