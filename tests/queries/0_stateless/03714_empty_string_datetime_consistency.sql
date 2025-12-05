SET session_timezone = 'UTC';

-- OrNull variants must return NULL on empty string
SELECT toDateOrNull('') FORMAT TSVRaw;
SELECT toDate32OrNull('') FORMAT TSVRaw;
SELECT toTimeOrNull('') FORMAT TSVRaw;
SELECT toTime64OrNull('', 3) FORMAT TSVRaw;
SELECT toDateTimeOrNull('') FORMAT TSVRaw;
SELECT toDateTime64OrNull('', 3) FORMAT TSVRaw;

-- Throw-mode variants must raise on empty string
SELECT toDate(''); -- { serverError CANNOT_PARSE_DATE }
SELECT toDate32(''); -- { serverError CANNOT_PARSE_DATE }
SELECT toTime(''); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toTime64('', 3); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime(''); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime64('', 3); -- { serverError CANNOT_PARSE_DATETIME }
SELECT timestamp(''); -- { serverError CANNOT_PARSE_DATETIME }
