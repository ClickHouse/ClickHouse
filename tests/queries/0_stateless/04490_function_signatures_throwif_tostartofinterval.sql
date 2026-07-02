-- Tests for the corrected declarative signatures of `throwIf` and `toStartOfInterval`
-- (function-signatures DSL). The signatures are documentation-only (the legacy
-- `getReturnTypeImpl` remains authoritative), so they are surfaced via `system.functions`.

-- `throwIf`: the legal shapes are spelled out as explicit prefixes rather than two independent
-- optional groups, so the metadata cannot advertise `throwIf(cond, <non-String>)`.
SELECT signature FROM system.functions WHERE name = 'throwIf';
SET allow_custom_error_code_in_throwif = 1;
SELECT signature FROM system.functions WHERE name = 'throwIf';
SET allow_custom_error_code_in_throwif = 0;

-- `toStartOfInterval`: all four argument shapes are listed, including the four-argument
-- `(value, interval, origin, timezone)` overload and the constant interval/origin/timezone positions.
SELECT signature FROM system.functions WHERE name = 'toStartOfInterval';

-- The four-argument origin+timezone overload that the signature now documents works.
SELECT toTypeName(toStartOfInterval(toDateTime('2023-07-15 14:30:00', 'UTC'), toIntervalHour(1), toDateTime('2023-01-01 00:00:00', 'UTC'), 'UTC'));
SELECT toStartOfInterval(toDateTime('2023-07-15 14:30:00', 'UTC'), toIntervalHour(1), toDateTime('2023-01-01 00:00:00', 'UTC'), 'UTC')
     = toDateTime('2023-07-15 14:00:00', 'UTC');

-- A non-String second argument is rejected (the shape the old optional-group signature wrongly accepted),
-- both with and without the custom error-code setting.
SELECT throwIf(1, toInt8(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT throwIf(1, toInt8(1)) SETTINGS allow_custom_error_code_in_throwif = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Valid `throwIf` forms.
SELECT throwIf(0, 'message');
SELECT throwIf(0, 'message', toInt8(42)) SETTINGS allow_custom_error_code_in_throwif = 1;
