-- Regression test for issue #86714.
--
-- Before the fix, `parseUUID` did not validate that the input characters
-- were valid hexadecimal digits. The 0..15 lookup table mapped invalid
-- characters to 0xff, so `unhex2` silently produced garbage bytes
-- (e.g. `unhex2('s', 't')` overflows back to 0xef in UInt8). This made
-- the `toUUID*` family accept arbitrary garbage strings of length 32 or
-- 36 and return a fabricated UUID instead of throwing or returning the
-- expected default value.
--
-- After the fix `parseUUID` and `tryParseUUID` reject any non-hex byte
-- (and any 36-char input where the dashes are not at positions 8, 13,
-- 18, 23). The strict `toUUID` throws, the `Or*` variants return their
-- documented fallback values.

SELECT '--- Reproducer from issue #86714: 32-char string with non-hex prefix ---';
SELECT toUUID('teststr-665785014160310158e823a3'); -- { serverError CANNOT_PARSE_UUID }
SELECT toUUIDOrNull('teststr-665785014160310158e823a3');
SELECT toUUIDOrZero('teststr-665785014160310158e823a3');
SELECT toUUIDOrDefault('teststr-665785014160310158e823a3', toUUID('11111111-1111-1111-1111-111111111111'));

SELECT '--- 36-char string, dashes at the right positions but non-hex elsewhere ---';
SELECT toUUID('zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz'); -- { serverError CANNOT_PARSE_UUID }
SELECT toUUIDOrNull('zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz');
SELECT toUUIDOrZero('zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz');

SELECT '--- 32-char string of fully non-hex characters ---';
SELECT toUUID('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'); -- { serverError CANNOT_PARSE_UUID }
SELECT toUUIDOrNull('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
SELECT toUUIDOrZero('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');

SELECT '--- 36-char string with valid hex but misplaced dashes ---';
-- Layout 12-4-4-4-12 instead of 8-4-4-4-12: every hex digit is valid,
-- but dashes are shifted, so the dash-position check rejects it.
SELECT toUUID('feefefef0000-0000-0000-0000-000000000000'); -- { serverError CANNOT_PARSE_UUID }
SELECT toUUIDOrNull('feefefef0000-0000-0000-0000-000000000000');
SELECT toUUIDOrZero('feefefef0000-0000-0000-0000-000000000000');

SELECT '--- 36-char string with one invalid hex digit at the start ---';
SELECT toUUID('Xeefefef-6657-8501-4160-310158e823a3'); -- { serverError CANNOT_PARSE_UUID }
SELECT toUUIDOrNull('Xeefefef-6657-8501-4160-310158e823a3');

SELECT '--- 36-char string with one invalid hex digit at the end ---';
SELECT toUUID('feefefef-6657-8501-4160-310158e823aX'); -- { serverError CANNOT_PARSE_UUID }
SELECT toUUIDOrNull('feefefef-6657-8501-4160-310158e823aX');

SELECT '--- Valid UUIDs must still parse (regression check for the fix itself) ---';
SELECT toUUID('feefefef-6657-8501-4160-310158e823a3') AS valid_36;
SELECT toUUID('feefefef665785014160310158e823a3') AS valid_32_no_dashes;
SELECT toUUID('00000000-0000-0000-0000-000000000000') AS zero_uuid;
SELECT toUUID('FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF') AS uppercase_hex;
SELECT toUUID('AbCdEf01-2345-6789-aBcD-eF0123456789') AS mixed_case;
SELECT toUUIDOrNull('feefefef-6657-8501-4160-310158e823a3') AS or_null_valid;
SELECT toUUIDOrZero('feefefef-6657-8501-4160-310158e823a3') AS or_zero_valid;
SELECT toUUIDOrDefault('feefefef-6657-8501-4160-310158e823a3',
                       toUUID('11111111-1111-1111-1111-111111111111')) AS or_default_valid;

SELECT '--- Round-trip: parse then format must be identity for valid 36-char input ---';
SELECT toString(toUUID('feefefef-6657-8501-4160-310158e823a3'));
SELECT toString(toUUID('feefefef665785014160310158e823a3'));

SELECT '--- CAST(String, UUID) rejects garbage; CAST(String, Nullable(UUID)) returns NULL ---';
SELECT CAST('teststr-665785014160310158e823a3' AS UUID); -- { serverError CANNOT_PARSE_UUID }
SELECT CAST('teststr-665785014160310158e823a3' AS Nullable(UUID));
SELECT accurateCastOrNull('teststr-665785014160310158e823a3', 'UUID');
