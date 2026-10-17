-- The optional UUID variant selector is read only from row 0 (`parseVariant`), so a
-- row-varying selector must be rejected at analysis time instead of silently encoding
-- every row with the first row's variant. `ILLEGAL_COLUMN` is the fleet-wide legacy code
-- for a non-constant value in a constant-only position (see e.g. `00396_uuid_v7`,
-- `01273_extractGroups`, `01595_countMatches`, `02562_regexp_extract`).
SELECT UUIDNumToString(toFixedString('0123456789abcdef', 16), materialize(toUInt8(1))); -- { serverError ILLEGAL_COLUMN }
SELECT UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', materialize(toUInt8(1))); -- { serverError ILLEGAL_COLUMN }
SELECT UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), materialize(toUInt8(1))); -- { serverError ILLEGAL_COLUMN }

-- Constant selectors keep working, on both variants.
SELECT UUIDNumToString(UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', 1), 1);
SELECT UUIDNumToString(UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', 2), 2);
SELECT hex(UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), 2));

-- A non-constant column in a declarative `const` position is rejected on the normal
-- column-aware path with the same `ILLEGAL_COLUMN`, regardless of whether the constant's
-- value participates in the result type (`randomFixedString`) or not (`port`).
SELECT port('http://example.com:8080/', materialize(toUInt16(80))); -- { serverError ILLEGAL_COLUMN }
SELECT port('http://example.com:8080/', toUInt16(80));
SELECT randomFixedString(materialize(10)); -- { serverError ILLEGAL_COLUMN }
