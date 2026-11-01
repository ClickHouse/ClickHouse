-- Tags: no-fasttest
-- Reason: lowerUTF8 / upperUTF8 are only available with ICU support, which is disabled in the fast-test build.

-- Test that `lowerUTF8`/`upperUTF8` handle the ICU code path without int32_t overflow.
-- The root cause was that destCapacity passed to ICU's `ucasemap_utf8ToLower/Upper`
-- overflowed int32_t when the accumulated output buffer exceeded ~2 GB, causing
-- U_ILLEGAL_ARGUMENT_ERROR. We cannot allocate 2 GB in a stateless test, but we
-- exercise the ICU branch with non-ASCII inputs (which bypass the all-ASCII fast path)
-- so any future regression in the wrapping/casting logic surfaces as a wrong result
-- or exception here.

-- Basic non-ASCII correctness
SELECT lowerUTF8('MÜNCHEN');
SELECT upperUTF8('münchen');
SELECT lowerUTF8('ПРИВЕТ МИР');
SELECT upperUTF8('привет мир');
SELECT lowerUTF8('北京HELLO');
SELECT upperUTF8('北京hello');

-- Batch of non-ASCII rows to exercise the per-row loop
SELECT lowerUTF8(s) FROM (
    SELECT arrayJoin(['ÄHREN', 'ÜBER', 'STRAẞE', 'NAÏVE', 'CAFÉ', 'ÑOÑO']) AS s
);

SELECT upperUTF8(s) FROM (
    SELECT arrayJoin(['ähren', 'über', 'straße', 'naïve', 'café', 'ñoño']) AS s
);

-- Single-character non-ASCII edge cases
SELECT lowerUTF8('Ü');
SELECT upperUTF8('ü');

-- UTF-8 byte-expansion ratios under full case mapping (Unicode `SpecialCasing.txt`).
-- Each row prints input, input bytes, upper-cased output, output bytes. The 3.0x rows
-- are the worst case and the binding constraint behind the BAD_ARGUMENTS guard:
-- a single-row input > INT32_MAX / 3 ≈ 715 MiB of these would produce > INT32_MAX bytes,
-- which ICU cannot return as int32_t.
--   1.0x:  α  (U+03B1) -> Α  (U+0391):                 2 bytes -> 2 bytes
--   1.5x:  ŉ  (U+0149) -> ʼN (U+02BC U+004E):          2 bytes -> 3 bytes
--   2.0x:  և  (U+0587) -> ԵՒ (U+0535 U+0552):          2 bytes -> 4 bytes
--   3.0x:  ΐ  (U+0390) -> Ϊ́  (U+0399 U+0308 U+0301):   2 bytes -> 6 bytes
--   3.0x:  ΰ  (U+03B0) -> Ϋ́  (U+03A5 U+0308 U+0301):   2 bytes -> 6 bytes
SELECT 'α' AS input, length('α') AS in_bytes, upperUTF8('α') AS upper, length(upperUTF8('α')) AS out_bytes;
SELECT 'ŉ' AS input, length('ŉ') AS in_bytes, upperUTF8('ŉ') AS upper, length(upperUTF8('ŉ')) AS out_bytes;
SELECT 'և' AS input, length('և') AS in_bytes, upperUTF8('և') AS upper, length(upperUTF8('և')) AS out_bytes;
SELECT 'ΐ' AS input, length('ΐ') AS in_bytes, upperUTF8('ΐ') AS upper, length(upperUTF8('ΐ')) AS out_bytes;
SELECT 'ΰ' AS input, length('ΰ') AS in_bytes, upperUTF8('ΰ') AS upper, length(upperUTF8('ΰ')) AS out_bytes;
