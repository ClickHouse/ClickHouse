-- Test: exercises `UUIDv7ToDateTime` with non-v7 UUIDs to trigger the
-- documented "If the UUID is not a valid version 7 UUID, it returns
-- 1970-01-01 00:00:00.000" path.
-- Covers: src/Functions/FunctionsCodingUUID.cpp:466 — the `: 0` arm of
-- `((hiBytes & 0xf000) == 0x7000) ? (hiBytes >> 16) : 0`.

-- Zero UUID — version nibble is 0, not 7 → epoch
SELECT UUIDv7ToDateTime(toUUID('00000000-0000-0000-0000-000000000000'), 'UTC');

-- Arbitrary UUID with version nibble 6 (not 7) → epoch
SELECT UUIDv7ToDateTime(toUUID('00112233-4455-6677-8899-aabbccddeeff'), 'UTC');

-- v4 UUID (version nibble = 4) → epoch
SELECT UUIDv7ToDateTime(toUUID('aaaaaaaa-bbbb-4ccc-dddd-eeeeeeeeeeee'), 'UTC');

-- v1 UUID (version nibble = 1) → epoch
SELECT UUIDv7ToDateTime(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 'UTC');

-- A real v7 UUID — sanity check that valid v7 still extracts the timestamp
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'UTC');

-- Non-v7 inputs from a column (non-const code path)
SELECT UUIDv7ToDateTime(uuid, 'UTC')
FROM (
    SELECT arrayJoin([
        toUUID('00000000-0000-0000-0000-000000000000'),
        toUUID('aaaaaaaa-bbbb-4ccc-dddd-eeeeeeeeeeee'),
        toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1')
    ]) AS uuid
)
ORDER BY uuid;
