-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- B23 mutable-per-part-state oracle: with assign_part_uuids=1, two INSERTs of IDENTICAL data produce
-- two parts whose column content is byte-identical but whose per-part uuid.txt differs. On a
-- cas disk the two parts dedup to ONE shared manifest, while their mutable per-part
-- files (uuid.txt / txn_version.txt / metadata_version.txt) live in a per-ref sidecar and are
-- overlaid on read. Before B23 the second part read the FIRST part's uuid (the shared manifest
-- embedded one part's mutable files), so the two uuids collided. This is a natural black-box oracle:
-- the cas table must behave exactly like a normal MergeTree table, and the two parts
-- must carry two DISTINCT uuids.

DROP TABLE IF EXISTS t_cas_mut;
DROP TABLE IF EXISTS t_ref_mut;

CREATE TABLE t_cas_mut (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS assign_part_uuids = 1, disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04282',
    name = '04282_cas',
    path = '04282_cas_pool/');

CREATE TABLE t_ref_mut (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS assign_part_uuids = 1;

-- Two IDENTICAL inserts → two parts with identical content but distinct per-part uuids.
INSERT INTO t_cas_mut SELECT number, toString(number % 5) FROM numbers(500);
INSERT INTO t_cas_mut SELECT number, toString(number % 5) FROM numbers(500);
INSERT INTO t_ref_mut SELECT number, toString(number % 5) FROM numbers(500);
INSERT INTO t_ref_mut SELECT number, toString(number % 5) FROM numbers(500);

-- Data oracle: the cas table matches the normal table exactly.
SELECT 'oracle_full_match',
       (SELECT groupArray((a, s)) FROM (SELECT * FROM t_cas_mut ORDER BY a, s))
     = (SELECT groupArray((a, s)) FROM (SELECT * FROM t_ref_mut ORDER BY a, s));

SELECT 'counts', count(), sum(a) FROM t_cas_mut;

-- Two active parts, each with its OWN non-empty, DISTINCT uuid (the B23 regression: no collision).
SELECT 'cas_active_parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_cas_mut' AND active;
SELECT 'cas_distinct_uuids', uniqExact(uuid) FROM system.parts
WHERE database = currentDatabase() AND table = 't_cas_mut' AND active;
SELECT 'cas_no_zero_uuid', countIf(uuid = toUUID('00000000-0000-0000-0000-000000000000')) FROM system.parts
WHERE database = currentDatabase() AND table = 't_cas_mut' AND active;

-- The same property holds on the normal table (oracle for the uuid behaviour).
SELECT 'ref_distinct_uuids', uniqExact(uuid) FROM system.parts
WHERE database = currentDatabase() AND table = 't_ref_mut' AND active;

DROP TABLE t_cas_mut;
DROP TABLE t_ref_mut;
SELECT 'dropped_ok';
