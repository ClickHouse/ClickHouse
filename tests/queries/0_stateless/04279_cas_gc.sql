-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- Correctness-under-active-GC oracle. The `cas` disk below opts in to the background
-- reachability GC (`gc_enabled=1`, default OFF) and runs it aggressively
-- and `old_parts_lifetime=1` drops the merged-away source parts quickly so their footers/blobs become
-- unreferenced and turn into genuine GC fodder *during* the test. We assert the CA table stays
-- byte-for-byte identical to a normal MergeTree table on the same data: if a concurrent sweep ever
-- dropped a live blob, the oracle below would diverge. The test is fully deterministic — it never
-- waits on GC and never asserts that GC has run by a deadline; correctness must hold regardless of
-- whether (and how often) the background sweep fired.

DROP TABLE IF EXISTS t_cas_gc;
DROP TABLE IF EXISTS t_ref_gc;

CREATE TABLE t_cas_gc (a UInt64, s String, d Date)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04279',
    name = '04279_cas_gc',
    path = '04279_cas_gc_pool/',
    gc_enabled = 1,
    gc_interval_sec = 1),
    old_parts_lifetime = 1;

CREATE TABLE t_ref_gc (a UInt64, s String, d Date)
ENGINE = MergeTree ORDER BY a
SETTINGS old_parts_lifetime = 1;

-- First insert: rows 0..999.
INSERT INTO t_cas_gc SELECT number, toString(number % 7), toDate('2020-01-01') + number FROM numbers(1000);
INSERT INTO t_ref_gc SELECT number, toString(number % 7), toDate('2020-01-01') + number FROM numbers(1000);

-- Second distinct insert: rows 1000..1999 (different data => different blobs, not deduped away).
INSERT INTO t_cas_gc SELECT number, toString(number % 7), toDate('2020-01-01') + number FROM numbers(1000, 1000);
INSERT INTO t_ref_gc SELECT number, toString(number % 7), toDate('2020-01-01') + number FROM numbers(1000, 1000);

-- Merge both tables: on the CA table this leaves the source parts outdated; with old_parts_lifetime=1
-- they are removed shortly, so their footers/blobs become unreferenced and the active GC may sweep them.
OPTIMIZE TABLE t_cas_gc FINAL;
OPTIMIZE TABLE t_ref_gc FINAL;

-- Oracle: aggregates must match the normal table exactly.
SELECT 'count_match',  (SELECT count() FROM t_cas_gc) = (SELECT count() FROM t_ref_gc);
SELECT 'sum_match',    (SELECT sum(a)  FROM t_cas_gc) = (SELECT sum(a)  FROM t_ref_gc);

-- Oracle: full ordered content must match exactly — proves no live blob was dropped by a concurrent sweep.
SELECT 'content_match',
       (SELECT groupArray((a, s, d)) FROM (SELECT * FROM t_cas_gc ORDER BY a, s, d))
     = (SELECT groupArray((a, s, d)) FROM (SELECT * FROM t_ref_gc ORDER BY a, s, d));

-- A few point/range reads (each resolves ref -> part_id -> footer -> blob) must also match the oracle.
SELECT 'point_match',
       (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_cas_gc WHERE a IN (0, 999, 1000, 1999) ORDER BY a, s))
     = (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_ref_gc WHERE a IN (0, 999, 1000, 1999) ORDER BY a, s));

SELECT 'range_match',
       (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_cas_gc WHERE a BETWEEN 500 AND 1500 ORDER BY a, s))
     = (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_ref_gc WHERE a BETWEEN 500 AND 1500 ORDER BY a, s));

DROP TABLE t_cas_gc;
DROP TABLE t_ref_gc;
SELECT 'dropped_ok';
