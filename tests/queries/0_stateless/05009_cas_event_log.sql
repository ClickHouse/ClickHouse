-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- Default-ON contract for `system.cas_log`: the per-event content-addressed audit log is
-- enabled by default. `programs/server/config.xml` ships a `<cas_log>` section because the
-- CAS disk feature is experimental and this audit log is its primary forensic instrument (it costs
-- nothing when no CAS disk is configured — events are emitted only by content-addressed disks). After we
-- exercise a content-addressed disk end-to-end (INSERT, OPTIMIZE), the table exists and carries this
-- disk's write-path events.

DROP TABLE IF EXISTS t_cas_event_log;

CREATE TABLE t_cas_event_log (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '05009',
    name = '05009_cas_event_log',
    path = '05009_cas_event_log_pool/');

-- Exercise the content-addressed write/merge path: this is exactly the work that emits put/ref events.
INSERT INTO t_cas_event_log SELECT number, toString(number % 7) FROM numbers(1000);
INSERT INTO t_cas_event_log SELECT number, toString(number % 7) FROM numbers(1000, 1000);
OPTIMIZE TABLE t_cas_event_log FINAL;

SELECT 'rows', count() FROM t_cas_event_log;

-- Make the buffered events durable before we read them back.
SYSTEM FLUSH LOGS cas_log;

-- Default-on assertion #1: the table exists (the config ships the section).
EXISTS TABLE system.cas_log;

-- Default-on assertion #2: our disk's write path emitted at least one `blob_put` event. Filter by
-- disk_name so parallel tests sharing this system table (e.g. the lane's own cas_s3 disk)
-- cannot perturb the result.
SELECT 'has_blob_put', count() > 0
FROM system.cas_log
WHERE disk_name = '05009_cas_event_log' AND event_type = 'blob_put';

DROP TABLE t_cas_event_log;
SELECT 'ok';
