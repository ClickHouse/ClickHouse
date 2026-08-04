-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- B36: after DETACH PARTITION on a cas disk, system.detached_parts must list the
-- detached part DIRECTORY name (e.g. all_1_2_1), not a sidecar / mutable file (metadata_version.txt).
-- The detached namespace is a container of detached part directories; the CA disk listing of the
-- "detached" path must yield the part directory names, not the files inside them.

DROP TABLE IF EXISTS t_cas_detach;

CREATE TABLE t_cas_detach (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04287',
    name = '04287_cas_detach',
    path = '04287_cas_detach_pool/');

INSERT INTO t_cas_detach SELECT number, number * 2 FROM numbers(50);
INSERT INTO t_cas_detach SELECT number, number * 2 FROM numbers(50, 50);
OPTIMIZE TABLE t_cas_detach FINAL;

SELECT 'count_before', count() FROM t_cas_detach;

ALTER TABLE t_cas_detach DETACH PARTITION tuple();

SELECT 'count_after', count() FROM t_cas_detach;

-- The detached parts listing must show the part directory name, not metadata_version.txt.
SELECT 'detached', name
FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_cas_detach'
ORDER BY name;

DROP TABLE t_cas_detach;
SELECT 'dropped_ok';
