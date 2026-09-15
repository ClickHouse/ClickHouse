#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-replicated-database
# ^ no-fasttest: cas is an object-storage metadata type; keep it off the minimal image.
#   no-shared-merge-tree: this exercises open-source ReplicatedMergeTree on a cas disk.
#   no-replicated-database: the source replica_path is hard-coded per the database, not per the replica.

# ALTER TABLE ... FETCH PARTITION ... FROM '<zk_path>' on a cas disk: the gate is lifted
# and a to_detached fetch takes the byte-fetch path (the downloaded files content-address into the
# detached/ namespace; relink-into-detached is deferred). The fetched part must land usably in the CA
# detached/ namespace: system.detached_parts lists it, ATTACH publishes an active part out of it, and a
# SELECT reads back the exact source data. Both tables share one inline CA pool (a single server fetches
# from its own zk path, as 03350 does), so this also exercises the cross-table detached landing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS t_cas_fetch_src;
DROP TABLE IF EXISTS t_cas_fetch_dst;

CREATE TABLE t_cas_fetch_src (key Int, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_cas_fetch_src', 'r1')
PARTITION BY (key % 2) ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '${CLICKHOUSE_DATABASE}_05002',
    name = '${CLICKHOUSE_DATABASE}_05002_cas_fetch',
    path = '${CLICKHOUSE_DATABASE}_05002_cas_fetch_pool/');

CREATE TABLE t_cas_fetch_dst (key Int, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_cas_fetch_dst', 'r1')
PARTITION BY (key % 2) ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '${CLICKHOUSE_DATABASE}_05002',
    name = '${CLICKHOUSE_DATABASE}_05002_cas_fetch',
    path = '${CLICKHOUSE_DATABASE}_05002_cas_fetch_pool/');

INSERT INTO t_cas_fetch_src VALUES (0, 'a'), (2, 'b'), (4, 'c');
SELECT 'src_parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_cas_fetch_src' AND active AND partition = '0';

-- Fetch the single part of partition 0 into the destination's detached/ namespace.
ALTER TABLE t_cas_fetch_dst FETCH PARTITION 0 FROM '/clickhouse/tables/{database}/t_cas_fetch_src'
    SETTINGS insert_keeper_fault_injection_probability = 0;

-- The fetched part must be present as a detached part.
SELECT 'detached_parts', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_cas_fetch_dst';

-- ATTACH publishes an active part out of the detached landing; SELECT must read back the exact data.
ALTER TABLE t_cas_fetch_dst ATTACH PARTITION 0
    SETTINGS insert_keeper_fault_injection_probability = 0;

SELECT 'detached_after_attach', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_cas_fetch_dst';
SELECT 'attached_rows', count() FROM t_cas_fetch_dst;
SELECT 'data_readback', key, s FROM t_cas_fetch_dst ORDER BY key;

DROP TABLE t_cas_fetch_src;
DROP TABLE t_cas_fetch_dst;
SELECT 'dropped_ok';

-- FORGET logs an operator WARNING; the harness runs the client at --send_logs_level=warning, which would
-- stream that expected warning to stderr and be flagged as a failure. Suppress it for the FORGET call only.
-- Both tables share the one named pool ('${CLICKHOUSE_DATABASE}_05002_cas_fetch'), so a single FORGET covers it.
SET send_logs_level = 'fatal';
SYSTEM CAS FORGET '${CLICKHOUSE_DATABASE}_05002_cas_fetch';
EOF
