#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-fasttest

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

# depends on parts name
${CLICKHOUSE_CLIENT} --insert_keeper_fault_injection_probability=0 -n --query "
DROP TABLE IF EXISTS t_packed_refcount SYNC;

CREATE TABLE t_packed_refcount (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/test/{database}/t_packed_refcount', '1')
ORDER BY id PARTITION BY id % 2
SETTINGS
    storage_policy = 's3_cache',
    allow_remote_fs_zero_copy_replication = 1,
    min_bytes_for_full_part_storage = '1G',
    old_parts_lifetime = 1,
    cleanup_delay_period = 1,
    cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration=0;

INSERT INTO t_packed_refcount VALUES (1, 10), (2, 20);

SET mutations_sync = 2;
ALTER TABLE t_packed_refcount UPDATE v = v * 10 WHERE id % 2 = 1;
"

wait_for_delete_inactive_parts "t_packed_refcount"

${CLICKHOUSE_CLIENT} -n --query "
SELECT name, active FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_refcount' ORDER BY name;

WITH (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND table = 't_packed_refcount') AS uuid
SELECT name, numChildren FROM system.zookeeper WHERE path = '/clickhouse/zero_copy/zero_copy_s3/' || uuid ORDER BY name;

SELECT id, v FROM t_packed_refcount ORDER BY id;

DROP TABLE IF EXISTS t_packed_refcount SYNC;
"
