#!/usr/bin/env bash
# Tags: no-shared-merge-tree, long, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

${CLICKHOUSE_CLIENT} -n --query "
DROP TABLE IF EXISTS t_lightweight_mut_4 SYNC;

CREATE TABLE t_lightweight_mut_4 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lightweight_mut_4', '1')
ORDER BY id
SETTINGS merge_selecting_sleep_ms = 50, max_merge_selecting_sleep_ms = 50;

SYSTEM STOP MERGES t_lightweight_mut_4;
INSERT INTO t_lightweight_mut_4 VALUES (1, 1);
"

function wait_for_alter()
{
    type=$1
    for i in {1..100}; do
        sleep 0.1
        ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE t_lightweight_mut_4" | grep -q "\`v\` $type" && break;

        if [[ $i -eq 100 ]]; then
            echo "Timed out while waiting for alter to execute!"
        fi
    done
}

${CLICKHOUSE_CLIENT} --alter_sync 0 --query "ALTER TABLE t_lightweight_mut_4 MODIFY COLUMN v String"

wait_for_alter "String"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_lightweight_mut_4 UPDATE v = 'x' WHERE 1"

${CLICKHOUSE_CLIENT} -n --query "
SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_mutations_on_fly = 0;
SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_mutations_on_fly = 1;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES t_lightweight_mut_4"
wait_for_mutation "t_lightweight_mut_4" "0000000001"

${CLICKHOUSE_CLIENT} -n --query "
SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_mutations_on_fly = 0;
SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_mutations_on_fly = 1;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_lightweight_mut_4"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_lightweight_mut_4 UPDATE v = '100' WHERE 1"
${CLICKHOUSE_CLIENT} --alter_sync 0 --query "ALTER TABLE t_lightweight_mut_4 MODIFY COLUMN v UInt64"

wait_for_alter "UInt64"

${CLICKHOUSE_CLIENT} -n --query "
SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_mutations_on_fly = 1;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES t_lightweight_mut_4"
wait_for_mutation "t_lightweight_mut_4" "0000000003"

${CLICKHOUSE_CLIENT} -n --query "
SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_mutations_on_fly = 0;
SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_mutations_on_fly = 1;
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_lightweight_mut_4 SYNC"
