#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

function wait_for_alter()
{
    type=$1
    for i in {1..100}; do
        sleep 0.1
        ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE t_limit_mutations" | grep -q "\`v\` $type" && break;

        if [[ $i -eq 100 ]]; then
            echo "Timed out while waiting for alter to execute"
        fi
    done
}

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_limit_mutations SYNC;

CREATE TABLE t_limit_mutations (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_limit_mutations', '1') ORDER BY id
SETTINGS number_of_mutations_to_throw = 2;

SET mutations_sync = 0;
SYSTEM STOP MERGES t_limit_mutations;

INSERT INTO t_limit_mutations VALUES (1, 2);

ALTER TABLE t_limit_mutations UPDATE v = 3 WHERE 1;
ALTER TABLE t_limit_mutations UPDATE v = 4 WHERE 1;

SYSTEM SYNC REPLICA t_limit_mutations PULL;

ALTER TABLE t_limit_mutations UPDATE v = 5 WHERE 1; -- { serverError TOO_MANY_MUTATIONS }
ALTER TABLE t_limit_mutations MODIFY COLUMN v String; -- { serverError TOO_MANY_MUTATIONS }

SELECT * FROM t_limit_mutations ORDER BY id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_limit_mutations' AND NOT is_done;
SHOW CREATE TABLE t_limit_mutations;
"

${CLICKHOUSE_CLIENT} --query "
ALTER TABLE t_limit_mutations UPDATE v = 6 WHERE 1 SETTINGS number_of_mutations_to_throw = 100;
ALTER TABLE t_limit_mutations MODIFY COLUMN v String SETTINGS number_of_mutations_to_throw = 100, alter_sync = 0;
"

wait_for_alter "String"

${CLICKHOUSE_CLIENT} --query "
SELECT * FROM t_limit_mutations ORDER BY id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_limit_mutations' AND NOT is_done;
SHOW CREATE TABLE t_limit_mutations;
"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_limit_mutations"

wait_for_mutation "t_limit_mutations" "0000000003"

${CLICKHOUSE_CLIENT} --query "
SELECT * FROM t_limit_mutations ORDER BY id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_limit_mutations' AND NOT is_done;
SHOW CREATE TABLE t_limit_mutations;

DROP TABLE IF EXISTS t_limit_mutations SYNC;
"
