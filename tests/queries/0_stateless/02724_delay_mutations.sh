#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_delay_mutations SYNC;

CREATE TABLE t_delay_mutations (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS
    number_of_mutations_to_delay = 2,
    number_of_mutations_to_throw = 10,
    min_delay_to_mutate_ms = 10,
    min_delay_to_mutate_ms = 1000;

SET mutations_sync = 0;
SYSTEM STOP MERGES t_delay_mutations;

INSERT INTO t_delay_mutations VALUES (1, 2);

ALTER TABLE t_delay_mutations UPDATE v = 3 WHERE 1;
ALTER TABLE t_delay_mutations UPDATE v = 4 WHERE 1;

ALTER TABLE t_delay_mutations UPDATE v = 5 WHERE 1;
ALTER TABLE t_delay_mutations UPDATE v = 6 WHERE 1;

SELECT * FROM t_delay_mutations ORDER BY id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_delay_mutations' AND NOT is_done;
"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_delay_mutations"
wait_for_mutation "t_delay_mutations" "mutation_5.txt"

${CLICKHOUSE_CLIENT} --query "
SELECT * FROM t_delay_mutations ORDER BY id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_delay_mutations' AND NOT is_done;

DROP TABLE IF EXISTS t_delay_mutations SYNC;
"

${CLICKHOUSE_CLIENT} --query "
SYSTEM FLUSH LOGS;

SELECT
    query,
    ProfileEvents['DelayedMutations'],
    ProfileEvents['DelayedMutationsMilliseconds'] BETWEEN 10 AND 1000
FROM system.query_log
WHERE
    type = 'QueryFinish' AND
    current_database = '$CLICKHOUSE_DATABASE' AND
    query ILIKE 'ALTER TABLE t_delay_mutations UPDATE%'
ORDER BY query;
"
