#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} -n --query "
DROP TABLE IF EXISTS t_lightweight_mut_1;

SET apply_mutations_on_fly = 1;

CREATE TABLE t_lightweight_mut_1 (id UInt64, v String, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    serialization_info_version = 'basic',
    storage_policy = 'default';

SYSTEM STOP MERGES t_lightweight_mut_1;

INSERT INTO t_lightweight_mut_1 VALUES (1, 'a', 'foo') (2, 'b', 'foo'), (3, 'c', 'foo');

ALTER TABLE t_lightweight_mut_1 UPDATE v = 'd' WHERE id = 1;
ALTER TABLE t_lightweight_mut_1 DELETE WHERE v = 'd';
ALTER TABLE t_lightweight_mut_1 UPDATE v = 'e' WHERE id = 2;
ALTER TABLE t_lightweight_mut_1 DELETE WHERE v = 'e';

SYSTEM DROP MARK CACHE;
SELECT id FROM t_lightweight_mut_1 ORDER BY id;

SYSTEM DROP MARK CACHE;
SELECT v FROM t_lightweight_mut_1 ORDER BY id;

SYSTEM DROP MARK CACHE;
SELECT id, v FROM t_lightweight_mut_1 ORDER BY id;

SYSTEM DROP MARK CACHE;
SELECT id, v, s FROM t_lightweight_mut_1 ORDER BY id;

SYSTEM DROP MARK CACHE;
SELECT id FROM t_lightweight_mut_1 ORDER BY id SETTINGS apply_mutations_on_fly = 0;

SYSTEM DROP MARK CACHE;
SELECT id, v FROM t_lightweight_mut_1 ORDER BY id SETTINGS apply_mutations_on_fly = 0;

SYSTEM FLUSH LOGS query_log;

SELECT query, ProfileEvents['FileOpen'] FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query ILIKE 'SELECT%FROM t_lightweight_mut_1%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

SELECT count() FROM system.mutations
WHERE table = 't_lightweight_mut_1' AND database = currentDatabase() AND NOT is_done;

SYSTEM START merges t_lightweight_mut_1;
"

wait_for_mutation "t_lightweight_mut_1" "mutation_5.txt"

$CLICKHOUSE_CLIENT -n --query "
SET apply_mutations_on_fly = 1;

SELECT count() FROM system.mutations
WHERE table = 't_lightweight_mut_1' AND database = currentDatabase() AND NOT is_done;

SELECT id, v FROM t_lightweight_mut_1 ORDER BY id;

DROP TABLE t_lightweight_mut_1;
"
