#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY DELETE: what its scan reads, with whose rights, and that it never stops early.
#   1. rights: `ALTER DELETE` alone is enough
#   2. IN PARTITION: only the named partitions lose rows
#   3. snapshot: a row already dead is neither matched nor counted again
#   4. skip index: the scan drops granules by the table's skip index, like a plain read
#   5. read limit: under break-mode `max_rows_to_read`, every matching row still dies
#   6. time limit: a soft timeout fails the DELETE instead of committing part of it
#   7. simple predicate: one that reads another table is refused, also through a column's expression

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1 --optimize_trivial_count_query 0 --optimize_use_implicit_projections 0"
USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# 1. rights: red if the scan checks `SELECT` on the table (`delete_without_select` fails).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_rights"
$CLICKHOUSE_CLIENT --query "CREATE TABLE uk_rights (id UInt64) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_rights SELECT number FROM numbers(10)"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $USER"
$CLICKHOUSE_CLIENT --query "CREATE USER $USER IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "GRANT ALTER DELETE ON ${CLICKHOUSE_DATABASE}.uk_rights TO $USER"

$CLICKHOUSE_CLIENT --user "$USER" --query "DELETE FROM ${CLICKHOUSE_DATABASE}.uk_rights WHERE id < 3" \
    && echo "delete_without_select 1" || echo "delete_without_select 0"
$CLICKHOUSE_CLIENT --query "SELECT 'rights_survivors', groupArray(id) FROM (SELECT id FROM uk_rights ORDER BY id)"

$CLICKHOUSE_CLIENT --query "DROP USER $USER"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_rights"

# 2. IN PARTITION: red if the clause is refused, or ignored so the other partitions lose rows too.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_in_partition"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_in_partition (id UInt64, p UInt8)
    ENGINE = MergeTree PARTITION BY p ORDER BY id UNIQUE KEY (id)
"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_in_partition SELECT number, number % 3 FROM numbers(30)"

$CLICKHOUSE_CLIENT --query "DELETE FROM uk_in_partition IN PARTITION 1 WHERE id < 20"
$CLICKHOUSE_CLIENT --query "SELECT 'one_partition', p, count() FROM uk_in_partition GROUP BY p ORDER BY p"
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_in_partition IN PARTITION 0, 2 WHERE id < 10"
$CLICKHOUSE_CLIENT --query "SELECT 'two_partitions', p, count() FROM uk_in_partition GROUP BY p ORDER BY p"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_in_partition"

# 3. snapshot: red if the scan misses the table's own bitmaps, so the second DELETE counts the
# first one's 10 rows again (`UniqueKeyDeleteRows` 20, not 10).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_dead_once"
$CLICKHOUSE_CLIENT --query "CREATE TABLE uk_dead_once (id UInt64) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_dead_once SELECT number FROM numbers(100)"

FIRST_QID="${CLICKHOUSE_TEST_UNIQUE_NAME}_first"
SECOND_QID="${CLICKHOUSE_TEST_UNIQUE_NAME}_second"
$CLICKHOUSE_CLIENT --query_id "$FIRST_QID" --query "DELETE FROM uk_dead_once WHERE id < 10"
$CLICKHOUSE_CLIENT --query_id "$SECOND_QID" --query "DELETE FROM uk_dead_once WHERE id < 20"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT query_id = '$FIRST_QID' ? 'first' : 'second', ProfileEvents['UniqueKeyDeleteRows']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id IN ('$FIRST_QID', '$SECOND_QID')
    ORDER BY 1
"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_dead_once"

# 4. skip index: red if the scan reads every granule (`skip_index_prunes 0`), i.e. the predicate never
# reaches the read's index analysis. The query condition cache is off so it cannot prune in the index's place.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_skip_index"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_skip_index (id UInt64, v UInt64, INDEX v_minmax v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
    SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0
"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_skip_index SELECT number, number FROM numbers(1000)"

WITH_INDEX_QID="${CLICKHOUSE_TEST_UNIQUE_NAME}_with_index"
WITHOUT_INDEX_QID="${CLICKHOUSE_TEST_UNIQUE_NAME}_without_index"
PLAIN_READ_QID="${CLICKHOUSE_TEST_UNIQUE_NAME}_plain_read"
$CLICKHOUSE_CLIENT --use_query_condition_cache 0 --query_id "$WITH_INDEX_QID" \
    --query "DELETE FROM uk_skip_index WHERE v BETWEEN 100 AND 103"
$CLICKHOUSE_CLIENT --use_query_condition_cache 0 --use_skip_indexes 0 --query_id "$WITHOUT_INDEX_QID" \
    --query "DELETE FROM uk_skip_index WHERE v BETWEEN 200 AND 203"
$CLICKHOUSE_CLIENT --use_query_condition_cache 0 --query_id "$PLAIN_READ_QID" \
    --query "SELECT count() FROM uk_skip_index WHERE v BETWEEN 300 AND 303" > /dev/null

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT
        'skip_index_prunes', marks['$WITH_INDEX_QID'] < marks['$WITHOUT_INDEX_QID'],
        'like_plain_read', marks['$WITH_INDEX_QID'] = marks['$PLAIN_READ_QID']
    FROM
    (
        SELECT mapFromArrays(groupArray(query_id), groupArray(ProfileEvents['SelectedMarks'])) AS marks
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND query_id IN ('$WITH_INDEX_QID', '$WITHOUT_INDEX_QID', '$PLAIN_READ_QID')
    )
"
$CLICKHOUSE_CLIENT --query "
    SELECT 'skip_index_survivors', count(), countIf(v BETWEEN 100 AND 103 OR v BETWEEN 200 AND 203) FROM uk_skip_index
"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_skip_index"

# 5. read limit: red if the scan applies the query's read limits.
# Small blocks: break-mode limits are checked between blocks.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_del_readlimits"
$CLICKHOUSE_CLIENT --query "CREATE TABLE uk_del_readlimits (id UInt64, v String) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_del_readlimits"
$CLICKHOUSE_CLIENT --max_block_size 1000 --query "INSERT INTO uk_del_readlimits SELECT number, toString(number) FROM numbers(5000)"

$CLICKHOUSE_CLIENT --max_block_size 1000 --max_rows_to_read 1 --read_overflow_mode break \
    --query "DELETE FROM uk_del_readlimits WHERE id % 2 = 0"
$CLICKHOUSE_CLIENT --query "SELECT 'read_limit_survivors', count(), min(id), max(id) FROM uk_del_readlimits"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_del_readlimits"

# 6. time limit: red if a soft timeout ends the scan as if it had finished instead of failing it
# (`timeout_fails_delete 0`, `time_after_timeout` below 300).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_del_timelimit"
$CLICKHOUSE_CLIENT --query "CREATE TABLE uk_del_timelimit (id UInt64, v String) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_del_timelimit"
for start in 0 50 100 150 200 250; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO uk_del_timelimit SELECT number, toString(number) FROM numbers($start, 50)"
done
$CLICKHOUSE_CLIENT --query "
    SELECT 'time_parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_del_timelimit' AND active
"

# One thread at 0.02s per row takes ~6s, so the 1s limit lands mid-scan.
$CLICKHOUSE_CLIENT --max_threads 1 --max_execution_time 1 --timeout_overflow_mode break \
    --query "DELETE FROM uk_del_timelimit WHERE id >= 0 AND NOT sleepEachRow(0.02)" 2>&1 \
    | grep -q "TIMEOUT_EXCEEDED" && echo "timeout_fails_delete 1" || echo "timeout_fails_delete 0"
$CLICKHOUSE_CLIENT --query "SELECT 'time_after_timeout', count() FROM uk_del_timelimit"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_del_timelimit"

# 7. simple predicate: red if a predicate reading another table runs (`refused ... 0`, rows die), or if an Array
# column of the table on the right of IN is refused (`array_survivors` keeps id 1).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_del_simple"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_simple_keys"
$CLICKHOUSE_CLIENT --query "CREATE TABLE uk_simple_keys (id UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_del_simple (
        id UInt64,
        outside UInt8 ALIAS id NOT IN uk_simple_keys,
        outside_chain UInt8 ALIAS outside,
        outside_tuple Tuple(flag UInt8) ALIAS CAST(tuple(id NOT IN uk_simple_keys), 'Tuple(flag UInt8)'))
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_del_simple SELECT number FROM numbers(100)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_simple_keys VALUES (1)"

for case in \
    "in_subquery:id IN (SELECT id FROM uk_simple_keys)" \
    "in_table:id IN uk_simple_keys" \
    "scalar_subquery:id = (SELECT max(id) FROM uk_simple_keys)" \
    "exists:EXISTS (SELECT 1 FROM uk_simple_keys)" \
    "alias:outside" \
    "alias_chain:outside_chain" \
    "alias_qualified:uk_del_simple.outside" \
    "alias_subcolumn:outside_tuple.flag = 1"
do
    $CLICKHOUSE_CLIENT --query "DELETE FROM uk_del_simple WHERE ${case#*:}" 2>&1 \
        | grep -q "SUPPORT_IS_DISABLED" && echo "refused ${case%%:*} 1" || echo "refused ${case%%:*} 0"
done

$CLICKHOUSE_CLIENT --query "SELECT 'simple_survivors', count() FROM uk_del_simple"

# A DEFAULT column added after the insert, so the part computes it from its expression. Never read back: computing
# `IN` a table there aborts a debug server, a stock bug.
$CLICKHOUSE_CLIENT --query "CREATE TABLE uk_del_default (id UInt64) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_del_default SELECT number FROM numbers(100)"
$CLICKHOUSE_CLIENT --query "ALTER TABLE uk_del_default ADD COLUMN outside UInt8 DEFAULT id NOT IN uk_simple_keys"
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_del_default WHERE outside" 2>&1 \
    | grep -q "SUPPORT_IS_DISABLED" && echo "refused default 1" || echo "refused default 0"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_del_default"

$CLICKHOUSE_CLIENT --query "CREATE TABLE uk_del_array (id UInt64, allowed Array(UInt64)) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_del_array VALUES (1, [3]), (2, [2])"
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_del_array WHERE id IN (allowed)"
$CLICKHOUSE_CLIENT --query "SELECT 'array_survivors', groupArray(id) FROM uk_del_array"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_del_array"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_del_simple"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_simple_keys"
