#!/usr/bin/env bash

# The modification hash of a view must cover the `additional_table_filters` that change the rows it
# returns, and the tables those filters read:
# - an entry keyed by the view itself, which the planner folds into the query reading the view;
# - an entry in the `SETTINGS` clause of the view's stored query, which never appears in the settings
#   of the reading query.
# Otherwise `query_cache_use_only_when_data_was_not_changed` could serve a stale result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_05340 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE aux_05340 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src_05340 VALUES (1), (2), (3);
    INSERT INTO aux_05340 VALUES (1);
    CREATE VIEW v_05340 AS SELECT x FROM src_05340;
    CREATE VIEW v_stored_05340 AS SELECT x FROM src_05340
        SETTINGS additional_table_filters = {'src_05340': 'x IN (SELECT x FROM aux_05340)'};
"

hash_of()
{
    local view=$1
    shift
    $CLICKHOUSE_CLIENT "$@" -q "
        SELECT toString(modification_hash) FROM system.tables
        WHERE database = currentDatabase() AND name = '${view}'"
}

baseline=$(hash_of v_05340)
self_keyed=$(hash_of v_05340 --additional_table_filters "{'v_05340': 'x != 1'}")
[ -n "${baseline}" ] && [ "${baseline}" != "${self_keyed}" ] && echo 'a filter keyed by the view changes its hash: yes' || echo 'a filter keyed by the view changes its hash: no'

filter="{'v_05340': 'x IN (SELECT x FROM aux_05340)'}"
before=$(hash_of v_05340 --additional_table_filters "${filter}")
stored_before=$(hash_of v_stored_05340)
$CLICKHOUSE_CLIENT -q "INSERT INTO aux_05340 VALUES (2)"
after=$(hash_of v_05340 --additional_table_filters "${filter}")
stored_after=$(hash_of v_stored_05340)
[ -n "${before}" ] && [ "${before}" != "${after}" ] && echo 'a table read by a filter keyed by the view changes its hash: yes' || echo 'a table read by a filter keyed by the view changes its hash: no'
[ -n "${stored_before}" ] && [ "${stored_before}" != "${stored_after}" ] && echo 'a table read by a filter of the stored query changes its hash: yes' || echo 'a table read by a filter of the stored query changes its hash: no'

# The same through the query cache: a new row in the table read by the stored filter must not be hidden.
# In the `ParallelReplicas` CI configuration the filter from the `SETTINGS` of the stored query was observed not to apply
# (https://github.com/ClickHouse/ClickHouse/issues/124015), so pin parallel replicas off.
query="SELECT count() FROM v_stored_05340 SETTINGS enable_parallel_replicas = 0, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"
$CLICKHOUSE_CLIENT -q "${query}"
$CLICKHOUSE_CLIENT -q "INSERT INTO aux_05340 VALUES (3)"
$CLICKHOUSE_CLIENT -q "${query}"

$CLICKHOUSE_CLIENT -q "
    DROP VIEW v_stored_05340;
    DROP VIEW v_05340;
    DROP TABLE aux_05340;
    DROP TABLE src_05340;
"
