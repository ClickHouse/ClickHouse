#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

# A view's modification hash folds the effective reader's settings, because they can change the rows
# the view returns. The query cache settings cannot: they only decide whether, where and for how long
# a result is cached. Folding them in would turn a pure caching-policy edit - `ALTER USER ... SETTINGS
# use_query_cache = 1`, a different `query_cache_tag` - into a data change, so a `REFRESH ... IF
# CHANGED APPEND` view reading through this view would discard its watermark and append a duplicate
# copy of unchanged rows.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_hash_qc_05100 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_hash_qc_05100 VALUES (1);
    CREATE VIEW v_hash_qc_05100 AS SELECT x FROM t_hash_qc_05100;
"

hash_of_view()
{
    $CLICKHOUSE_CLIENT "$@" -q "
        SELECT toString(modification_hash)
        FROM system.tables
        WHERE database = currentDatabase() AND name = 'v_hash_qc_05100'
    "
}

baseline=$(hash_of_view)
[ -n "${baseline}" ] && echo 'hash is computed'

# Writes are switched off in the same call, so that the introspection query - which calls
# `currentDatabase`, a non-deterministic function - is not rejected by the query cache's own gate
# before it can report a hash. All three settings are folded (or not) the same way.
switches=$(hash_of_view --use_query_cache 1 --enable_reads_from_query_cache 0 --enable_writes_to_query_cache 0)
[ "${baseline}" = "${switches}" ] && echo 'query cache switches do not change the hash'

# The tag separates cache entries from each other, but it cannot change a single row of the view.
tag=$(hash_of_view --query_cache_tag 'some_tag')
[ "${baseline}" = "${tag}" ] && echo 'the query cache tag does not change the hash'

policy=$(hash_of_view --query_cache_ttl 1 --query_cache_min_query_runs 5 --query_cache_max_entries 7 --query_cache_share_between_users 1 --query_cache_use_only_when_data_was_not_changed 1)
[ "${baseline}" = "${policy}" ] && echo 'query cache policy settings do not change the hash'

# A setting that can change the rows the view returns still has to move the hash.
filter=$(hash_of_view --additional_table_filters "{'t_hash_qc_05100': 'x != 1'}")
[ "${baseline}" != "${filter}" ] && echo 'a row filter changes the hash'

$CLICKHOUSE_CLIENT -q "
    DROP VIEW v_hash_qc_05100;
    DROP TABLE t_hash_qc_05100;
"
