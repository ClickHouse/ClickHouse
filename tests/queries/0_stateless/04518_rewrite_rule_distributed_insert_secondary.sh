#!/usr/bin/env bash
# Tags: no-parallel, no-async-insert
# no-parallel: rewrite rules are global server state
# no-async-insert: this test observes the synchronous distributed foreground INSERT (through
# `RemoteInserter`) in `system.query_log`. With async inserts forced on, the shard fragment is sent
# from a background async-insert flush that is decoupled from the initiator's query, so it is not
# linked by `initial_query_id` and the secondary fragment cannot be observed here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Rewrite rules are applied once, on the initiator. A distributed INSERT must not forward
# `query_rules` to the shard: the initiator sends a secondary INSERT fragment to the shard via
# `RemoteInserter`, which must strip `query_rules` from the forwarded settings, just like the read
# path strips it in `MultiplexedConnections` / `HedgedConnections`. Otherwise a shard would look up
# rules that may not exist there (throwing `REWRITE_RULE_DOESNT_EXIST`) or re-apply them to the
# already-final fragment.
#
# `prefer_localhost_replica = 0` forces the foreground distributed INSERT through `RemoteInserter`
# (a genuine secondary query over the connection) instead of the local-shard shortcut, so the
# settings forwarded with the secondary fragment are observable in `system.query_log`.

# Rule names are global; make it unique per test database. `DROP RULE` has no `IF EXISTS` form.
RULE="rule_dist_insert_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}" 2>/dev/null
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dist"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_local"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_local (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_dist (x UInt64) ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, t_local)"

# A rule that exists (so the initiator does not fail with REWRITE_RULE_DOESNT_EXIST when the
# setting is active) but matches neither the user's INSERT nor the shard fragment.
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT 'never matches this insert') REJECT WITH 'rejected'"

# Foreground distributed INSERT through RemoteInserter, with query_rules active on the initiator.
# It must succeed and land one row. A unique `log_comment` tags the initiator's initial query so
# its row can be located in `system.query_log`.
LOG_COMMENT="04518_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_rules "${RULE}" --prefer_localhost_replica 0 --distributed_foreground_insert 1 --log_comment "${LOG_COMMENT}" --query "INSERT INTO t_dist VALUES (1)"

echo "rows in t_local:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_local"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# The secondary INSERT fragment sent to the shard must carry no rules: `query_rules` was stripped
# from the forwarded settings. Before the fix it forwarded the initiator's `query_rules` unchanged,
# so the fragment's logged settings would contain the rule name.
#
# Secondary queries on the shard run with `current_database = default`, not the test database, so
# they cannot be filtered by `current_database = currentDatabase()` directly. Instead, locate the
# coordinator's initial query (which does have `current_database = currentDatabase()` — the filter
# the style check requires in any test that reads from `system.query_log`) by its `log_comment` and
# match the secondary fragments by `initial_query_id`.
echo "secondary fragments / fragments carrying query_rules:"
$CLICKHOUSE_CLIENT --query "
    WITH initial_query AS
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND log_comment = '${LOG_COMMENT}'
            AND type = 'QueryFinish'
            AND is_initial_query = 1
            AND event_date >= yesterday()
    )
    SELECT
        count() AS secondary_fragments,
        countIf(Settings['query_rules'] != '') AS fragments_with_rules
    FROM system.query_log
    WHERE initial_query_id IN (SELECT query_id FROM initial_query)
        AND is_initial_query = 0
        AND type = 'QueryFinish'
        AND query LIKE '%t_local%'
        AND event_date >= yesterday()"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_dist"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_local"
