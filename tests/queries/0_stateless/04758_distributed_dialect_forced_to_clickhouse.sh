#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Queries written in a foreign dialect are rewritten to ClickHouse-SQL by the initiator, so the
# interserver senders force `dialect = 'clickhouse'` on the secondary query. That override only takes
# effect if it stays `changed`: `Connection::sendQuery` serializes changed settings only, so otherwise
# the shard falls back to its own user's/profile's default `dialect` and re-parses the already
# rewritten ClickHouse-SQL under a foreign parser. Cover all three interserver senders:
# `MultiplexedConnections`, `HedgedConnections` and `RemoteInserter` (the last one had no override at
# all). See 04758_distributed_compatibility_readonly_constrained_setting for the `compatibility` side
# of the same settings copies.

user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${profile}"

# The shard-side user defaults to a foreign dialect. The initiator keeps speaking ClickHouse-SQL, so
# this is exactly the state the override exists for: only the remote end would parse differently.
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${profile} SETTINGS dialect = 'kusto', allow_experimental_kusto_dialect = 1"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT REMOTE, CREATE TEMPORARY TABLE ON *.* TO ${user}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04758_dialect (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04758_dialect VALUES (1)"

# `remote()` SELECT through both senders. Two addresses, so two shards of one row each. A `Distributed`
# table is deliberately not covered: it connects as the cluster's configured user, whose `dialect` is
# the default one, so it cannot reach a shard that parses differently.
echo -n 'select_multiplexed: '
${CLICKHOUSE_CLIENT} --use_hedged_requests 0 --query "SELECT count() FROM remote('127.0.0.{1,2}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_04758_dialect', '${user}')"
echo -n 'select_hedged: '
${CLICKHOUSE_CLIENT} --use_hedged_requests 1 --query "SELECT count() FROM remote('127.0.0.{1,2}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_04758_dialect', '${user}')"

# Positive control, so the cases above cannot pass vacuously: the very same user, asked directly rather
# than as a shard, must have the server parse ClickHouse-SQL with the KQL parser and reject it. That
# proves the profile is in effect, hence that the distributed cases above only pass because the
# initiator forced `dialect` back. The error is expected, so it is captured - a test that leaks
# anything on stderr fails.
echo -n 'direct_query_as_that_user: '
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT 1" 2>&1 | grep -o -m1 "SYNTAX_ERROR"

# Distributed INSERT goes through `RemoteInserter`.
echo -n 'insert_remote: '
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION remote('127.0.0.2:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_04758_dialect', '${user}') VALUES (2)" && echo OK
echo -n 'count_after_insert: '
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04758_dialect"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04758_dialect"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${profile}"
