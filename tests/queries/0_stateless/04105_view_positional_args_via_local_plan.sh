#!/usr/bin/env bash
# Verify that positional arguments inside views are resolved correctly via the
# local-plan path (createLocalPlan) triggered when prefer_localhost_replica=1
# routes a distributed query shard to the local node.
#
# createLocalPlan sets enable_positional_arguments=false on a copy of the
# context to prevent double-resolution of the outer query's positional args.
# This must not prevent the view's own positional args from being resolved,
# because the view is expanded on the local node (not on the initiator).
#
# When enable_positional_arguments=0 is set explicitly by the user the view's
# GROUP BY 1 stays as a literal constant.  In the new analyzer this produces
# NOT_AN_AGGREGATE (215); in the old analyzer the constant GROUP BY is silently
# dropped and a global aggregate is returned instead.
# The two .reference files handle this behavioral difference.
#
# Refs: https://github.com/ClickHouse/ClickHouse/issues/89940

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_table SYNC"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS test_view SYNC"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_table (str String) ENGINE = MergeTree ORDER BY str"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_table VALUES ('a'), ('b'), ('c')"
# GROUP BY 1 resolves to GROUP BY str -> 3 distinct groups.
${CLICKHOUSE_CLIENT} -q "CREATE VIEW test_view AS SELECT str, count() AS cnt FROM test_table GROUP BY 1"

# 127.0.0.1 is the local host; prefer_localhost_replica=1 guarantees that
# this single shard is served by createLocalPlan (no remote TCP connection).
echo '--- prefer_localhost_replica=1, positional args enabled: 3 ---'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM remote('127.0.0.1', currentDatabase(), test_view)
SETTINGS prefer_localhost_replica = 1"

# Sanity check: disabling positional arguments must also be respected on the
# local-plan path.  New analyzer: GROUP BY 1 stays literal -> NOT_AN_AGGREGATE
# (215) -> no output here.  Old analyzer: constant GROUP BY is dropped,
# global aggregate returns one row -> prints 1.  See .oldanalyzer.reference.
echo '--- prefer_localhost_replica=1, positional args disabled: error ---'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM remote('127.0.0.1', currentDatabase(), test_view)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 0" 2>/dev/null || true

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_table SYNC"
${CLICKHOUSE_CLIENT} -q "DROP VIEW test_view SYNC"
