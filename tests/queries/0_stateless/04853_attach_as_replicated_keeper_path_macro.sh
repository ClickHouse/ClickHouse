#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree
# Rows needing a per-copy unique database name, which a .sql file cannot interpolate.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The rejection this test guards fires only on a name-based `default_replica_path`, which the test
# config does not set (it is {uuid}-based, and a {uuid} is never path-unsafe). So the treatment row
# lives in tests/integration/test_modify_engine_on_restart/test_unsafe_name.py, which configures
# that setting, and the rows here pin the exemptions the rejection must not disturb.
CLIENT="${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null"

# Converting a table with a path-safe name still works. Guards the check against rejecting the
# ordinary case: the shipped {uuid} template resolves through the same code path.
${CLIENT} -q "CREATE TABLE c1 (c0 Int) ENGINE = MergeTree ORDER BY c0"
${CLIENT} -q "INSERT INTO c1 VALUES (1), (2)"
${CLIENT} -q "DETACH TABLE c1"
${CLIENT} -q "ATTACH TABLE c1 AS REPLICATED"
${CLIENT} -q "SELECT 'safe_name_converts', engine, (SELECT count() FROM c1) FROM system.tables WHERE database = currentDatabase() AND name = 'c1'"

# The check only reads the definition, it must not rewrite the engine arguments. The stored
# template stays unexpanded, which is what keeps a metadata file copyable between replicas.
${CLIENT} -q "SELECT 'definition_not_rewritten', create_table_query LIKE '%{uuid}%' FROM system.tables WHERE database = currentDatabase() AND name = 'c1'"
${CLIENT} -q "DROP TABLE c1"

# The opposite direction strips the path arguments instead of minting one, so an unsafe name is
# irrelevant to it. The path is given explicitly here, so the CREATE itself is legal.
${CLIENT} -q "CREATE TABLE \`c3/unsafe\` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04853/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/c3', 'r1') ORDER BY c0"
${CLIENT} -q "DETACH TABLE \`c3/unsafe\`"
${CLIENT} -q "ATTACH TABLE \`c3/unsafe\` AS NOT REPLICATED"
${CLIENT} -q "SELECT 'as_not_replicated_allows_unsafe_name', engine FROM system.tables WHERE database = currentDatabase() AND name = 'c3/unsafe'"
${CLIENT} -q "DROP TABLE \`c3/unsafe\`"

# A table whose STORED path re-expands to a path-unsafe value keeps loading, both through a
# short ATTACH and through SYSTEM RESTART REPLICA. Two arming details:
#  * only a CONFIGURED macro survives into metadata unexpanded; a direct {database} is unfolded at
#    CREATE, leaving nothing to re-substitute.
#  * the DATABASE is renamed rather than the table, because RenamingRestrictions refuses to rename
#    a table whose path carries an implicit macro.
# Report the exemption from the client's exit status, not from a grep for one error code: the table
# is registered in the catalog before `startup()` runs on both routes, so a failure there leaves the
# table present and the count assertions below cannot see it.
run_exempt() {
    local label=$1 query=$2 out rc
    out=$(${CLIENT} -q "$query" 2>&1)
    rc=$?
    if [ "$rc" = "0" ]; then
        echo "$label OK"
    else
        echo "$label FAILED rc=$rc: $out"
    fi
}

LEGACY_DB="${CLICKHOUSE_DATABASE}_legacy"
${CLIENT} -q "DROP DATABASE IF EXISTS \`${LEGACY_DB}/d\` SYNC"
${CLIENT} -q "DROP DATABASE IF EXISTS \`${LEGACY_DB}\` SYNC"
${CLIENT} -q "CREATE DATABASE \`${LEGACY_DB}\`"
${CLIENT} -q "CREATE TABLE \`${LEGACY_DB}\`.t (c0 Int) ENGINE = ReplicatedMergeTree('{default_path_test}04853legacy', 'r2') ORDER BY c0"
${CLIENT} -q "SELECT 'stored_macro_armed', create_table_query LIKE '%{default_path_test}%' FROM system.tables WHERE database = '${LEGACY_DB}' AND name = 't'"
${CLIENT} -q "RENAME DATABASE \`${LEGACY_DB}\` TO \`${LEGACY_DB}/d\`"
${CLIENT} -q "DETACH TABLE \`${LEGACY_DB}/d\`.t"
run_exempt short_attach "ATTACH TABLE \`${LEGACY_DB}/d\`.t"
${CLIENT} -q "SELECT 'short_attach_count', count() FROM system.tables WHERE database = '${LEGACY_DB}/d' AND name = 't'"
run_exempt restart_replica "SYSTEM RESTART REPLICA \`${LEGACY_DB}/d\`.t"
${CLIENT} -q "SELECT 'restart_replica_count', count() FROM system.tables WHERE database = '${LEGACY_DB}/d' AND name = 't'"
# Re-resolve the path under the original name before dropping: the stored path re-expands {database},
# so under the new name the table points at a different znode tree than the CREATE made, and dropping
# it there would leave the original tree behind.
${CLIENT} -q "RENAME DATABASE \`${LEGACY_DB}/d\` TO \`${LEGACY_DB}\`"
${CLIENT} -q "DETACH TABLE \`${LEGACY_DB}\`.t"
${CLIENT} -q "ATTACH TABLE \`${LEGACY_DB}\`.t"
${CLIENT} -q "DROP DATABASE \`${LEGACY_DB}\` SYNC"
