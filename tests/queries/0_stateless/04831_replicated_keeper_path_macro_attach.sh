#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree
# Rows needing a per-copy unique database name or UUID, which a .sql file cannot interpolate.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ZK="/clickhouse/04831/${CLICKHOUSE_DATABASE}"
# A database name is a carrier in its own right, so use one whose own name is path-unsafe.
UNSAFE_DB="${CLICKHOUSE_DATABASE}/d"

# A full-definition ATTACH warns on stderr, which says nothing about the checks under test.
CLIENT="${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null"

# Report the error name once, whether or not the client repeats the message.
expect_bad_arguments() { grep -q -F 'BAD_ARGUMENTS' && echo BAD_ARGUMENTS || echo UNEXPECTED; }

${CLIENT} -q "DROP DATABASE IF EXISTS \`${UNSAFE_DB}\` SYNC"
${CLIENT} -q "CREATE DATABASE \`${UNSAFE_DB}\`"

# Only {database} substitutes here, so this row fails on the database branch alone.
${CLIENT} -q "CREATE TABLE \`${UNSAFE_DB}\`.t (c0 Int) ENGINE = ReplicatedMergeTree('${ZK}/{database}/fixed', 'r1') ORDER BY c0" 2>&1 |
    expect_bad_arguments

${CLIENT} -q "DROP DATABASE \`${UNSAFE_DB}\` SYNC"

# The same branch, reached by a database name that is illegal as a path COMPONENT instead of by
# carrying a '/': here the check on the substituted value has nothing to object to.
CTL_DB=$(printf '%s_g\001h' "${CLICKHOUSE_DATABASE}")
${CLIENT} -q "DROP DATABASE IF EXISTS \`${CTL_DB}\` SYNC"
${CLIENT} -q "CREATE DATABASE \`${CTL_DB}\`"
${CLIENT} -q "CREATE TABLE \`${CTL_DB}\`.t (c0 Int) ENGINE = ReplicatedMergeTree('${ZK}/{database}/fixed', 'r1c') ORDER BY c0" 2>&1 |
    expect_bad_arguments
${CLIENT} -q "DROP DATABASE \`${CTL_DB}\` SYNC"

# A full definition supplied by the user is judged like a CREATE, unlike the short form above.
U1=$(${CLIENT} -q "SELECT generateUUIDv4()")
${CLIENT} -q "ATTACH TABLE \`h/replicas/i\` UUID '${U1}' (c0 Int) ENGINE = ReplicatedMergeTree('${ZK}/{database}/{table}', 'r2') ORDER BY c0" 2>&1 |
    expect_bad_arguments

U2=$(${CLIENT} -q "SELECT generateUUIDv4()")
${CLIENT} -q "ATTACH TABLE t_full_def UUID '${U2}' (c0 Int) ENGINE = ReplicatedMergeTree('${ZK}/{database}/{table}', 'r3') ORDER BY c0" 2>/dev/null
${CLIENT} -q "SELECT count() FROM system.replicas WHERE database = currentDatabase() AND table = 't_full_def'"
${CLIENT} -q "DROP TABLE t_full_def"

# A table whose stored path still carries {database} keeps loading after the database is renamed to a
# path-unsafe name: re-reading a definition from metadata must not re-judge it. A configured macro
# supplies the {database}, so unlike the short ATTACH above the substitution does survive into metadata.
LEGACY_DB="${CLICKHOUSE_DATABASE}_legacy"
${CLIENT} -q "DROP DATABASE IF EXISTS \`${LEGACY_DB}/d\` SYNC"
${CLIENT} -q "DROP DATABASE IF EXISTS \`${LEGACY_DB}\` SYNC"
${CLIENT} -q "CREATE DATABASE \`${LEGACY_DB}\`"
${CLIENT} -q "CREATE TABLE \`${LEGACY_DB}\`.t (c0 Int) ENGINE = ReplicatedMergeTree('{default_path_test}04831legacy', 'r4') ORDER BY c0"
${CLIENT} -q "RENAME DATABASE \`${LEGACY_DB}\` TO \`${LEGACY_DB}/d\`"
${CLIENT} -q "DETACH TABLE \`${LEGACY_DB}/d\`.t"
${CLIENT} -q "ATTACH TABLE \`${LEGACY_DB}/d\`.t" 2>&1 | grep -q -F 'BAD_ARGUMENTS' && echo REJECTED || echo ATTACHED
${CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${LEGACY_DB}/d' AND name = 't'"
# Re-resolve the path under the original name before dropping: the stored path re-expands {database},
# so the table now points at a different znode tree than the one the CREATE made, and dropping it
# under the new name would leave the original tree behind. This is pre-existing behaviour of a stored
# {database}, unrelated to the checks, and it is why the table is created in its own database.
${CLIENT} -q "RENAME DATABASE \`${LEGACY_DB}/d\` TO \`${LEGACY_DB}\`"
${CLIENT} -q "DETACH TABLE \`${LEGACY_DB}\`.t"
${CLIENT} -q "ATTACH TABLE \`${LEGACY_DB}\`.t"
${CLIENT} -q "DROP DATABASE \`${LEGACY_DB}\` SYNC"

# SYSTEM RESTART REPLICA re-attaches from stored metadata, so it must not re-judge the path either.
# On a rejection the table is left permanently detached, so the count below is the real assertion.
RESTART_DB="${CLICKHOUSE_DATABASE}_restart"
${CLIENT} -q "DROP DATABASE IF EXISTS \`${RESTART_DB}/d\` SYNC"
${CLIENT} -q "DROP DATABASE IF EXISTS \`${RESTART_DB}\` SYNC"
${CLIENT} -q "CREATE DATABASE \`${RESTART_DB}\`"
${CLIENT} -q "CREATE TABLE \`${RESTART_DB}\`.t (c0 Int) ENGINE = ReplicatedMergeTree('{default_path_test}04831restart', 'r5') ORDER BY c0"
# A direct {database} is unfolded before the definition is stored, leaving nothing to re-substitute,
# so assert the retention this row depends on instead of assuming it.
${CLIENT} -q "SELECT create_table_query LIKE '%{default_path_test}%' FROM system.tables WHERE database = '${RESTART_DB}' AND name = 't'"
${CLIENT} -q "RENAME DATABASE \`${RESTART_DB}\` TO \`${RESTART_DB}/d\`"
${CLIENT} -q "SYSTEM RESTART REPLICA \`${RESTART_DB}/d\`.t" 2>&1 | grep -q -F 'BAD_ARGUMENTS' && echo REJECTED || echo RESTARTED
${CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${RESTART_DB}/d' AND name = 't'"
${CLIENT} -q "RENAME DATABASE \`${RESTART_DB}/d\` TO \`${RESTART_DB}\`"
${CLIENT} -q "DETACH TABLE \`${RESTART_DB}\`.t"
${CLIENT} -q "ATTACH TABLE \`${RESTART_DB}\`.t"
${CLIENT} -q "DROP DATABASE \`${RESTART_DB}\` SYNC"
