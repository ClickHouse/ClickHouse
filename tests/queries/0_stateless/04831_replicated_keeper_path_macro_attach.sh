#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database
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

# A full definition supplied by the user is judged like a CREATE, unlike the short form above.
U1=$(${CLIENT} -q "SELECT generateUUIDv4()")
${CLIENT} -q "ATTACH TABLE \`h/replicas/i\` UUID '${U1}' (c0 Int) ENGINE = ReplicatedMergeTree('${ZK}/{database}/{table}', 'r2') ORDER BY c0" 2>&1 |
    expect_bad_arguments

U2=$(${CLIENT} -q "SELECT generateUUIDv4()")
${CLIENT} -q "ATTACH TABLE t_full_def UUID '${U2}' (c0 Int) ENGINE = ReplicatedMergeTree('${ZK}/{database}/{table}', 'r3') ORDER BY c0" 2>/dev/null
${CLIENT} -q "SELECT count() FROM system.replicas WHERE database = currentDatabase() AND table = 't_full_def'"
${CLIENT} -q "DROP TABLE t_full_def"
