#!/usr/bin/env bash
# Tags: no-random-detach, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-random-detach: the test drives the hook itself, one query at a time
# no-object-storage, no-shared-merge-tree: the table under test defines its own local disk
# no-replicated-database: a custom disk is local to one server

# A table whose definition builds its own disk with the `disk` function must be skipped by the hook.
# The internal `ATTACH TABLE` is an ordinary user-initiated attach, and the security checks over a
# dynamic disk definition (`dynamic_disk_allow_from_env`, `dynamic_disk_allow_include`,
# `dynamic_disk_allow_from_zk`, and the restriction on resolving the server's own S3 credentials) are
# skipped only when the table is loaded from existing metadata. A table created once under a session
# that was allowed to define such a disk would therefore fail to attach back under the server's
# default settings, and the recovery `ATTACH` would fail the same way, leaving the table detached.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_own_disk"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_server_disk"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_reattach_own_disk (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_DATABASE}_own_disk/')"

check_if_not_detached "SELECT count() FROM t_reattach_own_disk" "t_reattach_own_disk"

# Control: a table that only uses disks from the server configuration stays a reattach candidate, so
# the check above cannot pass just because the hook did nothing at all.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_server_disk (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_detached "SELECT count() FROM t_reattach_server_disk" "t_reattach_server_disk"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_own_disk"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_server_disk"
