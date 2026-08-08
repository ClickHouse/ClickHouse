#!/usr/bin/env bash
# A full-definition `ATTACH TABLE ... UUID '...' (<definition>)` is CREATE-like user input, so the
# projection name length is validated there too, unlike a short `ATTACH TABLE t` that reads the
# definition from stored metadata. The bare `ATTACH TABLE t (<definition>)` without a UUID is
# rejected as INCORRECT_QUERY whatever the names are, so the UUID form is the only way to reach it.
#
# The UUID is generated at runtime rather than written as a fixed literal: the length check runs in
# `registerStorageMergeTree::create`, which `InterpreterCreateQuery` reaches only after taking
# `TemporaryLockForUUIDDirectory{create.uuid}`, so a concurrent copy of this test holding the same
# UUID would fail with TABLE_ALREADY_EXISTS before the name is ever measured.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 214 is the current limit; 215 is one over.
OVER_LIMIT=$(printf 'f%.0s' $(seq 215))
AT_LIMIT=$(printf 'g%.0s' $(seq 214))
UUID_OVER=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
UUID_OK=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# -m1 because the error message may contain the error code name multiple times.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_attach_over UUID '${UUID_OVER}' (a UInt64,
    PROJECTION ${OVER_LIMIT} (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a" 2>&1 \
    | grep -m 1 -o -F 'ARGUMENT_OUT_OF_BOUND'

# Positive control: a name at the limit attaches through the same code path and works end to end, so
# the arm above cannot pass by rejecting every full-definition ATTACH.
# `send_logs_level=fatal` suppresses the "full table definition is not recommended" warning.
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "ATTACH TABLE t_attach_ok UUID '${UUID_OK}' (a UInt64, b UInt64,
    PROJECTION ${AT_LIMIT} (SELECT a, sum(b) GROUP BY a)) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_attach_ok SELECT number, number FROM numbers(5)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_attach_ok"
$CLICKHOUSE_CLIENT -q "SELECT sum(rows) FROM system.projection_parts
    WHERE database = currentDatabase() AND table = 't_attach_ok' AND active"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_attach_ok"
