#!/usr/bin/env bash
# Native TCP insert path: INSERT FORMAT with external data + RETURNING.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o errexit

$CLICKHOUSE_CLIENT --async_insert=0 --query "DROP TABLE IF EXISTS t_insert_returning_native"
$CLICKHOUSE_CLIENT --async_insert=0 --query "CREATE TABLE t_insert_returning_native (id UInt64, name String) ENGINE = Memory"

echo -e '1\tfoo' | $CLICKHOUSE_CLIENT --async_insert=0 --query "INSERT INTO t_insert_returning_native (id, name) RETURNING (SELECT id, name FROM t_insert_returning_native WHERE id = 1 ORDER BY id) FORMAT TabSeparated"

$CLICKHOUSE_CLIENT --async_insert=0 --query "SELECT id, name FROM t_insert_returning_native ORDER BY id"

# `input`-backed source keeps `FORMAT` before `RETURNING`; formatting must be stable across round-trips.
query_with_input="INSERT INTO t_insert_returning_native (id, name) SELECT * FROM input('id UInt64, name String') FORMAT TabSeparated RETURNING (SELECT count() FROM t_insert_returning_native)"
once=$(echo "$query_with_input" | ${CLICKHOUSE_FORMAT})
twice=$(echo "$once" | ${CLICKHOUSE_FORMAT})
[ "$once" = "$twice" ] && echo "stable" || echo "UNSTABLE"

echo -e '2\tbar' | $CLICKHOUSE_CLIENT --async_insert=0 --query "$query_with_input"

echo "source settings restored before delayed returning"
query_with_input_source_settings="INSERT INTO t_insert_returning_native (id, name) SELECT id + toUInt64(getSettingOrDefault('custom_insert_source', 'unset') = 'x') * 100, name FROM input('id UInt64, name String') FORMAT TabSeparated RETURNING (SELECT getSettingOrDefault('custom_insert_source', 'unset')) SETTINGS custom_insert_source = 'x'"
echo -e '3\tbaz' | $CLICKHOUSE_CLIENT --async_insert=0 --query "$query_with_input_source_settings"

$CLICKHOUSE_CLIENT --async_insert=0 --query "SELECT id, name FROM t_insert_returning_native ORDER BY id"

$CLICKHOUSE_CLIENT --async_insert=0 --query "DROP TABLE t_insert_returning_native"
