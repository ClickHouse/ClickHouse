#!/usr/bin/env bash
# Tags: no-fasttest
# Protobuf format is not built in the fasttest environment.

set -e -o pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CUR_DIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=single_value_or_null_protobuf_state_round_trip
STATE_FILE=$(mktemp "${CLICKHOUSE_TMP}/${TABLE}.XXXXXX")
trap 'rm -f -- "$STATE_FILE"; $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE" >/dev/null 2>&1 || true' EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS $TABLE;
    CREATE TABLE $TABLE
    (
        state AggregateFunction(1, singleValueOrNull, UInt64)
    )
    ENGINE = MergeTree
    ORDER BY tuple();
"

$CLICKHOUSE_CLIENT --query "SELECT singleValueOrNullState(toUInt64(42)) AS state FROM numbers(1) FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/05027_single_value_or_null_protobuf_state:SingleValueOrNullState'" > "$STATE_FILE"

$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE SETTINGS format_schema='$SCHEMADIR/05027_single_value_or_null_protobuf_state:SingleValueOrNullState' FORMAT Protobuf" < "$STATE_FILE"

$CLICKHOUSE_CLIENT -q "SELECT singleValueOrNullMerge(state) FROM $TABLE"
