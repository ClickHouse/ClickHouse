#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./02530_dictionaries_update_field.lib
. "$CUR_DIR"/02530_dictionaries_update_field.lib

# NOTE: dictionaries will be updated according to server TZ, not session, so prohibit it's randomization
$CLICKHOUSE_CLIENT --session_timezone '' -q "
    CREATE TABLE table_for_update_field_dictionary
    (
        key UInt64,
        value String,
        last_insert_time DateTime
    )
    ENGINE = MergeTree()
    ORDER BY tuple()
"

run_test_with_layout "hashed"
