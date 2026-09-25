#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_legacy_binary_settings"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_legacy_binary_settings (d Date, i Int32)
    ENGINE = MergeTree PARTITION BY d ORDER BY i
"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_legacy_binary_settings VALUES ('2021-01-01', 1), ('2021-01-02', 2)
"

python3 "$CURDIR"/05136_legacy_binary_settings_signed_integer.python

$CLICKHOUSE_CLIENT -q "DROP TABLE t_legacy_binary_settings"
