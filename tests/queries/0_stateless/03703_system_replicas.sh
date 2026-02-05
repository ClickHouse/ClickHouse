#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create table rmt (key Int) engine=ReplicatedMergeTree('/tables/{database}', 'r1') order by ()"
uuid=$($CLICKHOUSE_CLIENT -q "select uuid from system.tables where database = currentDatabase() and table = 'rmt'")
$CLICKHOUSE_CLIENT -q "select uuid != toUUIDOrDefault(null) from system.tables where database = currentDatabase() and table = 'rmt'"
$CLICKHOUSE_CLIENT -q "select count() from system.replicas where uuid = '$uuid'"
