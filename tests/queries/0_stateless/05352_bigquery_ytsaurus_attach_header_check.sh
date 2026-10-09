#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A full-definition ATTACH is user input like CREATE, so the header values are checked there too.
# Atomic and Replicated databases need a UUID for it; Ordinary ones do not accept one.
uuid_clause() {
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
        echo "UUID '$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")'"
    fi
}

$CLICKHOUSE_CLIENT -q "
    ATTACH TABLE t_bigquery $(uuid_clause) (x Int64)
    ENGINE = BigQuery('project', 'dataset', 'table', 'token', billing_project = 'p\r\nX-Injected: 1', base_url = 'http://127.0.0.1:1')" 2>&1 \
    | grep -o -m1 'HTTP header "X-Goog-User-Project" has invalid character'

$CLICKHOUSE_CLIENT -q "
    ATTACH TABLE t_ytsaurus $(uuid_clause) (x Int64)
    ENGINE = YTsaurus('http://127.0.0.1:1', '//tmp/t', 'token\r\nX-Injected: 1')
    SETTINGS allow_experimental_ytsaurus_table_engine = 1" 2>&1 \
    | grep -o -m1 'HTTP header "Authorization" has invalid character'

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('t_bigquery', 't_ytsaurus')"
