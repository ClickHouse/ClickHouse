#!/usr/bin/env bash
# Tags: use-rocksdb, no-parallel
# Tag no-parallel - broken PG database may affect tests, reading system.tablesg

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q --multiline "
  CREATE DATABASE pg_$CLICKHOUSE_DATABASE Engine=PostgreSQL('invalid:5432', 'invalid', 'invalid', 'invalid');
  CREATE TABLE rocksdb_table (key UInt64, value String) Engine=EmbeddedRocksDB PRIMARY KEY(key) SETTINGS optimize_for_bulk_insert = 0;
  INSERT INTO rocksdb_table SELECT number, format('Hello, world ({})', toString(number)) FROM numbers(10);
"

$CLICKHOUSE_CLIENT -q --send_logs_level='trace' "
  SELECT value FROM system.rocksdb WHERE database = currentDatabase() and table = 'rocksdb_table' and name = 'number.keys.written';
" 2>&1 | grep -e "^10$" -e POSTGRESQL_CONNECTION_FAILURE

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS pg_$CLICKHOUSE_DATABASE;"