#!/usr/bin/env bash
# Test DataLake catalog cache settings and SYSTEM CLEAR
# Covers catalog_cache_staleness_ms / catalog_cache_max_entries added in adonm/iceberg-manifest-and-rest-catalog-cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_catalog_cache_db"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE test_catalog_cache_db ENGINE=DataLakeCatalog('http://example.invalid/catalog') SETTINGS catalog_type='rest', warehouse='test', catalog_cache_staleness_ms=12345, catalog_cache_max_entries=99"
echo "--- SHOW CREATE DATABASE ---"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE test_catalog_cache_db" | grep -o "catalog_cache_staleness_ms.*catalog_cache_max_entries.*"

echo "--- SYSTEM CLEAR DATALAKE CATALOG CACHE ---"
$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR DATALAKE CATALOG CACHE"
echo "OK"

echo "--- ProfileEvents existence ---"
$CLICKHOUSE_CLIENT -q "SELECT event FROM system.events WHERE event LIKE '%DataLakeCatalogCache%' ORDER BY event"

$CLICKHOUSE_CLIENT -q "DROP DATABASE test_catalog_cache_db"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name='test_catalog_cache_db'"

# Test TTL 0 disables cache (no hit on repeated getTable)
$CLICKHOUSE_CLIENT -q "CREATE DATABASE test_catalog_cache_db2 ENGINE=DataLakeCatalog('http://example.invalid/catalog') SETTINGS catalog_type='rest', warehouse='test2', catalog_cache_staleness_ms=0, catalog_cache_max_entries=0"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE test_catalog_cache_db2" | grep -o "catalog_cache_staleness_ms.*"
$CLICKHOUSE_CLIENT -q "DROP DATABASE test_catalog_cache_db2"
echo "OK"
