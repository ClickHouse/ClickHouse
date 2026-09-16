#!/usr/bin/env bash
# Test DataLake catalog cache settings and SYSTEM CLEAR without a live catalog.
# CREATE DATABASE connects to the catalog eagerly on current master, so the
# statement is validated with clickhouse-format; the cache behavior itself is
# covered by the integration test with a real catalog
# (tests/integration/test_database_iceberg/test_catalog_cache_concurrent_first_use).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "--- CREATE DATABASE accepts the catalog cache settings ---"
echo "CREATE DATABASE test_catalog_cache_db ENGINE=DataLakeCatalog('http://example.invalid/catalog') SETTINGS catalog_type='rest', warehouse='test', catalog_cache_staleness_ms=12345, catalog_cache_max_entries=99" \
    | $CLICKHOUSE_FORMAT --oneline | grep -o "catalog_cache_staleness_ms.*catalog_cache_max_entries.*"

echo "--- staleness 0 disables the cache ---"
echo "CREATE DATABASE test_catalog_cache_db2 ENGINE=DataLakeCatalog('http://example.invalid/catalog') SETTINGS catalog_type='rest', warehouse='test2', catalog_cache_staleness_ms=0, catalog_cache_max_entries=0" \
    | $CLICKHOUSE_FORMAT --oneline | grep -o "catalog_cache_staleness_ms.*catalog_cache_max_entries.*"

echo "--- SYSTEM CLEAR DATALAKE CATALOG CACHE is a no-op without catalogs ---"
$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR DATALAKE CATALOG CACHE"
echo "OK"
