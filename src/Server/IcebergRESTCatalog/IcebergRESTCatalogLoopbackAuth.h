#pragma once

namespace DB::IcebergRESTCatalog
{

/// A `DataLakeCatalog` database with `catalog_type = 'clickhouse'` talks to the catalog listener of
/// its own server over loopback. It authenticates with these two headers instead of a password:
/// the user it acts as, and the per-process secret from `Context::getIcebergRESTCatalogSecret`.
constexpr auto LOOPBACK_USER_HEADER = "X-ClickHouse-User";
constexpr auto LOOPBACK_SECRET_HEADER = "X-ClickHouse-Iceberg-Catalog-Secret";

}
