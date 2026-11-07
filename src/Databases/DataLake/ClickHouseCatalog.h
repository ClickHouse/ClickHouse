#pragma once

#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/RestCatalog.h>

namespace DataLake
{

/// Client of the Iceberg REST catalog served by this same server (`catalog_type = 'clickhouse'`).
/// It is a plain `RestCatalog` that authenticates with the per-process loopback secret and acts
/// as `catalog_user`, the user who created the database.
class ClickHouseCatalog final : public RestCatalog
{
public:
    ClickHouseCatalog(
        const std::string & warehouse_,
        const std::string & base_url_,
        const std::string & catalog_user_,
        bool flat_namespaces_,
        DB::ContextPtr context_);

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_CLICKHOUSE;
    }

private:
    DB::HTTPHeaderEntries getAuthHeaders(const CatalogState & catalog_state, bool update_token) const override;

    const std::string catalog_user;
};

}

#endif
