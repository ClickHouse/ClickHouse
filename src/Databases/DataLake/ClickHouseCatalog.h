#pragma once

#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/RestCatalog.h>

namespace DB
{
class IcebergRESTCatalogService;
using IcebergRESTCatalogServicePtr = std::shared_ptr<const IcebergRESTCatalogService>;
}

namespace DataLake
{

/// Client of Iceberg REST catalog of this same server (`catalog_type = 'clickhouse'`).
/// Reuses `RestCatalog`, but calls `IcebergRESTCatalogService` inside the process instead of HTTP.
/// Each catalog owns its service.
class ClickHouseCatalog final : public RestCatalog
{
public:
    ClickHouseCatalog(
        const std::string & warehouse_,
        bool flat_namespaces_,
        DB::ContextPtr context_,
        DB::DatabaseDataLakeSnapshotLoadingMode snapshot_loading_mode_);

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_CLICKHOUSE;
    }

private:
    /// `RestCatalog` sends every request through `createReadBuffer` and `sendRequest`.
    /// Overrides call catalog service of this server instead of HTTP.

    /// Runs `GET`.
    std::unique_ptr<DB::ReadBuffer> createReadBuffer(
        const CatalogState & catalog_state,
        const std::string & endpoint,
        const Poco::URI::QueryParameters & params,
        const DB::HTTPHeaderEntries & headers,
        const std::optional<DB::HTTPHeaderEntries> & auth_headers) const override;

    /// Runs a request with a body, such as `POST` or `DELETE`.
    String sendRequest(
        const CatalogState & catalog_state,
        const String & endpoint,
        Poco::JSON::Object::Ptr request_body,
        const String & method,
        bool ignore_result) const override;

    /// Runs one request in catalog service and returns response body.
    String callService(const String & method, const Poco::URI & uri, const String & body) const;

    const DB::IcebergRESTCatalogServicePtr service;
};

}

#endif
