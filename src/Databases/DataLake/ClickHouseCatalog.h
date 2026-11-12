#pragma once

#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/RestCatalog.h>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace Poco::Net
{
    class IPAddress;
}

namespace DataLake
{

/// Client of the Iceberg REST catalog served by this same server (`catalog_type = 'clickhouse'`).
/// It reuses `RestCatalog` and only changes how requests are authenticated.
///
/// Requests go over loopback to the catalog listener of this server.
/// Any process on the machine can connect to loopback, so the client also sends a secret.
/// The secret is random and lives only in this process' memory, so it proves the request is internal.
/// This is the same idea as the cluster `secret` that `TCPHandler` uses between servers.
///
/// The client sends the secret together with `catalog_user`, the user who created the database.
/// The catalog then runs each request with that user's permissions and no password.
class ClickHouseCatalog final : public RestCatalog
{
public:
    ClickHouseCatalog(
        const std::string & warehouse_,
        const std::string & catalog_user_,
        bool flat_namespaces_,
        DB::ContextPtr context_,
        DB::DatabaseDataLakeSnapshotLoadingMode snapshot_loading_mode_);

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_CLICKHOUSE;
    }

    /// The loopback address where the catalog of this server listens.
    /// The catalog also sees requests of the database arrive from this address.
    static Poco::Net::IPAddress getLoopbackAddress(const Poco::Util::AbstractConfiguration & config);

private:
    /// The database sends requests to the Iceberg REST catalog of this same server over loopback.
    static std::string getCatalogURL(const Poco::Util::AbstractConfiguration & config);

    /// A proxy runs on another machine, where the loopback address means that machine.
    bool bypassProxy() const override { return true; }

    DB::HTTPHeaderEntries getAuthHeaders(const CatalogState & catalog_state, bool update_token) const override;

    const std::string catalog_user;
};

}

#endif
