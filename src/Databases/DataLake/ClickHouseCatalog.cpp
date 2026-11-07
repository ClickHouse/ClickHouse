#include <Databases/DataLake/ClickHouseCatalog.h>

#if USE_AVRO

#include <algorithm>
#include <Interpreters/Context.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogLoopbackAuth.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DataLake
{

std::string ClickHouseCatalog::getCatalogURL(const Poco::Util::AbstractConfiguration & config)
{
    const auto port = config.getString("iceberg_rest_catalog.port", "");
    if (port.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
            "catalog_type = 'clickhouse' requires iceberg_rest_catalog.port in the server config");

    /// Same default as `getListenHosts` in `Server.cpp`.
    auto listen_hosts = DB::getMultipleValuesFromConfig(config, "", "listen_host");
    if (listen_hosts.empty())
        listen_hosts = {"::1", "127.0.0.1"};

    /// A wildcard address also accepts loopback connections.
    auto listens_on_loopback = [&](Poco::Net::IPAddress::Family family)
    {
        return std::ranges::any_of(listen_hosts, [&](const auto & host)
        {
            Poco::Net::IPAddress address;
            return Poco::Net::IPAddress::tryParse(host, address)
                && address.family() == family
                && (address.isLoopback() || address.isWildcard());
        });
    };

    /// Prefer IPv4. The server lists `::1` by default but may not bind it on a host without IPv6.
    if (listens_on_loopback(Poco::Net::IPAddress::IPv4))
        return fmt::format("http://127.0.0.1:{}", port);
    if (listens_on_loopback(Poco::Net::IPAddress::IPv6))
        return fmt::format("http://[::1]:{}", port);

    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
        "catalog_type = 'clickhouse' requires the server to listen on a loopback address, got listen_host: {}",
        fmt::join(listen_hosts, ", "));
}

ClickHouseCatalog::ClickHouseCatalog(
    const std::string & warehouse_,
    const std::string & catalog_user_,
    bool flat_namespaces_,
    DB::ContextPtr context_,
    DB::DatabaseDataLakeSnapshotLoadingMode snapshot_loading_mode_)
    : RestCatalog(
        warehouse_,
        getCatalogURL(context_->getConfigRef()),
        /* auth_scope_ */ "",
        /* oauth_server_uri_ */ "",
        /* oauth_server_use_request_body_ */ false,
        flat_namespaces_,
        context_,
        snapshot_loading_mode_)
    , catalog_user(catalog_user_)
{
    /// `loadConfig` fetches the catalog settings from the REST `/v1/config` endpoint.
    /// That request must carry the loopback headers from `getAuthHeaders` below.
    /// The base constructor would use the base `getAuthHeaders` instead, so it skips `loadConfig`.
    /// Load the catalog config here, where the override is in place.
    CatalogState initial_state;
    initial_state.config = loadConfig(initial_state);
    state.set(std::make_unique<const CatalogState>(std::move(initial_state)));
}

DB::HTTPHeaderEntries ClickHouseCatalog::getAuthHeaders(const CatalogState &, bool) const
{
    DB::HTTPHeaderEntries headers;
    headers.emplace_back(DB::IcebergRESTCatalog::LOOPBACK_USER_HEADER, catalog_user);
    headers.emplace_back(DB::IcebergRESTCatalog::LOOPBACK_SECRET_HEADER, getContext()->getIcebergRESTCatalogSecret());
    return headers;
}

}

#endif
