#include <Databases/DataLake/ClickHouseCatalog.h>

#if USE_AVRO

#include <optional>
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

Poco::Net::IPAddress ClickHouseCatalog::getLoopbackAddress(const Poco::Util::AbstractConfiguration & config)
{
    /// Same default as `getListenHosts` in `Server.cpp`.
    auto listen_hosts = DB::getMultipleValuesFromConfig(config, "", "listen_host");
    if (listen_hosts.empty())
        listen_hosts = {"::1", "127.0.0.1"};

    auto find_loopback_address = [&](Poco::Net::IPAddress::Family family) -> std::optional<Poco::Net::IPAddress>
    {
        for (const auto & host : listen_hosts)
        {
            Poco::Net::IPAddress address;
            if (!Poco::Net::IPAddress::tryParse(host, address) || address.family() != family)
                continue;
            /// Every `127.x.x.x` address is loopback. The catalog listens only on the configured one.
            if (address.isLoopback())
                return address;
            /// A wildcard address also accepts loopback connections.
            if (address.isWildcard())
                return Poco::Net::IPAddress(family == Poco::Net::IPAddress::IPv4 ? "127.0.0.1" : "::1");
        }
        return {};
    };

    /// Prefer IPv4. The server lists `::1` by default but may not bind it on a host without IPv6.
    if (auto address = find_loopback_address(Poco::Net::IPAddress::IPv4))
        return *address;
    if (auto address = find_loopback_address(Poco::Net::IPAddress::IPv6))
        return *address;

    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
        "catalog_type = 'clickhouse' requires the server to listen on a loopback address, got listen_host: {}",
        fmt::join(listen_hosts, ", "));
}

std::string ClickHouseCatalog::getCatalogURL(const Poco::Util::AbstractConfiguration & config)
{
    const auto port = config.getString("iceberg_rest_catalog.port", "");
    if (port.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
            "catalog_type = 'clickhouse' requires iceberg_rest_catalog.port in the server config");

    const auto address = getLoopbackAddress(config);
    if (address.family() == Poco::Net::IPAddress::IPv6)
        return fmt::format("http://[{}]:{}", address.toString(), port);
    return fmt::format("http://{}:{}", address.toString(), port);
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
