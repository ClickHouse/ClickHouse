#include <Databases/DataLake/ClickHouseCatalog.h>

#if USE_AVRO

#include <Interpreters/Context.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogLoopbackAuth.h>

namespace DataLake
{

ClickHouseCatalog::ClickHouseCatalog(
    const std::string & warehouse_,
    const std::string & base_url_,
    const std::string & catalog_user_,
    bool flat_namespaces_,
    DB::ContextPtr context_)
    : RestCatalog(
        warehouse_,
        base_url_,
        /* auth_scope_ */ "",
        /* oauth_server_uri_ */ "",
        /* oauth_server_use_request_body_ */ false,
        flat_namespaces_,
        context_)
    , catalog_user(catalog_user_)
{
    /// The config is loaded here and not in the base constructor, so that the request
    /// already carries the headers of the overridden `getAuthHeaders`.
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
