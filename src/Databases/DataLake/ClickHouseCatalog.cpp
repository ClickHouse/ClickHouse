#include <Databases/DataLake/ClickHouseCatalog.h>

#if USE_AVRO

#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogService.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Poco/Net/HTTPRequest.h>

#include <sstream>

namespace DB::ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

namespace DataLake
{

ClickHouseCatalog::ClickHouseCatalog(
    const std::string & warehouse_,
    bool flat_namespaces_,
    DB::ContextPtr context_,
    DB::DatabaseDataLakeSnapshotLoadingMode snapshot_loading_mode_)
    : RestCatalog(
        warehouse_,
        /* base_url_ */ "/",
        /* auth_scope_ */ "",
        /* oauth_server_uri_ */ "",
        /* oauth_server_use_request_body_ */ false,
        flat_namespaces_,
        context_,
        snapshot_loading_mode_)
    , service(DB::createIcebergRESTCatalogService(context_->getGlobalContext()))
{
    /// Repeats config loading from `RestCatalog` constructor. Needed so `loadConfig` calls overridden `createReadBuffer` instead of HTTP.
    CatalogState initial_state;
    initial_state.config = loadConfig(initial_state);
    state.set(std::make_unique<const CatalogState>(std::move(initial_state)));
}

String ClickHouseCatalog::callService(const String & method, const Poco::URI & uri, const String & body) const
{
    /// ClickHouse checks grants on the database for the user of each query. So catalog checks no user for these requests.
    const auto response = service->handleInternalRequest({.method = method, .uri = uri.getPathAndQuery(), .body = body});

    /// With real HTTP, an error status arrives as `HTTPException`. `RestCatalog` catches it, for example 404 means a missing table.
    /// Throw the same exception for an error status from the service.
    const bool is_error_status = response.status >= Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
    if (is_error_status)
        throw DB::HTTPException(
            DB::ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            uri.toString(),
            response.status,
            Poco::Net::HTTPResponse::getReasonForStatus(response.status),
            response.body);

    return response.body;
}

std::unique_ptr<DB::ReadBuffer> ClickHouseCatalog::createReadBuffer(
    const CatalogState & /* catalog_state */,
    const std::string & endpoint,
    const Poco::URI::QueryParameters & params,
    const DB::HTTPHeaderEntries & /* headers */,
    const std::optional<DB::HTTPHeaderEntries> & /* auth_headers */) const
{
    /// Skip URL encoding. Catalog expects encoded names, like `foo%2Fbar`.
    Poco::URI uri(base_url / endpoint, /* enable_url_encoding */ false);
    if (!params.empty())
        uri.setQueryParameters(params);

    return std::make_unique<DB::ReadBufferFromOwnString>(callService(Poco::Net::HTTPRequest::HTTP_GET, uri, /* body */ {}));
}

String ClickHouseCatalog::sendRequest(
    const CatalogState & /* catalog_state */,
    const String & endpoint,
    Poco::JSON::Object::Ptr request_body,
    const String & method,
    bool ignore_result) const
{
    std::ostringstream oss;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    if (request_body)
        request_body->stringify(oss);
    const std::string body = DB::removeEscapedSlashes(oss.str());

    /// Skip URL encoding. Catalog expects encoded names, like `foo%2Fbar`.
    Poco::URI uri(endpoint, /* enable_url_encoding */ false);

    auto response = callService(method, uri, body);
    return ignore_result ? String{} : response;
}

}

#endif
