#include "config.h"

#if USE_AVRO && USE_SSL && USE_AWS_S3

#include <Databases/DataLake/S3TablesCatalog.h>
#include <Databases/DataLake/AWSV4Signer.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/S3/Client.h>
#include <IO/S3/URI.h>
#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/String.h>
#include <Poco/URI.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>

#include <mutex>
#include <sstream>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DATALAKE_DATABASE_ERROR;
}

namespace DB::Setting
{
    extern const SettingsUInt64 s3_max_connections;
    extern const SettingsUInt64 s3_max_redirects;
    extern const SettingsUInt64 s3_retry_attempts;
    extern const SettingsBool s3_slow_all_threads_after_network_error;
    extern const SettingsBool enable_s3_requests_logging;
    extern const SettingsUInt64 s3_connect_timeout_ms;
    extern const SettingsUInt64 s3_request_timeout_ms;
}

namespace DB::ServerSetting
{
    extern const ServerSettingsUInt64 s3_max_redirects;
    extern const ServerSettingsUInt64 s3_retry_attempts;
}

namespace DataLake
{

S3TablesCatalog::S3TablesCatalog(
    const String & warehouse_,
    const String & base_url_,
    const String & region_,
    const CatalogSettings & catalog_settings_,
    DB::ContextPtr context_)
    : RestCatalog(warehouse_, base_url_, "", "", false, context_)
    , region(region_)
    , storage_endpoint(catalog_settings_.storage_endpoint)
    , signing_service("s3tables")
{
    if (region.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "S3 Tables catalog requires non-empty `region` setting");

    DB::S3::CredentialsConfiguration creds_config;
    creds_config.use_environment_credentials = true;
    creds_config.role_arn = catalog_settings_.aws_role_arn;
    creds_config.role_session_name = catalog_settings_.aws_role_session_name;

    const auto & server_settings = getContext()->getGlobalContext()->getServerSettings();
    const DB::Settings & global_settings = getContext()->getGlobalContext()->getSettingsRef();

    int s3_max_redirects = static_cast<int>(server_settings[DB::ServerSetting::s3_max_redirects]);
    if (global_settings.isChanged("s3_max_redirects"))
        s3_max_redirects = static_cast<int>(global_settings[DB::Setting::s3_max_redirects]);

    int s3_retry_attempts = static_cast<int>(server_settings[DB::ServerSetting::s3_retry_attempts]);
    if (global_settings.isChanged("s3_retry_attempts"))
        s3_retry_attempts = static_cast<int>(global_settings[DB::Setting::s3_retry_attempts]);

    bool s3_slow_all_threads_after_network_error = global_settings[DB::Setting::s3_slow_all_threads_after_network_error];
    bool s3_slow_all_threads_after_retryable_error = false;
    bool enable_s3_requests_logging = global_settings[DB::Setting::enable_s3_requests_logging];

    DB::S3::PocoHTTPClientConfiguration poco_config = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        getContext()->getRemoteHostFilter(),
        s3_max_redirects,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = static_cast<unsigned>(s3_retry_attempts)},
        s3_slow_all_threads_after_network_error,
        s3_slow_all_threads_after_retryable_error,
        enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {});

    Aws::Auth::AWSCredentials credentials(catalog_settings_.aws_access_key_id, catalog_settings_.aws_secret_access_key);
    credentials_provider = DB::S3::getCredentialsProvider(poco_config, credentials, creds_config);

    signer = std::make_unique<Aws::Client::AWSAuthV4Signer>(
        credentials_provider,
        "s3tables",
        Aws::String(region.data(), region.size()),
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always,
        /* urlEscapePath = */ false);

    config = loadConfig();

    if (config.prefix.empty())
    {
        String encoded_warehouse;
        Poco::URI::encode(warehouse_, "", encoded_warehouse);
        config.prefix = encoded_warehouse;
    }
}

/// S3 Tables only supports a single level of namespaces (no nesting),
/// so we use flat getNamespaces() instead of the base class's getNamespacesRecursive().
DB::Names S3TablesCatalog::getTables() const
{
    auto namespaces = getNamespaces("");

    auto & pool = getContext()->getIcebergCatalogThreadpool();
    DB::ThreadPoolCallbackRunnerLocal<void> runner(pool, DB::ThreadName::DATALAKE_REST_CATALOG);

    DB::Names tables;
    std::mutex mutex;
    for (const auto & ns : namespaces)
    {
        runner.enqueueAndKeepTrack(
            [&, ns]
            {
                auto tables_in_ns = RestCatalog::getTables(ns);
                std::lock_guard lock(mutex);
                std::move(tables_in_ns.begin(), tables_in_ns.end(), std::back_inserter(tables));
            });
    }
    runner.waitForAllToFinishAndRethrowFirstError();
    return tables;
}

bool S3TablesCatalog::tryGetTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    if (!RestCatalog::tryGetTableMetadata(namespace_name, table_name, result))
        return false;

    if (!result.requiresCredentials())
        return true;

    bool need_credentials = true;
    if (const auto storage_credentials = result.getStorageCredentials())
    {
        auto creds = std::dynamic_pointer_cast<S3Credentials>(storage_credentials);
        if (creds && !creds->isEmpty())
            need_credentials = false;
    }

    if (need_credentials)
    {
        LOG_DEBUG(log, "S3 Tables: no vended credentials for {}.{}, injecting catalog IAM credentials", namespace_name, table_name);
        auto aws_creds = credentials_provider->GetAWSCredentials();
        result.setStorageCredentials(std::make_shared<S3Credentials>(
            aws_creds.GetAWSAccessKeyId(), aws_creds.GetAWSSecretKey(), aws_creds.GetSessionToken()));
    }

    if (result.getEndpoint().empty())
    {
        String endpoint = storage_endpoint.empty()
            ? DB::S3::expandRegionToAmazonPath(region)
            : storage_endpoint;
        LOG_DEBUG(log, "S3 Tables: no endpoint for {}.{}, injecting: {}", namespace_name, table_name, endpoint);
        result.setEndpoint(endpoint);
    }

    if (auto props = result.getDataLakeSpecificProperties();
        props && !props->iceberg_metadata_file_location.empty())
    {
        const String & loc = props->iceberg_metadata_file_location;
        auto scheme_end = loc.find("://");
        if (scheme_end != String::npos)
        {
            auto path_start = loc.find('/', scheme_end + 3);
            if (path_start != String::npos)
                props->iceberg_metadata_file_location = loc.substr(path_start + 1);
        }
        result.setDataLakeSpecificProperties(std::move(props));
    }

    return true;
}

void S3TablesCatalog::dropTable(const String & namespace_name, const String & table_name) const
{
    const std::string endpoint
        = (base_url / config.prefix / "namespaces" / namespace_name / "tables" / table_name).string()
        + "?purgeRequested=True";

    Poco::JSON::Object::Ptr request_body = nullptr;
    try
    {
        sendRequest(endpoint, request_body, Poco::Net::HTTPRequest::HTTP_DELETE, true);
    }
    catch (const DB::HTTPException & ex)
    {
        if (ex.getHTTPStatus() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
            // 404 is returned by the API when the table does not exist
            LOG_DEBUG(log, "S3 Tables: table {}.{} already does not exist (404 on purge-delete)", namespace_name, table_name);
        else
            throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Failed to drop table {}", ex.displayText());
    }
}

namespace
{

/// `signRequestWithAWSV4` returns the full set of headers that the AWS SDK kept on
/// the signed request (input headers + signed auth). We only want the SigV4-specific
/// ones here (`Authorization`, `X-Amz-*`); the original request headers are appended
/// separately by the caller. HTTP header names are case-insensitive, and the AWS SDK
/// is free to vary the casing it emits, so compare using `Poco::icompare`.
bool isSigV4AuthHeader(const String & name)
{
    if (Poco::icompare(name, "authorization") == 0)
        return true;
    static constexpr size_t x_amz_prefix_len = 6;
    return name.size() >= x_amz_prefix_len
        && Poco::icompare(name, 0, x_amz_prefix_len, "x-amz-") == 0;
}

DB::HTTPHeaderEntries extractSigV4AuthHeaders(DB::HTTPHeaderEntries && all_signed)
{
    DB::HTTPHeaderEntries auth_headers;
    for (auto & h : all_signed)
    {
        if (isSigV4AuthHeader(h.name))
            auth_headers.push_back(std::move(h));
    }
    return auth_headers;
}

}

DB::ReadWriteBufferFromHTTPPtr S3TablesCatalog::createReadBuffer(
    const std::string & endpoint,
    const Poco::URI::QueryParameters & params,
    const DB::HTTPHeaderEntries & headers) const
{
    const auto & context = getContext();

    /// enable_url_encoding=false to allow use tables with encoded sequences in names like 'foo%2Fbar'
    Poco::URI url(base_url / endpoint, /* enable_url_encoding */ false);
    if (!params.empty())
        url.setQueryParameters(params);

    DB::HTTPHeaderEntries all_signed;
    signRequestWithAWSV4(
        Poco::Net::HTTPRequest::HTTP_GET, url, headers, /* payload */ {},
        *signer, region, signing_service, all_signed);

    auto result_headers = extractSigV4AuthHeaders(std::move(all_signed));
    for (const auto & h : headers)
        result_headers.push_back(h);

    LOG_DEBUG(log, "Requesting (SigV4): {}", url.toString());

    return DB::BuilderRWBufferFromHTTP(url)
        .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
        .withSettings(context->getReadSettings())
        .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(result_headers)
        .withDelayInit(false)
        .withSkipNotFound(false)
        .create(credentials);
}

void S3TablesCatalog::sendRequest(
    const String & endpoint,
    Poco::JSON::Object::Ptr request_body,
    const String & method,
    bool ignore_result) const
{
    std::ostringstream oss;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    if (request_body)
        request_body->stringify(oss);
    const std::string body_str = DB::removeEscapedSlashes(oss.str());

    const auto & context = getContext();

    DB::ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback;
    if (!body_str.empty())
    {
        out_stream_callback = [body_str](std::ostream & os) { os << body_str; };
    }

    /// enable_url_encoding=false to allow use tables with encoded sequences in names like 'foo%2Fbar'
    Poco::URI url(endpoint, /* enable_url_encoding */ false);

    DB::HTTPHeaderEntries extra_headers;
    extra_headers.emplace_back("Content-Type", "application/json");

    DB::HTTPHeaderEntries all_signed;
    signRequestWithAWSV4(method, url, extra_headers, body_str, *signer, region, signing_service, all_signed);

    auto headers = extractSigV4AuthHeaders(std::move(all_signed));
    headers.emplace_back("Content-Type", "application/json");

    auto wb = DB::BuilderRWBufferFromHTTP(url)
        .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
        .withMethod(method)
        .withSettings(context->getReadSettings())
        .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(headers)
        .withOutCallback(out_stream_callback)
        .withSkipNotFound(false)
        .create(credentials);

    String response_str;
    if (!ignore_result)
        readJSONObjectPossiblyInvalid(response_str, *wb);
    else
        wb->ignoreAll();
}

}

#endif
