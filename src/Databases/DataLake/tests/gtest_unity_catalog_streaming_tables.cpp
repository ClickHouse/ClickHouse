#include "config.h"

#if USE_PARQUET

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Databases/DataLake/UnityCatalog.h>
#include <Databases/DataLake/ICatalog.h>
#include <Databases/DataLake/StorageCredentials.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <Poco/AutoPtr.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/SharedPtr.h>

#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

using namespace DataLake;

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace
{

const std::string STREAMING_TABLE_ID = "11111111-1111-1111-1111-111111111111";
const std::string STREAMING_TABLE_FALLBACK_ID = "22222222-2222-2222-2222-222222222222";
const std::string MATERIALIZED_VIEW_ID = "33333333-3333-3333-3333-333333333333";
const std::string UNKNOWN_VIEW_ID = "44444444-4444-4444-4444-444444444444";

/// Enum describing which table metadata scenario the mock Unity Catalog
/// server should return.
enum class TableShape
{
    /// Streaming table WITH storage_location and
    /// securable_kind = TABLE_STREAMING_LIVE_TABLE.
    StreamingTableWithLocation,
    /// Streaming table WITHOUT storage_location but WITH
    /// properties.spark.internal.streaming_table.backing_table_path.
    StreamingTableFallbackPath,
    /// Materialized view WITHOUT storage_location but WITH
    /// properties.spark.internal.pipelines.backing_table_path.
    MaterializedViewFallbackPath,
    /// Unknown securable_kind (TABLE_VIEW) with storage_location.
    UnknownSecurableKind,
};

/// Log of requests to the mock temporary-table-credentials endpoint,
/// shared between the test body and the HTTP handlers.
struct CredentialsRequestLog
{
    struct Entry
    {
        std::string table_id;
        std::string operation;
    };

    std::mutex mutex;
    std::vector<Entry> requests;

    std::vector<Entry> getRequests()
    {
        std::lock_guard lock(mutex);
        return requests;
    }
};

void writeJSON(Poco::Net::HTTPServerResponse & response, const std::string & body)
{
    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/json");
    response.setContentLength(body.size());
    response.send() << body;
}

std::string getRawPath(const std::string & uri)
{
    const auto query_pos = uri.find('?');
    if (query_pos == std::string::npos)
        return uri;
    return uri.substr(0, query_pos);
}

/// Mock HTTP handler that serves Unity Catalog API responses.
/// Routes:
///   GET  /tables/{full_name}           -> table metadata JSON
///   POST /temporary-table-credentials  -> vended S3 credentials
class UnityCatalogRequestHandler final : public Poco::Net::HTTPRequestHandler
{
public:
    UnityCatalogRequestHandler(TableShape shape_, std::shared_ptr<CredentialsRequestLog> credentials_log_)
        : shape(shape_)
        , credentials_log(std::move(credentials_log_))
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override
    {
        const auto path = getRawPath(request.getURI());

        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
            && path == "/temporary-table-credentials")
        {
            handleCredentialsRequest(request, response);
            return;
        }

        if (path == "/tables/unity.default.streaming_table")
        {
            if (shape == TableShape::StreamingTableWithLocation)
            {
                writeJSON(response, R"({
                    "name": "streaming_table",
                    "catalog_name": "unity",
                    "schema_name": "default",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "securable_kind": "TABLE_STREAMING_LIVE_TABLE",
                    "storage_location": "s3://test-bucket/streaming_table",
                    "table_id": ")" + STREAMING_TABLE_ID + R"(",
                    "columns": [
                        {"name": "id", "type_text": "int", "type_name": "INT", "type_json": "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}", "nullable": false, "position": 0}
                    ],
                    "properties": {}
                })");
                return;
            }

            if (shape == TableShape::StreamingTableFallbackPath)
            {
                writeJSON(response, R"({
                    "name": "streaming_table",
                    "catalog_name": "unity",
                    "schema_name": "default",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "securable_kind": "TABLE_STREAMING_LIVE_TABLE",
                    "table_id": ")" + STREAMING_TABLE_FALLBACK_ID + R"(",
                    "columns": [
                        {"name": "id", "type_text": "int", "type_name": "INT", "type_json": "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}", "nullable": false, "position": 0}
                    ],
                    "properties": {
                        "spark.internal.streaming_table.backing_table_path": "s3://test-bucket/streaming_table_backing"
                    }
                })");
                return;
            }
        }

        if (path == "/tables/unity.default.mv_table")
        {
            if (shape == TableShape::MaterializedViewFallbackPath)
            {
                writeJSON(response, R"({
                    "name": "mv_table",
                    "catalog_name": "unity",
                    "schema_name": "default",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "securable_kind": "TABLE_MATERIALIZED_VIEW",
                    "table_id": ")" + MATERIALIZED_VIEW_ID + R"(",
                    "columns": [
                        {"name": "id", "type_text": "int", "type_name": "INT", "type_json": "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}", "nullable": false, "position": 0}
                    ],
                    "properties": {
                        "spark.internal.pipelines.backing_table_path": "s3://test-bucket/mv_backing"
                    }
                })");
                return;
            }
        }

        if (path == "/tables/unity.default.unknown_view")
        {
            if (shape == TableShape::UnknownSecurableKind)
            {
                writeJSON(response, R"({
                    "name": "unknown_view",
                    "catalog_name": "unity",
                    "schema_name": "default",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "securable_kind": "TABLE_VIEW",
                    "storage_location": "s3://test-bucket/unknown_view",
                    "table_id": ")" + UNKNOWN_VIEW_ID + R"(",
                    "columns": [
                        {"name": "id", "type_text": "int", "type_name": "INT", "type_json": "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}", "nullable": false, "position": 0}
                    ],
                    "properties": {}
                })");
                return;
            }
        }

        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Unexpected request to mock Unity Catalog: {}",
            request.getURI());
    }

private:
    void handleCredentialsRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response)
    {
        std::string body{std::istreambuf_iterator<char>(request.stream()), std::istreambuf_iterator<char>{}};
        Poco::JSON::Parser parser;
        const auto object = parser.parse(body).extract<Poco::JSON::Object::Ptr>();

        size_t request_number;
        {
            std::lock_guard lock(credentials_log->mutex);
            credentials_log->requests.push_back(
                {object->getValue<std::string>("table_id"), object->getValue<std::string>("operation")});
            request_number = credentials_log->requests.size();
        }

        /// Return a different session token on every request, so the tests can
        /// distinguish the initial credentials from a refreshed set.
        writeJSON(response, R"({
            "aws_temp_credentials": {
                "access_key_id": "test-access-key",
                "secret_access_key": "test-secret-key",
                "session_token": "session-token-)" + std::to_string(request_number) + R"("
            }
        })");
    }

    TableShape shape;
    std::shared_ptr<CredentialsRequestLog> credentials_log;
};

class UnityCatalogRequestHandlerFactory final
    : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    UnityCatalogRequestHandlerFactory(TableShape shape_, std::shared_ptr<CredentialsRequestLog> credentials_log_)
        : shape(shape_)
        , credentials_log(std::move(credentials_log_))
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(
        const Poco::Net::HTTPServerRequest &) override
    {
        return new UnityCatalogRequestHandler(shape, credentials_log);
    }

private:
    TableShape shape;
    std::shared_ptr<CredentialsRequestLog> credentials_log;
};

/// Lightweight mock Unity Catalog HTTP server.
class UnityCatalogTestServer
{
public:
    explicit UnityCatalogTestServer(TableShape shape)
        : credentials_log(std::make_shared<CredentialsRequestLog>())
        , server_socket(std::make_unique<Poco::Net::ServerSocket>(
              Poco::Net::SocketAddress("127.0.0.1", 0)))
        , handler_factory(new UnityCatalogRequestHandlerFactory(shape, credentials_log))
        , server_params(new Poco::Net::HTTPServerParams())
        , server(std::make_unique<Poco::Net::HTTPServer>(
              handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    ~UnityCatalogTestServer()
    {
        server->stop();
    }

    std::string getUrl() const
    {
        return "http://" + server_socket->address().toString();
    }

    std::shared_ptr<CredentialsRequestLog> getCredentialsLog() const
    {
        return credentials_log;
    }

private:
    std::shared_ptr<CredentialsRequestLog> credentials_log;
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<UnityCatalogRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

/// Helper result struct.
struct TableMetadataResult
{
    bool readable = false;
    std::string location;
    std::string unreadable_reason;
    std::shared_ptr<S3Credentials> s3_credentials;
    std::vector<CredentialsRequestLog::Entry> credentials_requests;
};

/// Create a UnityCatalog pointing at the mock server, fetch metadata
/// for the given schema/table, and return whether it was readable plus
/// the resolved location (and, when requested, the vended credentials).
TableMetadataResult fetchTableMetadata(
    TableShape shape,
    const std::string & schema_name,
    const std::string & table_name,
    bool with_storage_credentials = false)
{
    UnityCatalogTestServer server(shape);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    UnityCatalog catalog(
        "unity",
        server.getUrl(),
        /* catalog_credential */ "",
        context);

    TableMetadata metadata;
    metadata.withLocation();
    metadata.withSchema();
    if (with_storage_credentials)
        metadata.withStorageCredentials();

    catalog.tryGetTableMetadata(schema_name, table_name, metadata);

    TableMetadataResult result;
    result.readable = metadata.isDefaultReadableTable();
    if (result.readable)
        result.location = metadata.getLocation();
    else
        result.unreadable_reason = metadata.getReasonWhyTableIsUnreadable();

    if (with_storage_credentials)
        result.s3_credentials = std::dynamic_pointer_cast<S3Credentials>(metadata.getStorageCredentials());
    result.credentials_requests = server.getCredentialsLog()->getRequests();

    return result;
}

DB::UUID parseUUID(const std::string & text)
{
    DB::UUID uuid;
    DB::ReadBufferFromString in(text);
    DB::readUUIDText(uuid, in);
    return uuid;
}

} // anonymous namespace


// =========================================================================
// Tests
// =========================================================================

/// Streaming table (TABLE_STREAMING_LIVE_TABLE) with an explicit
/// storage_location: must be readable and location resolved from
/// storage_location.
TEST(UnityCatalogStreamingTables, StreamingTableWithStorageLocation)
{
    auto result = fetchTableMetadata(
        TableShape::StreamingTableWithLocation,
        "default", "streaming_table");

    EXPECT_TRUE(result.readable);
    EXPECT_EQ(result.location, "s3://test-bucket/streaming_table");
}

/// Streaming table without storage_location but with
/// properties.spark.internal.streaming_table.backing_table_path:
/// must resolve location from the properties field.
///
/// Before PR #96825 this would fail with DATALAKE_DATABASE_ERROR
/// because the properties fallback did not exist.
TEST(UnityCatalogStreamingTables, StreamingTableFallbackToProperties)
{
    auto result = fetchTableMetadata(
        TableShape::StreamingTableFallbackPath,
        "default", "streaming_table");

    EXPECT_TRUE(result.readable);
    EXPECT_EQ(result.location, "s3://test-bucket/streaming_table_backing");
}

/// Materialized view (TABLE_MATERIALIZED_VIEW) without storage_location
/// but with properties.spark.internal.pipelines.backing_table_path:
/// must resolve location from the properties field.
TEST(UnityCatalogStreamingTables, MaterializedViewFallbackToProperties)
{
    auto result = fetchTableMetadata(
        TableShape::MaterializedViewFallbackPath,
        "default", "mv_table");

    EXPECT_TRUE(result.readable);
    EXPECT_EQ(result.location, "s3://test-bucket/mv_backing");
}

/// Table with an unsupported securable_kind (TABLE_VIEW):
/// must be marked as NOT readable.
TEST(UnityCatalogStreamingTables, UnknownSecurableKindNotReadable)
{
    auto result = fetchTableMetadata(
        TableShape::UnknownSecurableKind,
        "default", "unknown_view");

    EXPECT_FALSE(result.readable);
    EXPECT_TRUE(
        result.unreadable_reason.find("unsupported securable_kind") != std::string::npos
        || result.unreadable_reason.find("TABLE_VIEW") != std::string::npos);
}

/// DataLakeCatalog enables vended credentials by default, so real reads do
/// not stop at location resolution: they also request temporary table
/// credentials. For a streaming table resolved through the properties
/// fallback, the Delta path belongs to the backing table while credentials
/// must still be requested for the table_id of the catalog object itself.
TEST(UnityCatalogStreamingTables, StreamingTableVendedCredentials)
{
    auto result = fetchTableMetadata(
        TableShape::StreamingTableFallbackPath,
        "default", "streaming_table",
        /* with_storage_credentials */ true);

    EXPECT_TRUE(result.readable);
    EXPECT_EQ(result.location, "s3://test-bucket/streaming_table_backing");

    ASSERT_NE(result.s3_credentials, nullptr);
    EXPECT_EQ(result.s3_credentials->getAccessKeyId(), "test-access-key");
    EXPECT_EQ(result.s3_credentials->getSecretAccessKey(), "test-secret-key");
    EXPECT_EQ(result.s3_credentials->getSessionToken(), "session-token-1");

    ASSERT_EQ(result.credentials_requests.size(), 1u);
    EXPECT_EQ(result.credentials_requests[0].table_id, STREAMING_TABLE_FALLBACK_ID);
    EXPECT_EQ(result.credentials_requests[0].operation, "READ");
}

/// Same as above for a materialized view: credentials must be requested
/// with the materialized view's own table_id, not anything derived from
/// the backing table path.
TEST(UnityCatalogStreamingTables, MaterializedViewVendedCredentials)
{
    auto result = fetchTableMetadata(
        TableShape::MaterializedViewFallbackPath,
        "default", "mv_table",
        /* with_storage_credentials */ true);

    EXPECT_TRUE(result.readable);
    EXPECT_EQ(result.location, "s3://test-bucket/mv_backing");

    ASSERT_NE(result.s3_credentials, nullptr);
    EXPECT_EQ(result.s3_credentials->getAccessKeyId(), "test-access-key");
    EXPECT_EQ(result.s3_credentials->getSecretAccessKey(), "test-secret-key");
    EXPECT_EQ(result.s3_credentials->getSessionToken(), "session-token-1");

    ASSERT_EQ(result.credentials_requests.size(), 1u);
    EXPECT_EQ(result.credentials_requests[0].table_id, MATERIALIZED_VIEW_ID);
    EXPECT_EQ(result.credentials_requests[0].operation, "READ");
}

/// An unreadable table must NOT trigger a credentials request even when
/// storage credentials are enabled.
TEST(UnityCatalogStreamingTables, UnreadableTableRequestsNoCredentials)
{
    auto result = fetchTableMetadata(
        TableShape::UnknownSecurableKind,
        "default", "unknown_view",
        /* with_storage_credentials */ true);

    EXPECT_FALSE(result.readable);
    EXPECT_EQ(result.s3_credentials, nullptr);
    EXPECT_TRUE(result.credentials_requests.empty());
}

/// The credentials refresh callback built from the StorageID UUID must
/// re-request temporary credentials for the streaming table's table_id
/// on every invocation.
TEST(UnityCatalogStreamingTables, StreamingTableCredentialsRefreshCallback)
{
    UnityCatalogTestServer server(TableShape::StreamingTableFallbackPath);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    UnityCatalog catalog(
        "unity",
        server.getUrl(),
        /* catalog_credential */ "",
        context);

    const DB::StorageID storage_id("unity_db", "default.streaming_table", parseUUID(STREAMING_TABLE_FALLBACK_ID));
    /// The override is private in UnityCatalog, call through the base class
    /// as DatabaseDataLake does.
    ICatalog & base_catalog = catalog;
    auto callback = base_catalog.getCredentialsConfigurationCallback(storage_id);
    ASSERT_TRUE(callback.has_value());

    auto first = std::dynamic_pointer_cast<S3Credentials>((*callback)());
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->getSessionToken(), "session-token-1");

    auto refreshed = std::dynamic_pointer_cast<S3Credentials>((*callback)());
    ASSERT_NE(refreshed, nullptr);
    EXPECT_EQ(refreshed->getSessionToken(), "session-token-2");

    const auto requests = server.getCredentialsLog()->getRequests();
    ASSERT_EQ(requests.size(), 2u);
    for (const auto & request : requests)
    {
        EXPECT_EQ(request.table_id, STREAMING_TABLE_FALLBACK_ID);
        EXPECT_EQ(request.operation, "READ");
    }
}

#endif
