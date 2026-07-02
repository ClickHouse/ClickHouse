#include "config.h"

#if USE_PARQUET

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Databases/DataLake/UnityCatalog.h>
#include <Databases/DataLake/ICatalog.h>
#include <Interpreters/Context.h>

#include <Poco/AutoPtr.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/SharedPtr.h>

#include <memory>
#include <string>

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
///   GET /tables/{full_name} -> table metadata JSON
class UnityCatalogRequestHandler final : public Poco::Net::HTTPRequestHandler
{
public:
    explicit UnityCatalogRequestHandler(TableShape shape_)
        : shape(shape_)
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override
    {
        const auto path = getRawPath(request.getURI());

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
                    "table_id": "11111111-1111-1111-1111-111111111111",
                    "columns": [
                        {"name": "id", "type_text": "int", "type_json": "\"int\"", "nullable": false, "position": 0}
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
                    "table_id": "22222222-2222-2222-2222-222222222222",
                    "columns": [
                        {"name": "id", "type_text": "int", "type_json": "\"int\"", "nullable": false, "position": 0}
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
                    "table_id": "33333333-3333-3333-3333-333333333333",
                    "columns": [
                        {"name": "id", "type_text": "int", "type_json": "\"int\"", "nullable": false, "position": 0}
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
                    "table_id": "44444444-4444-4444-4444-444444444444",
                    "columns": [
                        {"name": "id", "type_text": "int", "type_json": "\"int\"", "nullable": false, "position": 0}
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
    TableShape shape;
};

class UnityCatalogRequestHandlerFactory final
    : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit UnityCatalogRequestHandlerFactory(TableShape shape_)
        : shape(shape_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(
        const Poco::Net::HTTPServerRequest &) override
    {
        return new UnityCatalogRequestHandler(shape);
    }

private:
    TableShape shape;
};

/// Lightweight mock Unity Catalog HTTP server.
class UnityCatalogTestServer
{
public:
    explicit UnityCatalogTestServer(TableShape shape)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(
              Poco::Net::SocketAddress("127.0.0.1", 0)))
        , handler_factory(new UnityCatalogRequestHandlerFactory(shape))
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

private:
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
};

/// Create a UnityCatalog pointing at the mock server, fetch metadata
/// for the given schema/table, and return whether it was readable plus
/// the resolved location.
TableMetadataResult fetchTableMetadata(
    TableShape shape,
    const std::string & schema_name,
    const std::string & table_name)
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

    catalog.tryGetTableMetadata(schema_name, table_name, metadata);

    TableMetadataResult result;
    result.readable = metadata.isDefaultReadableTable();
    if (result.readable)
        result.location = metadata.getLocation();
    else
        result.unreadable_reason = metadata.getReasonWhyTableIsUnreadable();

    return result;
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

#endif
