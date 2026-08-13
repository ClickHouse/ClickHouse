#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Databases/DataLake/RestCatalog.h>
#include <IO/HTTPCommon.h>
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
#include <Poco/URI.h>

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

enum class CatalogShape
{
    TopLevelTable,
    NestedTableThenEmptySibling,
    Empty,
};

void writeJSON(Poco::Net::HTTPServerResponse & response, const std::string & body)
{
    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/json");
    response.setContentLength(body.size());
    response.send() << body;
}

void writeError(Poco::Net::HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status, const std::string & body)
{
    response.setStatus(status);
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

class RestCatalogRequestHandler final : public Poco::Net::HTTPRequestHandler
{
public:
    explicit RestCatalogRequestHandler(CatalogShape shape_)
        : shape(shape_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        Poco::URI uri(request.getURI());
        const auto path = getRawPath(request.getURI());
        const auto params = uri.getQueryParameters();

        if (path == "/v1/config")
        {
            writeJSON(response, R"({"defaults":{},"overrides":{}})");
            return;
        }

        if (path == "/v1/namespaces")
        {
            const auto parent = getParent(params);
            if (parent.empty())
            {
                if (shape == CatalogShape::NestedTableThenEmptySibling)
                    writeJSON(response, R"({"namespaces":[["parent"],["empty_later"]]})");
                else
                    writeJSON(response, R"({"namespaces":[["namespace"]]})");
                return;
            }

            if (shape == CatalogShape::NestedTableThenEmptySibling && parent == "parent")
                writeJSON(response, R"({"namespaces":[["leaf_with_table"]]})");
            else
                writeJSON(response, R"({"namespaces":[]})");
            return;
        }

        if (path == "/v1/namespaces/namespace/tables")
        {
            if (shape == CatalogShape::TopLevelTable)
                writeJSON(response, R"({"identifiers":[{"name":"table_a"}]})");
            else
                writeJSON(response, R"({"identifiers":[]})");
            return;
        }

        if (path == "/v1/namespaces/parent/tables"
            || path == "/v1/namespaces/empty_later/tables")
        {
            writeJSON(response, R"({"identifiers":[]})");
            return;
        }

        if (path == "/v1/namespaces/parent%1Fleaf_with_table/tables")
        {
            writeJSON(response, R"({"identifiers":[{"name":"table_a"}]})");
            return;
        }

        if (path == "/v1/namespaces/namespace/tables/table_a")
        {
            writeJSON(response, R"({"metadata":{"table-uuid":"11111111-2222-3333-4444-555555555555"}})");
            return;
        }

        if (path == "/v1/namespaces/namespace/tables/missing_table")
        {
            writeError(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, R"({"error":{"message":"Table does not exist","type":"NoSuchTableException","code":404}})");
            return;
        }

        if (path == "/v1/namespaces/namespace/tables/unauthorized_table")
        {
            writeError(response, Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED, R"({"error":{"message":"The access token has expired","type":"NotAuthorizedException","code":401}})");
            return;
        }

        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected request to fake Iceberg REST catalog: {}", request.getURI());
    }

private:
    static std::string getParent(const Poco::URI::QueryParameters & params)
    {
        for (const auto & [key, value] : params)
        {
            if (key == "parent")
                return value;
        }
        return {};
    }

    CatalogShape shape;
};

class RestCatalogRequestHandlerFactory final : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit RestCatalogRequestHandlerFactory(CatalogShape shape_)
        : shape(shape_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new RestCatalogRequestHandler(shape);
    }

private:
    CatalogShape shape;
};

class RestCatalogTestServer
{
public:
    explicit RestCatalogTestServer(CatalogShape shape)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0)))
        , handler_factory(new RestCatalogRequestHandlerFactory(shape))
        , server_params(new Poco::Net::HTTPServerParams())
        , server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    ~RestCatalogTestServer()
    {
        server->stop();
    }

    std::string getUrl() const
    {
        return "http://" + server_socket->address().toString();
    }

private:
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<RestCatalogRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

bool restCatalogEmpty(CatalogShape shape)
{
    RestCatalogTestServer server(shape);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    RestCatalog catalog(
        "warehouse",
        server.getUrl(),
        /* catalog_credential */"",
        /* auth_scope */"",
        /* auth_header */"",
        /* oauth_server_uri */"",
        /* oauth_server_use_request_body */false,
        context);

    return catalog.empty();
}

}

TEST(RestCatalog, EmptyReturnsFalseForTopLevelTable)
{
    EXPECT_FALSE(restCatalogEmpty(CatalogShape::TopLevelTable));
}

TEST(RestCatalog, EmptyKeepsFoundTableStateSticky)
{
    EXPECT_FALSE(restCatalogEmpty(CatalogShape::NestedTableThenEmptySibling));
}

TEST(RestCatalog, EmptyReturnsTrueWhenNoTablesExist)
{
    EXPECT_TRUE(restCatalogEmpty(CatalogShape::Empty));
}

TEST(RestCatalog, TryGetTableMetadataDistinguishesMissingTableFromOtherErrors)
{
    RestCatalogTestServer server(CatalogShape::TopLevelTable);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    RestCatalog catalog(
        "warehouse",
        server.getUrl(),
        /* catalog_credential */"",
        /* auth_scope */"",
        /* auth_header */"",
        /* oauth_server_uri */"",
        /* oauth_server_use_request_body */false,
        context);

    TableMetadata existing;
    EXPECT_TRUE(catalog.tryGetTableMetadata("namespace", "table_a", existing));
    EXPECT_TRUE(catalog.existsTable("namespace", "table_a"));

    TableMetadata missing;
    EXPECT_FALSE(catalog.tryGetTableMetadata("namespace", "missing_table", missing));
    EXPECT_FALSE(catalog.existsTable("namespace", "missing_table"));

    TableMetadata unauthorized;
    EXPECT_THROW(catalog.tryGetTableMetadata("namespace", "unauthorized_table", unauthorized), DB::HTTPException);
    EXPECT_THROW(catalog.existsTable("namespace", "unauthorized_table"), DB::HTTPException);
}

#endif
