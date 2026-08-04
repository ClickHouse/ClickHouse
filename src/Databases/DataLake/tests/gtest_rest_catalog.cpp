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

/// Fake Iceberg REST catalog with a single top-level namespace holding `table_a`.
class RestCatalogRequestHandler final : public Poco::Net::HTTPRequestHandler
{
public:
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

        if (path == "/v1/oauth/tokens")
        {
            writeJSON(response, R"({"token_type":"Bearer","expires_in":3600,"access_token":"mock-access-token"})");
            return;
        }

        if (path == "/v1/namespaces")
        {
            if (getParent(params).empty())
                writeJSON(response, R"({"namespaces":[["namespace"]]})");
            else
                writeJSON(response, R"({"namespaces":[]})");
            return;
        }

        if (path == "/v1/namespaces/namespace/tables")
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
};

class RestCatalogRequestHandlerFactory final : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new RestCatalogRequestHandler();
    }
};

class RestCatalogTestServer
{
public:
    RestCatalogTestServer()
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0)))
        , handler_factory(new RestCatalogRequestHandlerFactory())
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

}

TEST(RestCatalog, TryGetTableMetadataDistinguishesMissingTableFromOtherErrors)
{
    RestCatalogTestServer server;
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
