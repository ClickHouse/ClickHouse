#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Databases/DataLake/RestCatalog.h>
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
    /// Flat-namespace catalog (Databricks Delta Sharing style) that ignores the `parent` filter and
    /// echoes the same top-level namespace for every parent. A REST catalog would recurse on this
    /// forever (gold -> gold.gold -> ...); a flat-namespace catalog must list the top level only.
    ParentIgnoringEcho,
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
                else if (shape == CatalogShape::ParentIgnoringEcho)
                    writeJSON(response, R"({"namespaces":[["gold"]]})");
                else
                    writeJSON(response, R"({"namespaces":[["namespace"]]})");
                return;
            }

            if (shape == CatalogShape::NestedTableThenEmptySibling && parent == "parent")
                writeJSON(response, R"({"namespaces":[["leaf_with_table"]]})");
            else if (shape == CatalogShape::ParentIgnoringEcho)
                /// Ignores `parent` and echoes the top-level namespace back for any parent.
                writeJSON(response, R"({"namespaces":[["gold"]]})");
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
            || path == "/v1/namespaces/empty_later/tables"
            || path == "/v1/namespaces/gold/tables")
        {
            writeJSON(response, R"({"identifiers":[]})");
            return;
        }

        if (path == "/v1/namespaces/parent%1Fleaf_with_table/tables")
        {
            writeJSON(response, R"({"identifiers":[{"name":"table_a"}]})");
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

bool deltaSharingCatalogEmpty(CatalogShape shape)
{
    RestCatalogTestServer server(shape);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    DeltaSharingCatalog catalog(
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

TEST(RestCatalog, DeltaSharingTerminatesWhenParentFilterIgnored)
{
    /// Databricks Delta Sharing has flat namespaces and echoes the same namespace for any parent. As
    /// a `DeltaSharingCatalog` it must list the top level only and terminate (a plain REST catalog
    /// would recurse gold -> gold.gold -> ... forever). With no tables under the echoed namespace the
    /// catalog is reported empty.
    EXPECT_TRUE(deltaSharingCatalogEmpty(CatalogShape::ParentIgnoringEcho));
}

TEST(RestCatalog, EmptyKeepsFoundTableStateSticky)
{
    EXPECT_FALSE(restCatalogEmpty(CatalogShape::NestedTableThenEmptySibling));
}

TEST(RestCatalog, EmptyReturnsTrueWhenNoTablesExist)
{
    EXPECT_TRUE(restCatalogEmpty(CatalogShape::Empty));
}

#endif
