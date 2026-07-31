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
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
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

        if (path == "/v1/oauth/tokens")
        {
            writeJSON(response, R"({"token_type":"Bearer","expires_in":3600,"access_token":"mock-access-token"})");
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

void expectThrowsCode(std::function<void()> fn, int expected_code)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception with code " << expected_code;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
    }
}

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

TEST(RestCatalog, ApplySettingsChangesWithoutAuthenticationRejected)
{
    RestCatalogTestServer server(CatalogShape::Empty);
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

    DB::SettingsChanges changes;
    changes.emplace_back("catalog_credential", "id:secret");
    expectThrowsCode([&] { catalog.applySettingsChanges(changes); }, DB::ErrorCodes::BAD_ARGUMENTS);
}

TEST(RestCatalog, ApplySettingsChangesCredentialMode)
{
    RestCatalogTestServer server(CatalogShape::Empty);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    RestCatalog catalog(
        "warehouse",
        server.getUrl(),
        /* catalog_credential */"client-1:secret-1",
        /* auth_scope */"scope",
        /* auth_header */"",
        /* oauth_server_uri */"",
        /* oauth_server_use_request_body */false,
        context);

    EXPECT_EQ(catalog.getStateSnapshot()->client_id, "client-1");

    DB::SettingsChanges changes;
    changes.emplace_back("catalog_credential", "client-2:secret-2");
    catalog.applySettingsChanges(changes);

    const auto snapshot = catalog.getStateSnapshot();
    EXPECT_EQ(snapshot->client_id, "client-2");
    EXPECT_EQ(snapshot->client_secret, "secret-2");

    DB::SettingsChanges mode_switch;
    mode_switch.emplace_back("auth_header", "Authorization: Bearer token");
    expectThrowsCode([&] { catalog.applySettingsChanges(mode_switch); }, DB::ErrorCodes::BAD_ARGUMENTS);

    DB::SettingsChanges unknown_setting;
    unknown_setting.emplace_back("warehouse", "other");
    expectThrowsCode([&] { catalog.applySettingsChanges(unknown_setting); }, DB::ErrorCodes::BAD_ARGUMENTS);

    /// Malformed credential (no `:` separator) fails the ALTER atomically.
    DB::SettingsChanges malformed;
    malformed.emplace_back("catalog_credential", "no-separator");
    expectThrowsCode([&] { catalog.applySettingsChanges(malformed); }, DB::ErrorCodes::BAD_ARGUMENTS);
    EXPECT_EQ(catalog.getStateSnapshot()->client_id, "client-2");
}

TEST(RestCatalog, ApplySettingsChangesAuthHeaderMode)
{
    RestCatalogTestServer server(CatalogShape::Empty);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    RestCatalog catalog(
        "warehouse",
        server.getUrl(),
        /* catalog_credential */"",
        /* auth_scope */"",
        /* auth_header */"Authorization: Bearer token-1",
        /* oauth_server_uri */"",
        /* oauth_server_use_request_body */false,
        context);

    DB::SettingsChanges changes;
    changes.emplace_back("auth_header", "Authorization: Bearer token-2");
    catalog.applySettingsChanges(changes);

    const auto snapshot = catalog.getStateSnapshot();
    ASSERT_TRUE(snapshot->auth_header.has_value());
    EXPECT_EQ(snapshot->auth_header->value, " Bearer token-2");

    DB::SettingsChanges mode_switch;
    mode_switch.emplace_back("catalog_credential", "id:secret");
    expectThrowsCode([&] { catalog.applySettingsChanges(mode_switch); }, DB::ErrorCodes::BAD_ARGUMENTS);
}

TEST(RestCatalog, OneLakeApplySettingsChangesBearerMode)
{
    RestCatalogTestServer server(CatalogShape::Empty);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    OneLakeCatalog catalog(
        "warehouse",
        server.getUrl(),
        /* onelake_tenant_id */"tenant-1",
        /* onelake_client_id */"",
        /* onelake_client_secret */"",
        /* bearer_token */"token-1",
        /* auth_scope */"",
        /* oauth_server_uri */"",
        /* oauth_server_use_request_body */false,
        context);

    const auto snapshot_before = catalog.getStateSnapshot();
    EXPECT_EQ(snapshot_before->tenant_id, "tenant-1");
    EXPECT_EQ(snapshot_before->bearer_token, "token-1");
    ASSERT_TRUE(snapshot_before->auth_header.has_value());
    EXPECT_EQ(snapshot_before->auth_header->value, "Bearer token-1");

    DB::SettingsChanges changes;
    changes.emplace_back("onelake_bearer_token", "token-2");
    changes.emplace_back("onelake_tenant_id", "tenant-2");
    catalog.applySettingsChanges(changes);

    const auto snapshot_after = catalog.getStateSnapshot();
    EXPECT_EQ(snapshot_after->tenant_id, "tenant-2");
    EXPECT_EQ(snapshot_after->bearer_token, "token-2");
    ASSERT_TRUE(snapshot_after->auth_header.has_value());
    EXPECT_EQ(snapshot_after->auth_header->value, "Bearer token-2");

    EXPECT_EQ(snapshot_before->tenant_id, "tenant-1");
    EXPECT_EQ(snapshot_before->bearer_token, "token-1");

    DB::SettingsChanges mode_switch;
    mode_switch.emplace_back("onelake_tenant_id", "tenant-3");
    mode_switch.emplace_back("onelake_client_id", "client-1");
    expectThrowsCode([&] { catalog.applySettingsChanges(mode_switch); }, DB::ErrorCodes::BAD_ARGUMENTS);
    EXPECT_EQ(catalog.getStateSnapshot()->tenant_id, "tenant-2");

    DB::SettingsChanges unknown_setting;
    unknown_setting.emplace_back("warehouse", "other");
    expectThrowsCode([&] { catalog.applySettingsChanges(unknown_setting); }, DB::ErrorCodes::BAD_ARGUMENTS);

    DB::SettingsChanges empty_value;
    empty_value.emplace_back("onelake_bearer_token", "");
    expectThrowsCode([&] { catalog.applySettingsChanges(empty_value); }, DB::ErrorCodes::BAD_ARGUMENTS);
}

#endif
