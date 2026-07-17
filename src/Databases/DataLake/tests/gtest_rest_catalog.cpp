#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
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

#include <atomic>
#include <iterator>
#include <memory>
#include <string>

using namespace DataLake;

namespace ProfileEvents
{
    extern const Event OneLakeAccessTokenRequests;
    extern const Event OneLakeAccessTokenRequestFailures;
    extern const Event OneLakeAccessTokenExpirations;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int DATALAKE_DATABASE_ERROR;
}
}

namespace
{

constexpr int DEFAULT_TOKEN_EXPIRES_IN_SECONDS = 3600;

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

void writeJSON(Poco::Net::HTTPServerResponse & response, const std::string & body, Poco::Net::HTTPResponse::HTTPStatus status = Poco::Net::HTTPResponse::HTTP_OK)
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
    RestCatalogRequestHandler(CatalogShape shape_, int token_expires_in_seconds_, std::atomic<size_t> & token_requests_)
        : shape(shape_)
        , token_expires_in_seconds(token_expires_in_seconds_)
        , token_requests(token_requests_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        Poco::URI uri(request.getURI());
        const auto path = getRawPath(request.getURI());
        const auto params = uri.getQueryParameters();

        /// Entra ID style token endpoint for the OneLake refresh-token flow.
        if (path == "/token")
        {
            const std::string request_body(std::istreambuf_iterator<char>(request.stream()), {});
            ++token_requests;
            if (request_body.contains("refresh_token=expired-refresh"))
            {
                writeJSON(
                    response,
                    R"({"error":"invalid_grant","error_description":"AADSTS700082: The refresh token has expired due to inactivity."})",
                    Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
                return;
            }
            writeJSON(
                response,
                fmt::format(
                    R"({{"token_type":"Bearer","expires_in":{},"access_token":"mock-access-token-{}","refresh_token":"rotated-refresh-token"}})",
                    token_expires_in_seconds, token_requests.load()));
            return;
        }

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
    int token_expires_in_seconds;
    std::atomic<size_t> & token_requests;
};

class RestCatalogRequestHandlerFactory final : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    RestCatalogRequestHandlerFactory(CatalogShape shape_, int token_expires_in_seconds_, std::atomic<size_t> & token_requests_)
        : shape(shape_)
        , token_expires_in_seconds(token_expires_in_seconds_)
        , token_requests(token_requests_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new RestCatalogRequestHandler(shape, token_expires_in_seconds, token_requests);
    }

private:
    CatalogShape shape;
    int token_expires_in_seconds;
    std::atomic<size_t> & token_requests;
};

class RestCatalogTestServer
{
public:
    explicit RestCatalogTestServer(CatalogShape shape, int token_expires_in_seconds = DEFAULT_TOKEN_EXPIRES_IN_SECONDS)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0)))
        , handler_factory(new RestCatalogRequestHandlerFactory(shape, token_expires_in_seconds, token_requests))
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

    size_t tokenRequests() const
    {
        return token_requests.load();
    }

private:
    std::atomic<size_t> token_requests{0};
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
        /* refresh_token */"",
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

TEST(RestCatalog, OneLakeRefreshTokenTransparentRenewal)
{
    /// expires_in = 0: every issued access token is immediately expired, so every
    /// catalog request must transparently redeem the refresh token again.
    RestCatalogTestServer server(CatalogShape::Empty, /* token_expires_in_seconds */ 0);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    OneLakeCatalog catalog(
        "warehouse",
        server.getUrl(),
        /* onelake_tenant_id */"tenant-1",
        /* onelake_client_id */"client-1",
        /* onelake_client_secret */"",
        /* bearer_token */"",
        /* refresh_token */"good-refresh",
        /* auth_scope */"https://storage.azure.com/.default",
        /* oauth_server_uri */server.getUrl() + "/token",
        /* oauth_server_use_request_body */true,
        context);

    const auto requests_after_construction = server.tokenRequests();
    EXPECT_GE(requests_after_construction, 1u);

    const auto token_requests_before = ProfileEvents::global_counters[ProfileEvents::OneLakeAccessTokenRequests];
    const auto expirations_before = ProfileEvents::global_counters[ProfileEvents::OneLakeAccessTokenExpirations];

    EXPECT_TRUE(catalog.empty());
    EXPECT_GT(server.tokenRequests(), requests_after_construction);

    EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::OneLakeAccessTokenRequests], token_requests_before);
    EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::OneLakeAccessTokenExpirations], expirations_before);

    const auto requests_before_storage_token = server.tokenRequests();
    const auto [storage_token, expires_on] = catalog.getCurrentAccessToken();
    EXPECT_TRUE(storage_token.starts_with("mock-access-token-"));
    EXPECT_GT(server.tokenRequests(), requests_before_storage_token);
}

TEST(RestCatalog, OneLakeRefreshTokenExpiredThrowsWithAlterHint)
{
    RestCatalogTestServer server(CatalogShape::Empty);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    const auto failures_before = ProfileEvents::global_counters[ProfileEvents::OneLakeAccessTokenRequestFailures];

    try
    {
        OneLakeCatalog catalog(
            "warehouse",
            server.getUrl(),
            /* onelake_tenant_id */"tenant-1",
            /* onelake_client_id */"client-1",
            /* onelake_client_secret */"",
            /* bearer_token */"",
            /* refresh_token */"expired-refresh",
            /* auth_scope */"https://storage.azure.com/.default",
            /* oauth_server_uri */server.getUrl() + "/token",
            /* oauth_server_use_request_body */true,
            context);
        /// ADD_FAILURE (rather than FAIL) does not return from the test, so the
        /// profile event check below is reached on every path; FAIL would make
        /// `failures_before` a dead store on the no-throw path for clang-tidy.
        ADD_FAILURE() << "expected an exception for an expired refresh token";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::DATALAKE_DATABASE_ERROR);
        EXPECT_NE(e.message().find("ALTER DATABASE"), std::string::npos);
        EXPECT_NE(e.message().find("onelake_refresh_token"), std::string::npos);
        EXPECT_NE(e.message().find("AADSTS700082"), std::string::npos);
    }

    EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::OneLakeAccessTokenRequestFailures], failures_before);
}

TEST(RestCatalog, OneLakeApplySettingsChangesRefreshMode)
{
    RestCatalogTestServer server(CatalogShape::Empty);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    OneLakeCatalog catalog(
        "warehouse",
        server.getUrl(),
        /* onelake_tenant_id */"tenant-1",
        /* onelake_client_id */"client-1",
        /* onelake_client_secret */"",
        /* bearer_token */"",
        /* refresh_token */"good-refresh",
        /* auth_scope */"https://storage.azure.com/.default",
        /* oauth_server_uri */server.getUrl() + "/token",
        /* oauth_server_use_request_body */true,
        context);

    DB::SettingsChanges changes;
    changes.emplace_back("onelake_refresh_token", "another-good-refresh");
    catalog.applySettingsChanges(changes);
    EXPECT_EQ(catalog.getStateSnapshot()->refresh_token, "another-good-refresh");

    /// The mode is fixed: a bearer token cannot be set on a refresh-token catalog.
    DB::SettingsChanges mode_switch;
    mode_switch.emplace_back("onelake_bearer_token", "token");
    expectThrowsCode([&] { catalog.applySettingsChanges(mode_switch); }, DB::ErrorCodes::BAD_ARGUMENTS);

    /// An expired refresh token fails the ALTER during prepare, nothing is published.
    DB::SettingsChanges expired;
    expired.emplace_back("onelake_refresh_token", "expired-refresh");
    expectThrowsCode([&] { catalog.applySettingsChanges(expired); }, DB::ErrorCodes::DATALAKE_DATABASE_ERROR);
    EXPECT_EQ(catalog.getStateSnapshot()->refresh_token, "another-good-refresh");
}

#endif
