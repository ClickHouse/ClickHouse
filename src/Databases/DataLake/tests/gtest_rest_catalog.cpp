#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/tests/gtest_global_context.h>
#include <Databases/DataLake/HTTPBasedCatalogUtils.h>
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

#include <algorithm>
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
    /// The shapes below advertise two namespaces, `alive` (holding `table_a`) and `doomed`, and then
    /// fail one of the two calls that read `doomed` — modelling a namespace another client dropped
    /// after it was listed. The status varies so that only the not-found case may be tolerated.
    VanishedChildListing,
    VanishedChildListingUnauthorized,
    VanishedChildListingServerError,
    VanishedTableListing,
    VanishedTableListingUnauthorized,
    /// The two shapes below serve a non-empty FIRST page and then 404 the follow-up page, modelling
    /// a namespace dropped part-way through a paginated listing. A 404 names the namespace, so every
    /// page collected so far must be discarded: returning the pages already read would report a
    /// subset of a dropped namespace as if it were the whole of a live one.
    VanishedChildListingSecondPage,
    VanishedTableListingSecondPage,
    /// The root `/v1/namespaces` route itself 404s: a misconfigured endpoint, never a race.
    MissingRootRoute,
};

bool isVanishingShape(CatalogShape shape)
{
    return shape == CatalogShape::VanishedChildListing || shape == CatalogShape::VanishedChildListingUnauthorized
        || shape == CatalogShape::VanishedChildListingServerError || shape == CatalogShape::VanishedTableListing
        || shape == CatalogShape::VanishedTableListingUnauthorized
        || shape == CatalogShape::VanishedChildListingSecondPage || shape == CatalogShape::VanishedTableListingSecondPage;
}

/// Requests the fake catalog served, so a test can assert the tolerant branch was really reached
/// (a fixture that never 404s would otherwise pass vacuously).
struct RequestCounters
{
    std::atomic<size_t> doomed_child_listing{0};
    std::atomic<size_t> doomed_table_listing{0};
    std::atomic<size_t> root_listing{0};
};

void writeJSON(Poco::Net::HTTPServerResponse & response, const std::string & body, Poco::Net::HTTPResponse::HTTPStatus status = Poco::Net::HTTPResponse::HTTP_OK)
{
    response.setStatus(status);
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

constexpr auto NO_SUCH_NAMESPACE_BODY
    = R"({"error":{"message":"Namespace doomed not found.","type":"NoSuchNamespaceException","code":404}})";
constexpr auto NOT_AUTHORIZED_BODY = R"({"error":{"message":"The access token has expired","type":"NotAuthorizedException","code":401}})";
constexpr auto SERVER_ERROR_BODY = R"({"error":{"message":"Internal error","type":"ServerError","code":500}})";

class RestCatalogRequestHandler final : public Poco::Net::HTTPRequestHandler
{
public:
    RestCatalogRequestHandler(
        CatalogShape shape_, int token_expires_in_seconds_, std::atomic<size_t> & token_requests_, RequestCounters * counters_)
        : shape(shape_)
        , token_expires_in_seconds(token_expires_in_seconds_)
        , token_requests(token_requests_)
        , counters(counters_)
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
            const std::string request_body(std::istreambuf_iterator<char>(request.stream()), {});
            ++token_requests;
            /// Horizon secret-only credentials omit client_id; standard REST includes it.
            if (request_body.contains("client_secret=") && !request_body.contains("client_id="))
            {
                writeJSON(response, R"({"token_type":"Bearer","expires_in":3600,"access_token":"mock-horizon-secret-only-token"})");
                return;
            }
            writeJSON(response, R"({"token_type":"Bearer","expires_in":3600,"access_token":"mock-access-token"})");
            return;
        }

        if (path == "/v1/namespaces")
        {
            const auto parent = getParent(params);
            if (parent.empty())
            {
                if (counters)
                    ++counters->root_listing;
                if (shape == CatalogShape::MissingRootRoute)
                {
                    /// A wrong catalog prefix 404s the root listing, which carries no `parent`.
                    writeError(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, R"({"error_code":"ENDPOINT_NOT_FOUND"})");
                    return;
                }
                if (shape == CatalogShape::NestedTableThenEmptySibling)
                    writeJSON(response, R"({"namespaces":[["parent"],["empty_later"]]})");
                else if (shape == CatalogShape::ParentIgnoringEcho)
                    writeJSON(response, R"({"namespaces":[["gold"]]})");
                else if (isVanishingShape(shape))
                    writeJSON(response, R"({"namespaces":[["alive"],["doomed"]]})");
                else
                    writeJSON(response, R"({"namespaces":[["namespace"]]})");
                return;
            }

            if (parent == "doomed" && isVanishingShape(shape))
            {
                /// Both pages count, so an arm can require >= 2 and thereby prove the follow-up
                /// request really was issued and really did 404.
                if (counters)
                    ++counters->doomed_child_listing;
                if (shape == CatalogShape::VanishedChildListing)
                    writeError(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, NO_SUCH_NAMESPACE_BODY);
                else if (shape == CatalogShape::VanishedChildListingUnauthorized)
                    writeError(response, Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED, NOT_AUTHORIZED_BODY);
                else if (shape == CatalogShape::VanishedChildListingServerError)
                    writeError(response, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, SERVER_ERROR_BODY);
                else if (shape == CatalogShape::VanishedChildListingSecondPage)
                {
                    if (getPageToken(params).empty())
                        writeJSON(response, R"({"namespaces":[["doomed","ghost"]],"next-page-token":"p2"})");
                    else
                        writeError(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, NO_SUCH_NAMESPACE_BODY);
                }
                else
                    writeJSON(response, R"({"namespaces":[]})");
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

        if (path == "/v1/namespaces/alive/tables")
        {
            writeJSON(response, R"({"identifiers":[{"name":"table_a"}]})");
            return;
        }

        if (path == "/v1/namespaces/doomed/tables")
        {
            /// `getRawPath` drops the query, so the follow-up page arrives on this same route and is
            /// counted too: an arm requiring >= 2 proves the second request happened.
            if (counters)
                ++counters->doomed_table_listing;
            if (shape == CatalogShape::VanishedTableListing)
                writeError(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, NO_SUCH_NAMESPACE_BODY);
            else if (shape == CatalogShape::VanishedTableListingUnauthorized)
                writeError(response, Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED, NOT_AUTHORIZED_BODY);
            else if (shape == CatalogShape::VanishedTableListingSecondPage)
            {
                if (getPageToken(params).empty())
                    writeJSON(response, R"({"identifiers":[{"name":"ghost_table"}],"next-page-token":"p2"})");
                else
                    writeError(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, NO_SUCH_NAMESPACE_BODY);
            }
            else
                writeJSON(response, R"({"identifiers":[]})");
            return;
        }

        /// A surviving first-page child of `doomed` is only ever requested when the child listing
        /// wrongly keeps it, so serving a table here gives that failure a name to assert on. An
        /// empty list would instead let the wrong behaviour produce the expected table set.
        if (path == "/v1/namespaces/doomed%1Fghost/tables")
        {
            writeJSON(response, R"({"identifiers":[{"name":"ghost_table"}]})");
            return;
        }

        if (path == "/v1/namespaces/doomed/tables/t")
        {
            /// A user who NAMES a table in the vanished namespace must still be told it is absent.
            writeError(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, NO_SUCH_NAMESPACE_BODY);
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

        if (path == "/v1/namespaces/namespace/tables/table_a")
        {
            writeJSON(
                response,
                R"({"metadata-location":"s3://bucket/table_a/metadata/v1.metadata.json",)"
                R"("metadata":{"table-uuid":"11111111-2222-3333-4444-555555555555","location":"s3://bucket/table_a"}})");
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

        /// Fabric-style response once the bearer token has expired.
        if (path == "/v1/namespaces/namespace/tables/expired_token_table")
        {
            writeJSON(
                response,
                R"({"error":{"code":"Unauthorized","message":"Lifetime validation failed, the token is expired."}})",
                Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
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

    /// Empty when absent, i.e. when the request is for the first page.
    static std::string getPageToken(const Poco::URI::QueryParameters & params)
    {
        for (const auto & [key, value] : params)
        {
            if (key == "pageToken")
                return value;
        }
        return {};
    }

    CatalogShape shape;
    int token_expires_in_seconds;
    std::atomic<size_t> & token_requests;
    RequestCounters * counters;
};

class RestCatalogRequestHandlerFactory final : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    RestCatalogRequestHandlerFactory(
        CatalogShape shape_, int token_expires_in_seconds_, std::atomic<size_t> & token_requests_, RequestCounters * counters_)
        : shape(shape_)
        , token_expires_in_seconds(token_expires_in_seconds_)
        , token_requests(token_requests_)
        , counters(counters_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new RestCatalogRequestHandler(shape, token_expires_in_seconds, token_requests, counters);
    }

private:
    CatalogShape shape;
    int token_expires_in_seconds;
    std::atomic<size_t> & token_requests;
    RequestCounters * counters;
};

class RestCatalogTestServer
{
public:
    explicit RestCatalogTestServer(
        CatalogShape shape, int token_expires_in_seconds = DEFAULT_TOKEN_EXPIRES_IN_SECONDS, RequestCounters * counters = nullptr)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0)))
        , handler_factory(new RestCatalogRequestHandlerFactory(shape, token_expires_in_seconds, token_requests, counters))
        , server_params(new Poco::Net::HTTPServerParams())
    {
        server_params->setKeepAlive(false);
        server = std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params);
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

std::unique_ptr<RestCatalog> makeRestCatalog(const RestCatalogTestServer & server, const DB::ContextMutablePtr & context)
{
    /// Collapse the retry backoff: the arms that exercise a retriable status would otherwise sleep
    /// through the default ladder, which is about 33 seconds. The attempt count is left alone so
    /// that the retry path itself is still exercised.
    context->setSetting("http_retry_initial_backoff_ms", 1u);
    context->setSetting("http_retry_max_backoff_ms", 2u);

    return std::make_unique<RestCatalog>(
        "warehouse",
        server.getUrl(),
        /* catalog_credential */"",
        /* auth_scope */"",
        /* auth_header */"",
        /* oauth_server_uri */"",
        /* oauth_server_use_request_body */false,
        context);
}

DB::Names sortedTableNames(const CatalogTables & tables)
{
    DB::Names names;
    names.reserve(tables.size());
    for (const auto & table : tables)
        names.push_back(table.name);
    std::sort(names.begin(), names.end());
    return names;
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

    auto existing = TableMetadata().withLocation();
    EXPECT_TRUE(catalog.tryGetTableMetadata("namespace", "table_a", existing));
    EXPECT_EQ(existing.getLocation(), "s3://bucket/table_a");
    EXPECT_TRUE(catalog.existsTable("namespace", "table_a"));

    TableMetadata missing;
    EXPECT_FALSE(catalog.tryGetTableMetadata("namespace", "missing_table", missing));
    EXPECT_FALSE(catalog.existsTable("namespace", "missing_table"));

    TableMetadata unauthorized;
    EXPECT_THROW(catalog.tryGetTableMetadata("namespace", "unauthorized_table", unauthorized), DB::HTTPException);
    EXPECT_THROW(catalog.existsTable("namespace", "unauthorized_table"), DB::HTTPException);
}

TEST(RestCatalog, TryGetTableMetadataAuthErrorPropagates)
{
    /// An expired or revoked token must not read as "table does not exist" (which surfaces
    /// as `UNKNOWN_TABLE` on SELECT and `EXISTS TABLE` returning 0): the HTTP 401 from the
    /// catalog propagates to the user instead.
    RestCatalogTestServer server(CatalogShape::TopLevelTable);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    OneLakeCatalog catalog(
        "warehouse",
        server.getUrl(),
        /* onelake_tenant_id */"tenant-1",
        /* onelake_client_id */"",
        /* onelake_client_secret */"",
        /* bearer_token */"expired-token",
        /* refresh_token */"",
        /* auth_scope */"",
        /* oauth_server_uri */"",
        /* oauth_server_use_request_body */false,
        context);

    TableMetadata metadata;
    try
    {
        catalog.tryGetTableMetadata("namespace", "expired_token_table", metadata);
        FAIL() << "expected the HTTP 401 from the catalog to propagate";
    }
    catch (const DB::HTTPException & e)
    {
        EXPECT_EQ(e.getHTTPStatus(), Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
        EXPECT_NE(e.displayText().find("the token is expired"), std::string::npos);
    }

    EXPECT_THROW(catalog.existsTable("namespace", "expired_token_table"), DB::HTTPException);
}

/// The tests below cover a namespace that another client drops after it was listed but before its
/// own listing is read. Such a namespace has no tables, so a listing must skip it and return the
/// rest of the catalog; every other error, and a user naming the namespace, must still be reported.

TEST(RestCatalog, GetTablesSkipsNamespaceVanishedDuringChildListing)
{
    RequestCounters counters;
    RestCatalogTestServer server(
        CatalogShape::VanishedChildListing, /* token_expires_in_seconds */ DEFAULT_TOKEN_EXPIRES_IN_SECONDS, &counters);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    EXPECT_EQ(sortedTableNames(catalog->getTables()), DB::Names{"alive.table_a"});
    /// Anti-vacuity: the tolerant branch was really reached.
    EXPECT_GE(counters.doomed_child_listing.load(), 1u);
}

TEST(RestCatalog, GetTablesSkipsNamespaceVanishedDuringTableListing)
{
    RequestCounters counters;
    RestCatalogTestServer server(
        CatalogShape::VanishedTableListing, /* token_expires_in_seconds */ DEFAULT_TOKEN_EXPIRES_IN_SECONDS, &counters);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    EXPECT_EQ(sortedTableNames(catalog->getTables()), DB::Names{"alive.table_a"});
    EXPECT_FALSE(catalog->empty());
    EXPECT_GE(counters.doomed_table_listing.load(), 1u);
}

TEST(RestCatalog, ChildListingDiscardsEarlierPagesWhenNamespaceVanishes)
{
    /// The namespace is dropped after its first page of children was already read. That page named
    /// `doomed.ghost`, which owns a table, so keeping the page would report `doomed.ghost.ghost_table`
    /// as a live table of a namespace that no longer exists.
    RequestCounters counters;
    RestCatalogTestServer server(
        CatalogShape::VanishedChildListingSecondPage, /* token_expires_in_seconds */ DEFAULT_TOKEN_EXPIRES_IN_SECONDS, &counters);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    EXPECT_EQ(sortedTableNames(catalog->getTables()), DB::Names{"alive.table_a"});
    /// Anti-vacuity: the follow-up page really was requested, and it is the request that 404'd.
    /// `>=` rather than an exact count, because a retriable status may reissue a request.
    EXPECT_GE(counters.doomed_child_listing.load(), 2u);
}

TEST(RestCatalog, TableListingDiscardsEarlierPagesWhenNamespaceVanishes)
{
    /// Same race one level down: the first page of `doomed`'s tables was read before the namespace
    /// was dropped, so keeping it would report `doomed.ghost_table`.
    RequestCounters counters;
    RestCatalogTestServer server(
        CatalogShape::VanishedTableListingSecondPage, /* token_expires_in_seconds */ DEFAULT_TOKEN_EXPIRES_IN_SECONDS, &counters);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    EXPECT_EQ(sortedTableNames(catalog->getTables()), DB::Names{"alive.table_a"});
    EXPECT_GE(counters.doomed_table_listing.load(), 2u);
}

TEST(RestCatalog, FilteredListingSkipsVanishedNamespace)
{
    RequestCounters counters;
    RestCatalogTestServer server(
        CatalogShape::VanishedTableListing, /* token_expires_in_seconds */ DEFAULT_TOKEN_EXPIRES_IN_SECONDS, &counters);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    /// `name = 'doomed.t'` and `name LIKE 'do%'` both push the listing down to `doomed` only. The
    /// filtered overload lives on the base class, which is also how `DatabaseDataLake` reaches it.
    /// The count is snapshotted around each call separately: a single assertion after both would
    /// still hold if only one of the two pushdowns ever reached the listing, and each has to be
    /// shown to reach it. `>` rather than an exact count, because a retry may reissue the request.
    const ICatalog & base = *catalog;

    const auto before_equals = counters.doomed_table_listing.load();
    EXPECT_TRUE(base.getTables(TableNameFilter{TableNameFilter::Kind::Equals, "doomed.t"}).empty());
    const auto after_equals = counters.doomed_table_listing.load();
    EXPECT_GT(after_equals, before_equals);

    EXPECT_TRUE(base.getTables(TableNameFilter{TableNameFilter::Kind::Like, "do%"}).empty());
    EXPECT_GT(counters.doomed_table_listing.load(), after_equals);
}

TEST(RestCatalog, ChildListingUnauthorizedStillThrows)
{
    RestCatalogTestServer server(CatalogShape::VanishedChildListingUnauthorized);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    expectThrowsCode([&] { catalog->getTables(); }, DB::ErrorCodes::DATALAKE_DATABASE_ERROR);
}

TEST(RestCatalog, ChildListingServerErrorStillThrows)
{
    RestCatalogTestServer server(CatalogShape::VanishedChildListingServerError);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    expectThrowsCode([&] { catalog->getTables(); }, DB::ErrorCodes::DATALAKE_DATABASE_ERROR);
}

TEST(RestCatalog, MissingRootListingRouteStillThrows)
{
    /// A 404 on the root listing means the catalog endpoint is misconfigured, not that a namespace
    /// vanished — it carries no `parent`, so tolerance must not apply to it.
    RequestCounters counters;
    RestCatalogTestServer server(
        CatalogShape::MissingRootRoute, /* token_expires_in_seconds */ DEFAULT_TOKEN_EXPIRES_IN_SECONDS, &counters);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    expectThrowsCode([&] { catalog->getTables(); }, DB::ErrorCodes::DATALAKE_DATABASE_ERROR);
    EXPECT_GE(counters.root_listing.load(), 1u);
    /// The root 404 short-circuits before any descent request is issued.
    EXPECT_EQ(counters.doomed_child_listing.load(), 0u);
}

TEST(RestCatalog, TableListingUnauthorizedStillThrows)
{
    RestCatalogTestServer server(CatalogShape::VanishedTableListingUnauthorized);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    EXPECT_THROW(catalog->getTables(), DB::HTTPException);
}

TEST(RestCatalog, NamedTableInVanishedNamespaceStillReportedAbsent)
{
    /// Listing tolerance must not leak into the path a user reaches by naming a table: it has to
    /// report absence so the engine still answers UNKNOWN_TABLE.
    RestCatalogTestServer server(CatalogShape::VanishedTableListing);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto catalog = makeRestCatalog(server, context);

    TableMetadata metadata;
    EXPECT_FALSE(catalog->tryGetTableMetadata("doomed", "t", metadata));
    EXPECT_FALSE(catalog->existsTable("doomed", "t"));
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

TEST(RestCatalog, OneLakeRejectsMalformedBearerToken)
{
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    /// A pre-obtained bearer token becomes the `Authorization: Bearer <token>` header, so it must
    /// pass the same validation as a user-supplied `auth_header`: a token with an embedded newline
    /// would smuggle a second header into the request. The constructor validates the synthetic
    /// header up front and must reject such a token before any request is issued.
    expectThrowsCode(
        [&]
        {
            OneLakeCatalog catalog(
                "warehouse",
                "http://127.0.0.1:1",
                /* onelake_tenant_id */ "tenant",
                /* onelake_client_id */ "",
                /* onelake_client_secret */ "",
                /* bearer_token */ "token\r\nX-Injected: evil",
                /* refresh_token */ "",
                /* auth_scope */ "",
                /* oauth_server_uri */ "",
                /* oauth_server_use_request_body */ false,
                context);
        },
        DB::ErrorCodes::BAD_ARGUMENTS);
}

TEST(RestCatalog, ValidateBearerTokenRejectsMalformedHeader)
{
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    /// Every bearer-token catalog path (Unity, and Paimon via HTTPBasedCatalogUtils) runs this
    /// shared check before the token becomes an `Authorization: Bearer <token>` header, matching
    /// the explicit validation OneLake performs. A token with an embedded CR/LF would otherwise
    /// smuggle a second header into the request.
    expectThrowsCode([&] { validateBearerToken(context, "token\r\nX-Injected: evil"); }, DB::ErrorCodes::BAD_ARGUMENTS);

    /// A well-formed token is accepted; an empty token sends no header and is a no-op.
    EXPECT_NO_THROW(validateBearerToken(context, "good-token"));
    EXPECT_NO_THROW(validateBearerToken(context, ""));
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

TEST(RestCatalog, HorizonParseCredentialKeepsColonsInSecret)
{
    {
        const auto [client_id, client_secret] = HorizonCatalog::parseHorizonCredential("my-pat-token");
        EXPECT_TRUE(client_id.empty());
        EXPECT_EQ(client_secret, "my-pat-token");
    }
    {
        /// Snowflake PATs may contain `:`; Horizon must not split them into client_id/client_secret.
        const auto [client_id, client_secret] = HorizonCatalog::parseHorizonCredential("ver:1-hint:abc:rest-of-token");
        EXPECT_TRUE(client_id.empty());
        EXPECT_EQ(client_secret, "ver:1-hint:abc:rest-of-token");
    }
    {
        const auto [client_id, client_secret] = HorizonCatalog::parseHorizonCredential("");
        EXPECT_TRUE(client_id.empty());
        EXPECT_TRUE(client_secret.empty());
    }
}

TEST(RestCatalog, HorizonCatalogAuthenticatesWithBarePAT)
{
    RestCatalogTestServer server(CatalogShape::TopLevelTable);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    HorizonCatalog catalog(
        "ICEBERG_TEST_DB",
        server.getUrl(),
        /* catalog_credential */"horizon-pat-without-colon",
        /* auth_scope */"session:role:DATA_ENGINEER",
        /* auth_header */"",
        /* oauth_server_uri */"",
        /* oauth_server_use_request_body */true,
        context);

    EXPECT_EQ(catalog.getCatalogType(), DB::DatabaseDataLakeCatalogType::ICEBERG_HORIZON);
    EXPECT_TRUE(catalog.getStateSnapshot()->client_id.empty());
    EXPECT_EQ(catalog.getStateSnapshot()->client_secret, "horizon-pat-without-colon");
    EXPECT_FALSE(catalog.empty());

    TableMetadata metadata;
    metadata.withLocation();
    catalog.getTableMetadata("namespace", "table_a", metadata);
    EXPECT_TRUE(metadata.hasLocation());
    EXPECT_EQ(metadata.getLocation(), "s3://bucket/table_a");
}

TEST(RestCatalog, HorizonCatalogRequiresCredentialOrAuthHeader)
{
    RestCatalogTestServer server(CatalogShape::Empty);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    expectThrowsCode(
        [&]
        {
            HorizonCatalog catalog(
                "ICEBERG_TEST_DB",
                server.getUrl(),
                /* catalog_credential */"",
                /* auth_scope */"session:role:DATA_ENGINEER",
                /* auth_header */"",
                /* oauth_server_uri */"",
                /* oauth_server_use_request_body */true,
                context);
        },
        DB::ErrorCodes::BAD_ARGUMENTS);
}

TEST(RestCatalog, HorizonApplySettingsChangesBarePAT)
{
    RestCatalogTestServer server(CatalogShape::Empty);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    HorizonCatalog catalog(
        "ICEBERG_TEST_DB",
        server.getUrl(),
        /* catalog_credential */"pat-one",
        /* auth_scope */"session:role:DATA_ENGINEER",
        /* auth_header */"",
        /* oauth_server_uri */"",
        /* oauth_server_use_request_body */true,
        context);

    DB::SettingsChanges changes;
    changes.emplace_back("catalog_credential", "pat-two");
    catalog.applySettingsChanges(changes);
    EXPECT_TRUE(catalog.getStateSnapshot()->client_id.empty());
    EXPECT_EQ(catalog.getStateSnapshot()->client_secret, "pat-two");

    /// Mode is fixed: cannot switch to auth_header on a credential catalog.
    DB::SettingsChanges mode_switch;
    mode_switch.emplace_back("auth_header", "Authorization: Bearer token");
    expectThrowsCode([&] { catalog.applySettingsChanges(mode_switch); }, DB::ErrorCodes::BAD_ARGUMENTS);
}

#endif
