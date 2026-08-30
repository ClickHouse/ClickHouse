#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestCatalogConfig.h>
#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestCredentialsConfig.h>
#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestError.h>
#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestNamespace.h>
#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestOAuth.h>
#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestTable.h>

#include <gtest/gtest.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

using namespace DataLake::IcebergRestModels;

namespace
{

TEST(IcebergRestModels, CatalogConfigRoundtrip)
{
    CatalogConfigResponse response;
    response.defaults.prefix = "prod";
    response.defaults.default_base_location = "s3://bucket/warehouse/";
    response.overrides.prefix = "prod2";

    const auto json = serializeCatalogConfigResponse(response);
    const auto parsed = parseCatalogConfigResponse(json);

    EXPECT_EQ(parsed.defaults.prefix, "prod");
    EXPECT_EQ(parsed.defaults.default_base_location, "s3://bucket/warehouse/");
    EXPECT_EQ(parsed.overrides.prefix, "prod2");

    const auto merged = parsed.merged();
    EXPECT_EQ(merged.prefix, "prod2");
    EXPECT_EQ(merged.default_base_location, "s3://bucket/warehouse/");
}

TEST(IcebergRestModels, EncodeNamespaceForURI)
{
    EXPECT_EQ(encodeNamespaceForURI("ns"), "ns");
    EXPECT_EQ(encodeNamespaceForURI("a.b"), "a%1Fb");
}

TEST(IcebergRestModels, ParentNamespaceQueryParams)
{
    const auto params = createParentNamespaceQueryParams("a.b");
    ASSERT_EQ(params.size(), 1u);
    EXPECT_EQ(params[0].first, "parent");
    EXPECT_EQ(params[0].second, std::string("a") + static_cast<char>(0x1F) + "b");
}

TEST(IcebergRestModels, ParseNamespaceListPage)
{
    const std::string json = R"({"namespaces":[["ns1"],["ns2"]],"next-page-token":"token-1"})";
    NamespaceListParseOptions options;
    const auto page = parseNamespaceListPage(json, "", options);
    ASSERT_EQ(page.namespaces.size(), 2u);
    EXPECT_EQ(page.namespaces[0], "ns1");
    EXPECT_EQ(page.namespaces[1], "ns2");
    EXPECT_EQ(page.next_page_token, "token-1");

    const auto nested = parseNamespaceListPage(R"({"namespaces":[["sales"]]})", "prod", options);
    ASSERT_EQ(nested.namespaces.size(), 1u);
    EXPECT_EQ(nested.namespaces[0], "prod.sales");
}

TEST(IcebergRestModels, ParseNamespaceListPageSkipsSubnamespacesForFlatCatalog)
{
    const std::string json = R"({"namespaces":[["other"]],"next-page-token":"token-1"})";
    NamespaceListParseOptions options;
    options.skip_subnamespaces_when_parent_non_empty = true;
    options.suppress_pagination_when_all_entries_skipped = true;
    const auto page = parseNamespaceListPage(json, "parent", options);
    EXPECT_TRUE(page.namespaces.empty());
    EXPECT_TRUE(page.next_page_token.empty());
}

TEST(IcebergRestModels, NamespaceListPageRoundtrip)
{
    NamespaceListPage page;
    page.namespaces = {"ns1", "ns2"};
    page.next_page_token = "token-1";
    const auto reparsed = parseNamespaceListPage(serializeNamespaceListPage(page), "", NamespaceListParseOptions{});
    EXPECT_EQ(reparsed.namespaces, page.namespaces);
    EXPECT_EQ(reparsed.next_page_token, page.next_page_token);
}

TEST(IcebergRestModels, CreateNamespaceRequestRoundtrip)
{
    const auto json = serializeCreateNamespaceRequest("ns1", "s3://bucket/ns1/");
    Poco::JSON::Parser parser;
    const auto object = parser.parse(json).extract<Poco::JSON::Object::Ptr>();
    const auto namespace_array = object->getArray("namespace");
    ASSERT_EQ(namespace_array->size(), 1u);
    EXPECT_EQ(namespace_array->get(static_cast<int>(0)).extract<std::string>(), "ns1");
    EXPECT_EQ(object->getObject("properties")->getValue<std::string>("location"), "s3://bucket/ns1/");
}

TEST(IcebergRestModels, ParseTableIdentifiersPage)
{
    const std::string json = R"({"identifiers":[{"name":"t1"},{"name":"foo/bar"}],"next-page-token":"token-2"})";
    const auto page = parseTableIdentifiersPage(json, "ns", 0);
    ASSERT_EQ(page.tables.size(), 2u);
    EXPECT_EQ(page.tables[0], "ns.t1");
    EXPECT_EQ(page.tables[1], "ns.foo%2Fbar");
    EXPECT_EQ(page.next_page_token, "token-2");
}

TEST(IcebergRestModels, TableIdentifiersPageRoundtrip)
{
    TableIdentifiersPage page;
    page.tables = {"ns.t1", "ns.t2"};
    page.next_page_token = "token-2";
    const auto reparsed = parseTableIdentifiersPage(serializeTableIdentifiersPage(page, "ns"), "ns", 0);
    EXPECT_EQ(reparsed.tables, page.tables);
    EXPECT_EQ(reparsed.next_page_token, page.next_page_token);
}

TEST(IcebergRestModels, ParseLoadTableResponse)
{
    const std::string json = R"({
        "metadata-location": "s3://bucket/metadata.json",
        "metadata": {"location": "s3://bucket/table/", "table-uuid": "uuid-1"},
        "config": {"s3.access-key-id": "key", "s3.secret-access-key": "secret"}
    })";

    const auto response = parseLoadTableResponse(json);
    ASSERT_TRUE(response.metadata_location.has_value());
    EXPECT_EQ(response.metadata_location.value(), "s3://bucket/metadata.json");
    ASSERT_NE(response.metadata, nullptr);
    EXPECT_EQ(response.metadata->getValue<std::string>("location"), "s3://bucket/table/");
    ASSERT_NE(response.config, nullptr);

    const auto vended = parseVendedStorageConfig(response.config);
    ASSERT_TRUE(vended.s3_access_key_id.has_value());
    EXPECT_EQ(vended.s3_access_key_id.value(), "key");
    ASSERT_TRUE(vended.s3_secret_access_key.has_value());
    EXPECT_EQ(vended.s3_secret_access_key.value(), "secret");
}

TEST(IcebergRestModels, ParseOAuthTokenResponse)
{
    const std::string json = R"({"access_token":"token","expires_in":3600,"token_type":"Bearer"})";
    const auto token = parseOAuthTokenResponse(json, /* require_bearer_type */ true);
    EXPECT_EQ(token.access_token, "token");
    ASSERT_TRUE(token.expires_in.has_value());
    EXPECT_EQ(token.expires_in.value(), 3600);
    EXPECT_EQ(token.token_type, "Bearer");
}

TEST(IcebergRestModels, ErrorResponseRoundtrip)
{
    ErrorResponse error{.message = "Not found", .type = "NoSuchNamespaceException", .code = 404};
    const auto json = serializeErrorResponse(error);
    const auto parsed = tryParseErrorResponse(json);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->message, error.message);
    EXPECT_EQ(parsed->type, error.type);
    EXPECT_EQ(parsed->code, error.code);
}

TEST(IcebergRestModels, ParseErrorResponseFromSpecExample)
{
    const std::string json = R"({"error":{"message":"Table does not exist","type":"NoSuchTableException","code":404}})";
    const auto parsed = tryParseErrorResponse(json);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->message, "Table does not exist");
    EXPECT_EQ(parsed->type, "NoSuchTableException");
    EXPECT_EQ(parsed->code, 404);
}

TEST(IcebergRestModels, ParseVendedStorageConfigAzure)
{
    Poco::JSON::Object::Ptr config = new Poco::JSON::Object;
    config->set("adls.sas-token.account", "sas-value");
    const auto vended = parseVendedStorageConfig(config);
    ASSERT_TRUE(vended.adls_sas_token.has_value());
    EXPECT_EQ(vended.adls_sas_token.value(), "sas-value");
}

}

#endif
