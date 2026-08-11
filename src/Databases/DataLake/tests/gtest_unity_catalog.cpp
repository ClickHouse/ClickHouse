#include "config.h"

#if USE_PARQUET

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Core/UUID.h>
#include <Common/tests/gtest_global_context.h>
#include <Databases/DataLake/StorageCredentials.h>
#include <Databases/DataLake/UnityCatalog.h>
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

#include <atomic>
#include <memory>

#include <fmt/format.h>

using namespace DataLake;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

enum class CredentialType
{
    GCS,
    S3,
};

void writeJSON(Poco::Net::HTTPServerResponse & response, const std::string & body)
{
    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/json");
    response.setContentLength(body.size());
    response.send() << body;
}

class UnityCatalogRequestHandler final : public Poco::Net::HTTPRequestHandler
{
public:
    UnityCatalogRequestHandler(CredentialType credential_type_, std::atomic<size_t> & credential_requests_)
        : credential_type(credential_type_)
        , credential_requests(credential_requests_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        if (request.getURI() == "/api/2.1/unity-catalog/tables/warehouse.namespace.table")
        {
            const char * const location = credential_type == CredentialType::GCS ? "gs://bucket/table" : "s3://bucket/table";
            writeJSON(
                response,
                fmt::format(
                    R"({{"name":"table","table_id":"11111111-2222-3333-4444-555555555555","storage_location":"{}","data_source_format":"DELTA"}})",
                    location));
            return;
        }

        if (request.getURI() == "/api/2.1/unity-catalog/temporary-table-credentials")
        {
            const size_t request_number = ++credential_requests;
            if (credential_type == CredentialType::GCS)
            {
                writeJSON(response, fmt::format(R"({{"gcp_oauth_token":{{"oauth_token":"gcp-token-{}"}}}})", request_number));
            }
            else
            {
                writeJSON(
                    response,
                    fmt::format(
                        R"({{"aws_temp_credentials":{{"access_key_id":"access-{}","secret_access_key":"secret","session_token":"session"}}}})",
                        request_number));
            }
            return;
        }

        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected request to fake Unity catalog: {}", request.getURI());
    }

private:
    CredentialType credential_type;
    std::atomic<size_t> & credential_requests;
};

class UnityCatalogRequestHandlerFactory final : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    UnityCatalogRequestHandlerFactory(CredentialType credential_type_, std::atomic<size_t> & credential_requests_)
        : credential_type(credential_type_)
        , credential_requests(credential_requests_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new UnityCatalogRequestHandler(credential_type, credential_requests);
    }

private:
    CredentialType credential_type;
    std::atomic<size_t> & credential_requests;
};

class UnityCatalogTestServer
{
public:
    explicit UnityCatalogTestServer(CredentialType credential_type)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0)))
        , handler_factory(new UnityCatalogRequestHandlerFactory(credential_type, credential_requests))
        , server_params(new Poco::Net::HTTPServerParams())
    {
        server_params->setKeepAlive(false);
        server = std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params);
        server->start();
    }

    ~UnityCatalogTestServer()
    {
        server->stop();
    }

    std::string getUrl() const
    {
        return "http://" + server_socket->address().toString() + "/api/2.1/unity-catalog";
    }

private:
    std::atomic<size_t> credential_requests{0};
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<UnityCatalogRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

class UnityCatalogTestHarness
{
public:
    explicit UnityCatalogTestHarness(CredentialType credential_type)
        : server(credential_type)
        , context(DB::Context::createCopy(getContext().context))
    {
        context->makeQueryContext();
        catalog = std::make_unique<UnityCatalog>("warehouse", server.getUrl(), "catalog-token", context);
    }

    std::shared_ptr<IStorageCredentials> getInitialCredentials()
    {
        auto metadata = TableMetadata().withLocation().withStorageCredentials();
        EXPECT_TRUE(catalog->tryGetTableMetadata("namespace", "table", metadata));
        return metadata.getStorageCredentials();
    }

    ICatalog::CredentialsRefreshCallback getRefreshCallback()
    {
        return static_cast<ICatalog *>(catalog.get())->getCredentialsConfigurationCallback(
            DB::StorageID("database", "table", DB::UUIDHelpers::generateV4()));
    }

private:
    UnityCatalogTestServer server;
    DB::ContextMutablePtr context;
    std::unique_ptr<UnityCatalog> catalog;
};

}

TEST(UnityCatalog, ParsesAndRefreshesGCSCredentials)
{
    UnityCatalogTestHarness harness(CredentialType::GCS);
    auto initial_gcs_credentials = std::dynamic_pointer_cast<GCSCredentials>(harness.getInitialCredentials());
    ASSERT_NE(initial_gcs_credentials, nullptr);
    EXPECT_EQ(initial_gcs_credentials->getToken(), "gcp-token-1");

    auto refresh_callback = harness.getRefreshCallback();
    ASSERT_TRUE(refresh_callback.has_value());
    auto refreshed_gcs_credentials = std::dynamic_pointer_cast<GCSCredentials>((*refresh_callback)());
    ASSERT_NE(refreshed_gcs_credentials, nullptr);
    EXPECT_EQ(refreshed_gcs_credentials->getToken(), "gcp-token-2");
}

TEST(UnityCatalog, PreservesS3CredentialParsing)
{
    UnityCatalogTestHarness harness(CredentialType::S3);
    auto initial_s3_credentials = std::dynamic_pointer_cast<S3Credentials>(harness.getInitialCredentials());
    ASSERT_NE(initial_s3_credentials, nullptr);
    EXPECT_EQ(initial_s3_credentials->getAccessKeyId(), "access-1");

    auto refresh_callback = harness.getRefreshCallback();
    ASSERT_TRUE(refresh_callback.has_value());
    auto refreshed_s3_credentials = std::dynamic_pointer_cast<S3Credentials>((*refresh_callback)());
    ASSERT_NE(refreshed_s3_credentials, nullptr);
    EXPECT_EQ(refreshed_s3_credentials->getAccessKeyId(), "access-2");
}

#endif
