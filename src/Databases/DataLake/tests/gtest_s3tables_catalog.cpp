#include "config.h"

#if USE_AVRO && USE_SSL && USE_AWS_S3

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/tests/gtest_global_context.h>
#include <Databases/DataLake/S3TablesCatalog.h>
#include <Interpreters/Context.h>
#include <base/scope_guard.h>

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
#include <chrono>
#include <memory>
#include <string>
#include <thread>

namespace DB::ErrorCodes
{
extern const int CANNOT_SCHEDULE_TASK;
}

namespace
{

constexpr size_t NUM_NAMESPACES = 16;

/// Catalog of NUM_NAMESPACES namespaces with one table each; listing a namespace takes 1 s, longer than a failed
/// scheduling takes to throw (its message may carry a symbolized stack trace).
class S3TablesCatalogRequestHandler final : public Poco::Net::HTTPRequestHandler
{
public:
    explicit S3TablesCatalogRequestHandler(std::atomic<size_t> & table_requests_)
        : table_requests(table_requests_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        const auto path = Poco::URI(request.getURI()).getPath();
        std::string body;
        if (path.ends_with("/namespaces"))
        {
            body = R"({"namespaces":[)";
            for (size_t i = 0; i < NUM_NAMESPACES; ++i)
                body += (i ? "," : "") + std::string(R"([")") + "ns" + std::to_string(i) + R"("])";
            body += "]}";
        }
        else if (path.ends_with("/tables"))
        {
            ++table_requests;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            const auto ns = path.substr(0, path.size() - std::string("/tables").size());
            body = R"({"identifiers":[{"namespace":[")" + ns.substr(ns.rfind('/') + 1) + R"("],"name":"t"}]})";
        }
        else
            body = R"({"defaults":{},"overrides":{}})";

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentType("application/json");
        response.setContentLength(body.size());
        response.send() << body;
    }

private:
    std::atomic<size_t> & table_requests;
};

class S3TablesCatalogRequestHandlerFactory final : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit S3TablesCatalogRequestHandlerFactory(std::atomic<size_t> & table_requests_)
        : table_requests(table_requests_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new S3TablesCatalogRequestHandler(table_requests);
    }

private:
    std::atomic<size_t> & table_requests;
};

class S3TablesCatalogTestServer
{
public:
    explicit S3TablesCatalogTestServer(std::atomic<size_t> & table_requests)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0)))
        , handler_factory(new S3TablesCatalogRequestHandlerFactory(table_requests))
        , server_params(new Poco::Net::HTTPServerParams())
    {
        server_params->setKeepAlive(false);
        server = std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params);
        server->start();
    }

    ~S3TablesCatalogTestServer()
    {
        server->stop();
    }

    std::string getUrl() const
    {
        return "http://" + server_socket->address().toString();
    }

private:
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<S3TablesCatalogRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

}

TEST(S3TablesCatalog, GetTablesWhenSchedulingThrows)
{
    std::atomic<size_t> table_requests = 0;
    S3TablesCatalogTestServer server(table_requests);
    auto context = DB::Context::createCopy(getContext().context);
    context->makeQueryContext();

    DataLake::CatalogSettings settings;
    settings.aws_access_key_id = "key";
    settings.aws_secret_access_key = "secret";
    DataLake::S3TablesCatalog catalog(
        "wh",
        server.getUrl() + "/v1",
        "us-east-1",
        settings,
        context,
        /* allow_server_credentials_in_user_queries */ false,
        DB::DatabaseDataLakeSnapshotLoadingMode::AUTO);

    /// Each scheduling fails with probability 0.5, so usually a later listing job fails to schedule while earlier ones run.
    bool threw_while_jobs_ran = false;
    for (size_t attempt = 0; attempt < 100 && !threw_while_jobs_ran; ++attempt)
    {
        table_requests = 0;
        CannotAllocateThreadFaultInjector::setFaultProbability(0.5);
        SCOPE_EXIT({ CannotAllocateThreadFaultInjector::setFaultProbability(0.0); });
        try
        {
            EXPECT_EQ(catalog.getTables().size(), NUM_NAMESPACES);
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(e.code(), DB::ErrorCodes::CANNOT_SCHEDULE_TASK);
            threw_while_jobs_ran = table_requests > 0 && table_requests < NUM_NAMESPACES;
        }
    }
    EXPECT_TRUE(threw_while_jobs_ran);
}

#endif
