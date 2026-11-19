#include <gtest/gtest.h>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/WriteBufferFromAzureDataLakeStorage.h>
#include <IO/AzureBlobStorage/PocoHTTPClient.h>
#include <IO/WriteSettings.h>
#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/NullStream.h>
#include <Poco/StreamCopier.h>

#include <functional>
#include <mutex>
#include <vector>

using DB::WriteOutcome;

namespace
{

/// Answers each request by its number: the method, the query and the status code.
using Answer = std::function<Poco::Net::HTTPResponse::HTTPStatus(size_t number, const std::string & method, const std::string & uri)>;

class ScriptedHandler : public Poco::Net::HTTPRequestHandler
{
public:
    ScriptedHandler(const Answer & answer_, std::mutex & mutex_, size_t & count_) : answer(answer_), mutex(mutex_), count(count_) {}

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        Poco::NullOutputStream sink;
        Poco::StreamCopier::copyStream(request.stream(), sink);
        size_t number = 0;
        {
            std::lock_guard lock(mutex);
            number = count++;
        }
        response.setStatus(answer(number, request.getMethod(), request.getURI()));
        response.set("ETag", "\"0x1\"");
        response.set("Last-Modified", "Wed, 18 Nov 2026 00:00:00 GMT");
        response.set("x-ms-request-id", "test");
        response.set("x-ms-request-server-encrypted", "true");
        response.setContentLength(0);
        response.send();
    }

private:
    const Answer & answer;
    std::mutex & mutex;
    size_t & count;
};

class ScriptedHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit ScriptedHandlerFactory(Answer answer_) : answer(std::move(answer_)) {}

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new ScriptedHandler(answer, mutex, count);
    }

private:
    Answer answer;
    std::mutex mutex;
    size_t count = 0;
};

struct WriteResult
{
    WriteOutcome outcome;
    /// The error of the write, empty if it succeeded.
    std::string error;
};

/// Writes `{}` with `If-None-Match: *` to an ADLS Gen2 server that answers as `answer` says.
WriteResult write(Answer answer)
{
    Poco::Net::ServerSocket server_socket(Poco::Net::SocketAddress("127.0.0.1", 0));
    Poco::Net::HTTPServer server(new ScriptedHandlerFactory(std::move(answer)), server_socket, new Poco::Net::HTTPServerParams);
    server.start();

    DB::RemoteHostFilter remote_host_filter;
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Transport.Transport = std::make_shared<DB::PocoAzureHTTPClient>(DB::PocoAzureHTTPClientConfiguration{
        .remote_host_filter = remote_host_filter,
        .max_redirects = 3,
        .for_disk_azure = false,
        .request_throttler = {},
        .extra_headers = {},
    });
    /// Every attempt is then one call of the writer.
    client_options.Retry.MaxRetries = 0;

    DB::AzureBlobStorage::Endpoint endpoint;
    endpoint.storage_account_url = "http://" + server_socket.address().toString();
    endpoint.container_name = "container";

    DB::WriteSettings write_settings;
    write_settings.object_storage_write_if_none_match = "*";

    std::optional<WriteOutcome> outcome;
    std::string error;
    {
        DB::WriteBufferFromAzureDataLakeStorage buffer(
            endpoint,
            DB::AzureBlobStorage::AuthMethod{},
            /* drop_credentials = */ true,
            client_options,
            "metadata/v2.metadata.json",
            DB::DBMS_DEFAULT_BUFFER_SIZE,
            write_settings,
            std::make_shared<DB::AzureBlobStorage::RequestSettings>());
        try
        {
            buffer.write("{}", 2);
            buffer.finalize();
        }
        catch (...)
        {
            /// `finalize` cancels the buffer.
            error = DB::getCurrentExceptionMessage(false);
        }
        outcome = buffer.getWriteOutcome();
    }

    server.stop();
    EXPECT_TRUE(outcome.has_value()) << error;
    return {outcome.value_or(WriteOutcome::Applied), error};
}

bool isCreate(const std::string & method, const std::string & uri)
{
    return method == "PUT" && uri.contains("resource=file");
}

}

TEST(AdlsWriteOutcome, CreateOfAnExistingFileFailsTheCondition)
{
    const auto result = write([](size_t, const std::string & method, const std::string & uri)
    {
        return isCreate(method, uri) ? Poco::Net::HTTPResponse::HTTP_CONFLICT : Poco::Net::HTTPResponse::HTTP_OK;
    });
    EXPECT_EQ(result.outcome, WriteOutcome::ConditionFailed) << result.error;
}

TEST(AdlsWriteOutcome, ConflictAfterAnAttemptThatMayHaveBeenAppliedIsUnknown)
{
    /// The first `Create` may have claimed the path before its answer failed, so the 409 can be the answer to a replay.
    const auto result = write([](size_t number, const std::string & method, const std::string & uri)
    {
        if (!isCreate(method, uri))
            return Poco::Net::HTTPResponse::HTTP_OK;
        return number == 0 ? Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR : Poco::Net::HTTPResponse::HTTP_CONFLICT;
    });
    EXPECT_EQ(result.outcome, WriteOutcome::Unknown) << result.error;
}

TEST(AdlsWriteOutcome, FailureAfterTheClaimIsUnknown)
{
    const auto result = write([](size_t, const std::string & method, const std::string & uri)
    {
        if (isCreate(method, uri))
            return Poco::Net::HTTPResponse::HTTP_CREATED;
        return uri.contains("action=flush") ? Poco::Net::HTTPResponse::HTTP_FORBIDDEN : Poco::Net::HTTPResponse::HTTP_ACCEPTED;
    });
    EXPECT_EQ(result.outcome, WriteOutcome::Unknown) << result.error;
    EXPECT_TRUE(result.error.contains("Flush failed")) << result.error;
}

TEST(AdlsWriteOutcome, FlushedWriteIsApplied)
{
    const auto result = write([](size_t, const std::string & method, const std::string & uri)
    {
        if (isCreate(method, uri))
            return Poco::Net::HTTPResponse::HTTP_CREATED;
        return uri.contains("action=flush") ? Poco::Net::HTTPResponse::HTTP_OK : Poco::Net::HTTPResponse::HTTP_ACCEPTED;
    });
    EXPECT_EQ(result.outcome, WriteOutcome::Applied) << result.error;
}

#endif
