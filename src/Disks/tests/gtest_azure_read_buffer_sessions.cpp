#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Common/CurrentThread.h>
#include <Common/HTTPConnectionPool.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/AzureBlobStorage/PocoHTTPClient.h>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>

#include <azure/storage/blobs/blob_container_client.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <string_view>

namespace
{

constexpr std::string_view blob = "0123456789abcdef";

class BlobRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit BlobRequestHandler(std::atomic<size_t> & requests_) : requests(requests_) { }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        ++requests;
        response.setKeepAlive(true);
        response.set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        response.set("ETag", "\"test-etag\"");
        response.set("x-ms-blob-type", "BlockBlob");

        if (request.getURI() == "/container/empty")
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            response.setContentLength(0);
            response.send();
            return;
        }

        const std::string range = request.get("x-ms-range", "bytes=0-");
        const size_t dash = range.find('-');
        const size_t begin = std::stoull(range.substr(6, dash - 6));
        const size_t end = dash + 1 == range.size()
            ? blob.size() - 1
            : std::min(static_cast<size_t>(std::stoull(range.substr(dash + 1))), blob.size() - 1);

        if (begin > end)
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_REQUESTED_RANGE_NOT_SATISFIABLE);
            response.setContentLength(0);
            response.send();
            return;
        }

        response.setStatus(Poco::Net::HTTPResponse::HTTP_PARTIAL_CONTENT);
        response.set("Content-Range", "bytes " + std::to_string(begin) + "-" + std::to_string(end) + "/" + std::to_string(blob.size()));
        response.setContentLength(end - begin + 1);
        response.send().write(blob.data() + begin, end - begin + 1);
    }

private:
    std::atomic<size_t> & requests;
};

class BlobRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit BlobRequestHandlerFactory(std::atomic<size_t> & requests_) : requests(requests_) { }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new BlobRequestHandler(requests);
    }

private:
    std::atomic<size_t> & requests;
};

class AzureReadBufferSessions : public testing::Test
{
protected:
    AzureReadBufferSessions()
        : server_socket(Poco::Net::SocketAddress("127.0.0.1", 0))
        , server(new BlobRequestHandlerFactory(requests), server_socket, new Poco::Net::HTTPServerParams)
    {
    }

    void SetUp() override
    {
        auto & pools = DB::HTTPConnectionPools::instance();
        const DB::HTTPConnectionPools::Limits limits;
        pools.setLimits(limits, limits, limits);
        pools.dropCache();
        DB::CurrentThread::getProfileEvents().resetCounters();
        server.start();

        const std::string url = "http://" + server_socket.address().toString();
        pool = pools.getPool(DB::HTTPConnectionGroupType::STORAGE, Poco::URI(url), DB::ProxyConfiguration{});
        connections_before = CurrentMetrics::get(pool->getMetrics().active_count);
        stored_before = CurrentMetrics::get(pool->getMetrics().stored_count);

        Azure::Storage::Blobs::BlobClientOptions options;
        options.Retry.MaxRetries = 0;
        options.Transport.Transport = std::make_shared<DB::PocoAzureHTTPClient>(DB::PocoAzureHTTPClientConfiguration{
            .remote_host_filter = remote_host_filter,
            .max_redirects = 0,
            .for_disk_azure = false,
            .request_throttler = {},
            .extra_headers = {},
        });
        container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
            Azure::Storage::Blobs::BlobContainerClient(url + "/container", options), "");
    }

    void TearDown() override
    {
        container_client.reset();
        pool.reset();
        DB::HTTPConnectionPools::instance().dropCache();
        server.stop();
    }

    std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeBuffer(
        size_t buffer_size = blob.size(), bool restricted_seek = false, bool external_buffer = false, const std::string & name = "blob")
    {
        DB::ReadSettings settings;
        settings.remote_fs_settings.buffer_size = buffer_size;
        settings.remote_fs_settings.min_bytes_for_seek = blob.size();
        return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(
            container_client, name, settings, 1, 1, external_buffer, restricted_seek);
    }

    void expectInUse(int64_t expected) const
    {
        const auto & metrics = pool->getMetrics();
        const auto connections = CurrentMetrics::get(metrics.active_count) - connections_before;
        const auto stored = CurrentMetrics::get(metrics.stored_count) - stored_before;
        EXPECT_EQ(expected, connections - stored);
    }

    void expectStored(int64_t expected) const
    {
        EXPECT_EQ(expected, CurrentMetrics::get(pool->getMetrics().stored_count) - stored_before);
    }

    static std::string readBytes(DB::ReadBufferFromAzureBlobStorage & reader, size_t size)
    {
        std::string result(size, '\0');
        reader.readStrict(result.data(), result.size());
        return result;
    }

    std::atomic<size_t> requests{0};
    DB::RemoteHostFilter remote_host_filter;
    Poco::Net::ServerSocket server_socket;
    Poco::Net::HTTPServer server;
    DB::IHTTPConnectionPoolForEndpoint::Ptr pool;
    DB::ReadBufferFromAzureBlobStorage::ContainerClientPtr container_client;
    int64_t connections_before = 0;
    int64_t stored_before = 0;
};

TEST_F(AzureReadBufferSessions, ReusesSessionWhileCompletedReadersKeepBufferedData)
{
    auto first = makeBuffer();
    ASSERT_TRUE(first->next());
    EXPECT_EQ(first->available(), blob.size());
    expectInUse(0);
    expectStored(1);

    auto second = makeBuffer();
    ASSERT_TRUE(second->next());
    expectInUse(0);
    EXPECT_EQ(DB::CurrentThread::getProfileEvents()[pool->getMetrics().created], 1);
    EXPECT_EQ(DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused], 1);
    EXPECT_EQ(requests.load(), 2);

    EXPECT_EQ(readBytes(*first, blob.size()), blob);
    EXPECT_EQ(readBytes(*second, blob.size()), blob);
    EXPECT_TRUE(first->eof());
    EXPECT_TRUE(first->eof());
    EXPECT_TRUE(second->eof());
    EXPECT_TRUE(second->eof());
    EXPECT_EQ(requests.load(), 2);
}

TEST_F(AzureReadBufferSessions, KeepsSessionUntilTheResponseIsFullyBuffered)
{
    auto reader = makeBuffer(4);
    for (size_t offset = 0; offset < blob.size(); offset += 4)
    {
        ASSERT_TRUE(reader->next());
        expectInUse(offset + 4 == blob.size() ? 0 : 1);
        EXPECT_EQ(readBytes(*reader, 4), blob.substr(offset, 4));
    }
    EXPECT_TRUE(reader->eof());
    EXPECT_EQ(requests.load(), 1);
}

TEST_F(AzureReadBufferSessions, DestructionReleasesAPartiallyBufferedResponse)
{
    {
        auto reader = makeBuffer(4);
        ASSERT_TRUE(reader->next());
        expectInUse(1);
        EXPECT_EQ(readBytes(*reader, 1), "0");
    }
    expectInUse(0);
    EXPECT_EQ(requests.load(), 1);
}

TEST_F(AzureReadBufferSessions, SeeksWithinTheCompletedBufferWithoutRequestingAgain)
{
    auto reader = makeBuffer();
    ASSERT_TRUE(reader->next());
    expectInUse(0);
    EXPECT_EQ(readBytes(*reader, 4), "0123");
    EXPECT_EQ(reader->seek(2, SEEK_SET), 2);
    EXPECT_EQ(readBytes(*reader, 3), "234");
    EXPECT_EQ(requests.load(), 1);
}

TEST_F(AzureReadBufferSessions, SeeksOutsideTheCompletedBufferAndDownloadsAgain)
{
    auto reader = makeBuffer(8);
    EXPECT_EQ(readBytes(*reader, blob.size()), blob);
    expectInUse(0);

    EXPECT_EQ(reader->seek(2, SEEK_SET), 2);
    ASSERT_TRUE(reader->next());
    expectInUse(1);
    EXPECT_EQ(readBytes(*reader, 8), "23456789");
    ASSERT_TRUE(reader->next());
    expectInUse(0);
    EXPECT_EQ(readBytes(*reader, 6), "abcdef");
    EXPECT_TRUE(reader->eof());
    EXPECT_EQ(requests.load(), 2);
}

TEST_F(AzureReadBufferSessions, SeeksForwardThroughTheResponseEndWithoutRequestingAgain)
{
    auto reader = makeBuffer(4);
    EXPECT_EQ(readBytes(*reader, 1), "0");
    expectInUse(1);
    EXPECT_EQ(reader->seek(blob.size(), SEEK_SET), blob.size());
    expectInUse(0);
    EXPECT_EQ(reader->getPosition(), blob.size());
    EXPECT_TRUE(reader->eof());
    EXPECT_TRUE(reader->eof());
    EXPECT_EQ(requests.load(), 1);
}

TEST_F(AzureReadBufferSessions, KeepsSeekRestrictionsAfterReleasingTheSession)
{
    auto reader = makeBuffer(blob.size(), /* restricted_seek */ true);
    EXPECT_EQ(reader->seek(4, SEEK_SET), 4);
    ASSERT_TRUE(reader->next());
    expectInUse(0);
    EXPECT_EQ(reader->seek(4, SEEK_SET), 4);
    EXPECT_THROW(reader->seek(5, SEEK_SET), DB::Exception);
    EXPECT_EQ(readBytes(*reader, 12), "456789abcdef");
    EXPECT_THROW(reader->seek(0, SEEK_SET), DB::Exception);
    EXPECT_EQ(requests.load(), 1);
}

TEST_F(AzureReadBufferSessions, ChangesAndClearsTheRightBoundAfterReleasingTheSession)
{
    auto reader = makeBuffer();
    reader->setReadUntilPosition(4);
    ASSERT_TRUE(reader->next());
    expectInUse(0);
    EXPECT_EQ(readBytes(*reader, 4), "0123");
    EXPECT_TRUE(reader->eof());
    EXPECT_TRUE(reader->eof());
    EXPECT_EQ(requests.load(), 1);

    reader->setReadUntilPosition(8);
    ASSERT_TRUE(reader->next());
    expectInUse(0);
    EXPECT_EQ(readBytes(*reader, 4), "4567");
    EXPECT_TRUE(reader->eof());

    EXPECT_EQ(reader->seek(2, SEEK_SET), 2);
    reader->setReadUntilPosition(6);
    ASSERT_TRUE(reader->next());
    expectInUse(0);
    EXPECT_EQ(readBytes(*reader, 2), "23");
    reader->setReadUntilEnd();
    EXPECT_EQ(reader->getPosition(), 4);
    ASSERT_TRUE(reader->next());
    expectInUse(0);
    EXPECT_EQ(readBytes(*reader, 12), "456789abcdef");
    EXPECT_TRUE(reader->eof());
    EXPECT_EQ(requests.load(), 4);
}

TEST_F(AzureReadBufferSessions, ReleasesTheSessionWhenAnExternalBufferContainsTheResponseEnd)
{
    auto reader = makeBuffer(4, /* restricted_seek */ false, /* external_buffer */ true);
    std::array<char, 8> first{};
    reader->set(first.data(), first.size());
    ASSERT_TRUE(reader->next());
    EXPECT_EQ(reader->position(), first.data());
    expectInUse(1);
    EXPECT_EQ(readBytes(*reader, first.size()), "01234567");

    std::array<char, 8> second{};
    reader->set(second.data(), second.size());
    ASSERT_TRUE(reader->next());
    EXPECT_EQ(reader->position(), second.data());
    expectInUse(0);
    EXPECT_EQ(readBytes(*reader, second.size()), "89abcdef");
    EXPECT_TRUE(reader->eof());
    EXPECT_TRUE(reader->eof());
    EXPECT_EQ(requests.load(), 1);
}

TEST_F(AzureReadBufferSessions, ReleasesAnEmptyResponseWithoutRequestingAgain)
{
    auto reader = makeBuffer(blob.size(), /* restricted_seek */ false, /* external_buffer */ false, "empty");
    EXPECT_FALSE(reader->next());
    expectInUse(0);
    expectStored(1);
    EXPECT_TRUE(reader->eof());
    EXPECT_TRUE(reader->eof());
    EXPECT_EQ(requests.load(), 1);
}

}

#endif
