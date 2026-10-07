#include "config.h"

#if USE_AWS_S3

#include <Core/ServerUUID.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadPipeline.h>
#include <IO/RemoteIOCleanup.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/Requests.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpClientFactory.h>
#include <base/scope_guard.h>
#include <gtest/gtest.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>

#include <algorithm>
#include <filesystem>
#include <future>
#include <fmt/format.h>

namespace DB
{

namespace FileCacheSetting
{
extern const FileCacheSettingsString path;
extern const FileCacheSettingsUInt64 max_size;
extern const FileCacheSettingsUInt64 max_elements;
extern const FileCacheSettingsUInt64 max_file_segment_size;
extern const FileCacheSettingsUInt64 boundary_alignment;
extern const FileCacheSettingsUInt64 background_download_threads;
}

namespace
{
class CancellationHTTPHandler final : public Poco::Net::HTTPRequestHandler
{
public:
    explicit CancellationHTTPHandler(std::atomic_size_t & requests_, bool serve_data_)
        : requests(requests_)
        , serve_data(serve_data_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        ++requests;
        if (serve_data)
        {
            const String data = "abcdefghijkl";
            const auto range = request.get("Range", "bytes=0-11");
            const size_t begin = std::stoull(range.substr(6));
            const size_t end = std::min(data.size() - 1, static_cast<size_t>(std::stoull(range.substr(range.find('-') + 1))));
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_PARTIAL_CONTENT);
            response.setContentLength(end - begin + 1);
            response.set("Content-Range", fmt::format("bytes {}-{}/{}", begin, end, data.size()));
            response.setKeepAlive(false);
            response.send() << data.substr(begin, end - begin + 1);
            return;
        }
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE);
        response.setContentType("application/xml");
        response.setKeepAlive(false);
        response.send() << "<Error><Code>SlowDown</Code><Message>Retryable test failure</Message></Error>";
    }

private:
    std::atomic_size_t & requests;
    const bool serve_data;
};

class CancellationHTTPFactory final : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit CancellationHTTPFactory(bool serve_data_ = false)
        : serve_data(serve_data_)
    {
    }
    std::atomic_size_t requests{0};

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new CancellationHTTPHandler(requests, serve_data);
    }

private:
    const bool serve_data;
};
}

class S3CacheCancellationHandoff : public ::testing::TestWithParam<std::tuple<bool, bool>>
{
};

TEST_P(S3CacheCancellationHandoff, ReusedS3ReaderUsesCurrentOperation)
{
    const auto [cancel_original, async_consumer] = GetParam();
    std::async(
        std::launch::async,
        [cancel_original, async_consumer]
        {
            ThreadStatus status;
            Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("127.0.0.1", 0));
            Poco::Net::HTTPRequestHandlerFactory::Ptr factory = new CancellationHTTPFactory(true);
            Poco::Net::HTTPServer server(factory, socket, new Poco::Net::HTTPServerParams);
            server.start();
            SCOPE_EXIT({ server.stopAll(true); });

            RemoteHostFilter filter;
            auto config = S3::ClientFactory::instance().createClientConfiguration(
                "us-east-1", filter, 3, {.max_retries = 0}, false, false, false, false, {}, {}, "http");
            config.endpointOverride = fmt::format("127.0.0.1:{}", socket.address().port());
            std::shared_ptr<S3::Client> client = S3::Client::create(
                3,
                {},
                std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test", "test"),
                config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                {.use_virtual_addressing = false});

            ServerUUID::setRandomForUnitTests();
            const String cache_name = fmt::format("s3_cancellation_{}_{}", cancel_original, async_consumer);
            const auto cache_path = std::filesystem::current_path() / cache_name;
            FileCacheSettings cache_settings;
            cache_settings[FileCacheSetting::path] = cache_path / "";
            cache_settings[FileCacheSetting::max_size] = 1024;
            cache_settings[FileCacheSetting::max_elements] = 10;
            cache_settings[FileCacheSetting::max_file_segment_size] = 1024;
            cache_settings[FileCacheSetting::boundary_alignment] = 0;
            cache_settings[FileCacheSetting::background_download_threads] = 0;
            auto cache = FileCacheFactory::instance().getOrCreate(cache_name, cache_settings, "");
            cache->initialize();
            SCOPE_EXIT({
                cache.reset();
                FileCacheFactory::instance().clear();
                std::filesystem::remove_all(cache_path);
            });

            auto original_canceled = std::make_shared<std::atomic_bool>(false);
            auto consumer_canceled = std::make_shared<std::atomic_bool>(false);
            auto original_group
                = ThreadGroup::createForMergeMutate(getContext().context, [original_canceled] { return original_canceled->load(); });
            auto consumer_group
                = ThreadGroup::createForMergeMutate(getContext().context, [consumer_canceled] { return consumer_canceled->load(); });
            size_t sources_created = 0;
            ThreadPoolRemoteFSReader async_reader(1, 2);
            auto create_reader = [&](bool async)
            {
                ReadSettings settings;
                settings.remote_fs_settings.buffer_size = 4;
                settings.local_fs_settings.buffer_size = 4;
                ReadPipeline pipeline;
                pipeline.setSource(
                    [&](const StoredObject &, const ReadSettings & read_settings, bool use_external_buffer, bool restrict_seek)
                    {
                        ++sources_created;
                        return std::make_unique<ReadBufferFromS3>(
                            client,
                            "bucket",
                            "key",
                            "",
                            S3::S3RequestSettings{},
                            read_settings,
                            use_external_buffer,
                            0,
                            12,
                            restrict_seek,
                            12);
                    },
                    StoredObjects{StoredObject("bucket/key", "key", 12)},
                    settings);
                pipeline.needFilesystemCache(cache, FilesystemCacheSettings{});
                if (async)
                    pipeline.needAsyncPrefetch(async_reader);
                return pipeline.build();
            };

            std::unique_ptr<ReadBufferFromFileBase> original;
            String prefix(4, '\0');
            {
                ThreadGroupSwitcher scope(original_group, ThreadName::MERGE_MUTATE);
                original = create_reader(false);
                original->readStrict(prefix.data(), prefix.size());
                ASSERT_EQ(prefix, "abcd");
            }
            ASSERT_EQ(sources_created, 1u);
            const auto segments = cache->getFileSegmentInfos(FileCache::getCommonOrigin().user_id);
            ASSERT_EQ(segments.size(), 1u);
            ASSERT_EQ(segments.front().downloaded_size, 4u);

            original_canceled->store(cancel_original);
            consumer_canceled->store(!cancel_original);
            ThreadGroupSwitcher scope(consumer_group, ThreadName::MERGE_MUTATE);
            auto consumer = create_reader(async_consumer);
            consumer->readStrict(prefix.data(), prefix.size());
            ASSERT_EQ(prefix, "abcd");
            if (async_consumer)
                consumer->prefetch(Priority{});
            if (cancel_original)
            {
                String tail;
                readStringUntilEOF(tail, *consumer);
                EXPECT_EQ(tail, "efghijkl");
            }
            else
                EXPECT_THROW(consumer->next(), Exception);
            EXPECT_EQ(sources_created, 1u) << "The source must be reused, not replaced";
        })
        .get();
}

INSTANTIATE_TEST_SUITE_P(OperationCancellation, S3CacheCancellationHandoff, ::testing::Combine(::testing::Bool(), ::testing::Bool()));

TEST(OperationCancellation, HTTPTransportBlocksCancelledAttemptsButAllowsInitialAbort)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto group = ThreadGroup::createForMergeMutate(getContext().context, [] { return true; });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("127.0.0.1", 0));
            Poco::Net::HTTPRequestHandlerFactory::Ptr factory = new CancellationHTTPFactory;
            auto & counters = static_cast<CancellationHTTPFactory &>(*factory);
            Poco::Net::HTTPServer server(factory, socket, new Poco::Net::HTTPServerParams);
            server.start();

            RemoteHostFilter filter;
            auto config = S3::ClientFactory::instance().createClientConfiguration(
                "us-east-1", filter, 3, {.max_retries = 3}, false, false, false, false, {}, {}, "http");
            S3::PocoHTTPClient transport(config);
            const auto endpoint = fmt::format("http://127.0.0.1:{}/bucket/key", socket.address().port());
            auto make_request = [&](Aws::Http::HttpMethod method, const String & suffix)
            { return Aws::Http::CreateHttpRequest(endpoint + suffix, method, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod); };

            auto ordinary = make_request(Aws::Http::HttpMethod::HTTP_HEAD, "");
            EXPECT_THROW(transport.MakeRequest(ordinary, nullptr, nullptr), Exception);
            EXPECT_EQ(counters.requests.load(), 0);

            auto abort = make_request(Aws::Http::HttpMethod::HTTP_DELETE, "?uploadId=test");
            EXPECT_NO_THROW(transport.MakeRequest(abort, nullptr, nullptr));
            EXPECT_EQ(counters.requests.load(), 1);
            abort->SetHeaderValue("amz-sdk-request", "attempt=2");
            EXPECT_THROW(transport.MakeRequest(abort, nullptr, nullptr), Exception);
            EXPECT_EQ(counters.requests.load(), 1);
            EXPECT_TRUE(CurrentThread::get().isQueryCanceled());

            config.endpointOverride = fmt::format("127.0.0.1:{}", socket.address().port());
            config.retryStrategy = std::make_shared<S3::Client::RetryStrategy>(config.retry_strategy);
            auto client = S3::Client::create(
                3,
                {},
                std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test", "test"),
                config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                {.use_virtual_addressing = false});
            S3::AbortMultipartUploadRequest sdk_abort;
            sdk_abort.SetBucket("bucket");
            sdk_abort.SetKey("key");
            sdk_abort.SetUploadId("test");
            EXPECT_THROW(client->AbortMultipartUpload(sdk_abort), Exception);
            EXPECT_EQ(counters.requests.load(), 2);

            EXPECT_FALSE(RemoteIOCleanupScope::isActive());
            {
                RemoteIOCleanupScope cleanup;
                {
                    RemoteIOCleanupScope nested_cleanup;
                    EXPECT_TRUE(RemoteIOCleanupScope::isActive());
                }
                EXPECT_TRUE(RemoteIOCleanupScope::isActive());
                S3::HeadObjectRequest ordinary_head;
                ordinary_head.SetBucket("bucket");
                ordinary_head.SetKey("key");
                EXPECT_THROW(client->HeadObject(ordinary_head), Exception);
                EXPECT_EQ(counters.requests.load(), 2);

                S3::DeleteObjectRequest remove;
                remove.SetBucket("bucket");
                remove.SetKey("key");
                EXPECT_THROW(client->DeleteObject(remove), Exception);
                EXPECT_EQ(counters.requests.load(), 3);

                S3::DeleteObjectsRequest remove_batch;
                remove_batch.SetBucket("bucket");
                Aws::S3::Model::Delete objects;
                objects.AddObjects(Aws::S3::Model::ObjectIdentifier{}.WithKey("key"));
                remove_batch.SetDelete(objects);
                EXPECT_THROW(client->DeleteObjects(remove_batch), Exception);
                EXPECT_EQ(counters.requests.load(), 4);
                EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
            }
            EXPECT_FALSE(RemoteIOCleanupScope::isActive());
            S3::DeleteObjectRequest ordinary_delete;
            ordinary_delete.SetBucket("bucket");
            ordinary_delete.SetKey("key");
            EXPECT_THROW(client->DeleteObject(ordinary_delete), Exception);
            EXPECT_EQ(counters.requests.load(), 4);
            server.stopAll(true);
        })
        .get();
}

TEST(OperationCancellation, ExistingS3RetryStrategyObservesOperationWithoutRequestHook)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto canceled = std::make_shared<std::atomic_bool>(false);
            auto group = ThreadGroup::createForMergeMutate(getContext().context, [canceled] { return canceled->load(); });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            S3::Client::RetryStrategy strategy(S3::PocoHTTPClientConfiguration::RetryStrategy{});
            Aws::Client::AWSError<Aws::Client::CoreErrors> error(
                Aws::Client::CoreErrors::NETWORK_CONNECTION, "TestNetworkFailure", "Retryable test failure", true);

            EXPECT_TRUE(strategy.ShouldRetry(error, 0));
            canceled->store(true);
            EXPECT_FALSE(strategy.ShouldRetry(error, 0));
            canceled->store(false);
            EXPECT_FALSE(strategy.ShouldRetry(error, 0));
        })
        .get();
}

TEST(OperationCancellation, CancellationAfterRetryDecisionPreventsNextTransportAttempt)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto canceled = std::make_shared<std::atomic_bool>(false);
            auto group = ThreadGroup::createForMergeMutate(getContext().context, [canceled] { return canceled->load(); });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("127.0.0.1", 0));
            Poco::Net::HTTPRequestHandlerFactory::Ptr factory = new CancellationHTTPFactory;
            auto & counters = static_cast<CancellationHTTPFactory &>(*factory);
            Poco::Net::HTTPServer server(factory, socket, new Poco::Net::HTTPServerParams);
            server.start();
            SCOPE_EXIT({ server.stopAll(true); });

            class CancelAtBackoff final : public S3::Client::RetryStrategy
            {
            public:
                explicit CancelAtBackoff(std::shared_ptr<std::atomic_bool> canceled_)
                    : S3::Client::RetryStrategy({.max_retries = 3})
                    , canceled(std::move(canceled_))
                {
                }

                long CalculateDelayBeforeNextRetry( // NOLINT(google-runtime-int)
                    const Aws::Client::AWSError<Aws::Client::CoreErrors> &, long) const override // NOLINT(google-runtime-int)
                {
                    /// The SDK already decided to retry. A zero delay makes cancellation at its tail deterministic.
                    canceled->store(true);
                    return 0;
                }

            private:
                const std::shared_ptr<std::atomic_bool> canceled;
            };

            RemoteHostFilter filter;
            auto config = S3::ClientFactory::instance().createClientConfiguration(
                "us-east-1", filter, 3, {.max_retries = 3}, false, false, false, false, {}, {}, "http");
            config.endpointOverride = fmt::format("127.0.0.1:{}", socket.address().port());
            config.retryStrategy = std::make_shared<CancelAtBackoff>(canceled);
            auto client = S3::Client::create(
                3,
                {},
                std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test", "test"),
                config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                {.use_virtual_addressing = false});
            S3::HeadObjectRequest request;
            request.SetBucket("bucket");
            request.SetKey("key");
            EXPECT_THROW(client->HeadObject(request), Exception);
            EXPECT_TRUE(canceled->load());
            EXPECT_EQ(counters.requests.load(), 1);
        })
        .get();
}


}

#endif
