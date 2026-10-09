#include "config.h"

#if USE_AWS_S3

#include <Core/ServerUUID.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadPipeline.h>
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/Requests.h>
#include <IO/S3/S3Capabilities.h>
#include <IO/S3/deleteFileFromS3.h>
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
#include <limits>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ABORTED;
}

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
    explicit CancellationHTTPHandler(std::atomic_size_t & requests_, bool serve_data_, size_t failures_before_success_)
        : requests(requests_)
        , serve_data(serve_data_)
        , failures_before_success(failures_before_success_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        const auto attempt = ++requests;
        if (attempt > failures_before_success)
        {
            const bool batch_delete = request.getMethod() == "POST" && request.getURI().contains("delete");
            const String body = request.has("x-amz-copy-source") ? "<CopyObjectResult><ETag>test</ETag></CopyObjectResult>" : batch_delete ? "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>" : "";
            response.setStatusAndReason(request.getMethod() == "DELETE"
                ? Poco::Net::HTTPResponse::HTTP_NO_CONTENT : Poco::Net::HTTPResponse::HTTP_OK);
            response.setContentType("application/xml");
            response.setContentLength(body.size());
            response.setKeepAlive(false);
            response.send() << body;
            return;
        }
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
    const size_t failures_before_success;
};

class CancellationHTTPFactory final : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit CancellationHTTPFactory(bool serve_data_ = false, size_t failures_before_success_ = std::numeric_limits<size_t>::max())
        : serve_data(serve_data_)
        , failures_before_success(failures_before_success_)
    {
    }
    std::atomic_size_t requests{0};

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new CancellationHTTPHandler(requests, serve_data, failures_before_success);
    }

private:
    const bool serve_data;
    const size_t failures_before_success;
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
            config.retryStrategy = std::make_shared<S3::Client::RetryStrategy>(config.retry_strategy);
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
                = ThreadGroup::createForMergeMutate(
                    getContext().context, [original_canceled]
                    {
                        return original_canceled->load() ? CancellationRequest::StopWork : CancellationRequest::None;
                    });
            auto consumer_group
                = ThreadGroup::createForMergeMutate(
                    getContext().context, [consumer_canceled]
                    {
                        return consumer_canceled->load() ? CancellationRequest::StopWork : CancellationRequest::None;
                    });
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

TEST(OperationCancellation, CancelledAbortRecoversAfterRetryableFailure)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [] { return CancellationRequest::StopWork; });
        ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
        Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("127.0.0.1", 0));
        Poco::Net::HTTPRequestHandlerFactory::Ptr factory = new CancellationHTTPFactory(false, 1);
        auto & counters = static_cast<CancellationHTTPFactory &>(*factory);
        Poco::Net::HTTPServer server(factory, socket, new Poco::Net::HTTPServerParams);
        server.start();
        SCOPE_EXIT({ server.stopAll(true); });
        RemoteHostFilter filter;
        auto config = S3::ClientFactory::instance().createClientConfiguration(
            "us-east-1", filter, 3, {.max_retries = 2, .initial_delay_ms = 1, .max_delay_ms = 1},
            false, false, false, false, {}, {}, "http");
        config.retryStrategy = std::make_shared<S3::Client::RetryStrategy>(config.retry_strategy);
        config.endpointOverride = fmt::format("127.0.0.1:{}", socket.address().port());
        auto client = S3::Client::create(
            3, {}, std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test", "test"), config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, {.use_virtual_addressing = false});
        S3::AbortMultipartUploadRequest request;
        ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
        request.SetBucket("bucket");
        request.SetKey("key");
        request.SetUploadId("test");
        EXPECT_NO_THROW({ EXPECT_TRUE(client->AbortMultipartUpload(request).IsSuccess()); });
        EXPECT_EQ(counters.requests.load(), 2);
        EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopWork);
    }).get();
}

TEST(OperationCancellation, CleanupRetriesDoNotPermitExplicitWork)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [] { return CancellationRequest::StopWork; });
        ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
        Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("127.0.0.1", 0));
        Poco::Net::HTTPRequestHandlerFactory::Ptr factory = new CancellationHTTPFactory;
        auto & counters = static_cast<CancellationHTTPFactory &>(*factory);
        Poco::Net::HTTPServer server(factory, socket, new Poco::Net::HTTPServerParams);
        server.start();
        SCOPE_EXIT({ server.stopAll(true); });

        RemoteHostFilter filter;
        auto config = S3::ClientFactory::instance().createClientConfiguration(
            "us-east-1", filter, 3, {.max_retries = 1, .initial_delay_ms = 1, .max_delay_ms = 1},
            false, false, false, false, {}, {}, "http");
        config.s3_slow_all_threads_after_retryable_error = true;
        config.retryStrategy = std::make_shared<S3::Client::RetryStrategy>(config.retry_strategy);
        config.endpointOverride = fmt::format("127.0.0.1:{}", socket.address().port());
        auto client = S3::Client::create(
            3, {}, std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test", "test"), config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, {.use_virtual_addressing = false});

        S3::AbortMultipartUploadRequest abort;
        abort.SetBucket("bucket");
        abort.SetKey("key");
        abort.SetUploadId("test");
        ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
        size_t callbacks = 0;
        std::vector<size_t> callback_states; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        abort.SetRequestRetryHandler([&, state = size_t{0}](const Aws::AmazonWebServiceRequest &) mutable
        {
            ExecutionPhaseScope work(ExecutionPhase::Work);
            ++callbacks;
            callback_states.push_back(++state);
            S3::HeadObjectRequest ordinary;
            ordinary.SetBucket("bucket");
            ordinary.SetKey("key");
            try
            {
                client->HeadObject(ordinary);
                FAIL() << "Ordinary request ignored operation cancellation";
            }
            catch (const Exception & error)
            {
                EXPECT_EQ(error.code(), ErrorCodes::ABORTED);
            }
            S3::GetObjectRequest read;
            read.SetBucket("bucket");
            read.SetKey("key");
            EXPECT_THROW(client->GetObject(read), Exception);
            S3::PutObjectRequest write;
            write.SetBucket("bucket");
            write.SetKey("key");
            EXPECT_THROW(client->PutObject(write), Exception);
            std::async(std::launch::async, [&]
            {
                ThreadStatus concurrent_status;
                ThreadGroupSwitcher concurrent_scope(group, ThreadName::MERGE_MUTATE);
                S3::HeadObjectRequest concurrent;
                concurrent.SetBucket("bucket");
                concurrent.SetKey("key");
                EXPECT_THROW(client->HeadObject(concurrent), Exception);
            }).get();
            EXPECT_TRUE(CurrentThread::get().isQueryCanceled());
        });
        /// Both SDK and response-body retry loops retain their configured two attempts.
        EXPECT_NO_THROW({ EXPECT_FALSE(client->AbortMultipartUpload(abort).IsSuccess()); });
        EXPECT_EQ(counters.requests.load(), 4);
        EXPECT_EQ(callbacks, 2);

        /// Reusing the request must restore its original callback rather than wrapping it again.
        EXPECT_NO_THROW({ EXPECT_FALSE(client->AbortMultipartUpload(abort).IsSuccess()); });
        EXPECT_EQ(counters.requests.load(), 8);
        EXPECT_EQ(callbacks, 4);
        EXPECT_EQ(callback_states, (std::vector<size_t>{1, 2, 3, 4})); // STYLE_CHECK_ALLOW_STD_CONTAINERS
        {
            ExecutionPhaseScope work(ExecutionPhase::Work);
            EXPECT_THROW(client->AbortMultipartUpload(abort), Exception);
            EXPECT_EQ(counters.requests.load(), 8);
            S3::DeleteObjectRequest remove;
            remove.SetBucket("bucket");
            remove.SetKey("key");
            EXPECT_THROW(client->DeleteObject(remove), Exception);
        }
        S3::DeleteObjectRequest remove;
        remove.SetBucket("bucket");
        remove.SetKey("key");
        EXPECT_NO_THROW({ EXPECT_FALSE(client->DeleteObject(remove).IsSuccess()); });
        EXPECT_EQ(counters.requests.load(), 12);

        S3::DeleteObjectsRequest batch;
        batch.SetBucket("bucket");
        Aws::S3::Model::Delete objects;
        objects.AddObjects(Aws::S3::Model::ObjectIdentifier{}.WithKey("key"));
        batch.SetDelete(objects);
        EXPECT_NO_THROW({ EXPECT_FALSE(client->DeleteObjects(batch).IsSuccess()); });
        EXPECT_EQ(counters.requests.load(), 16);

        ExecutionPhaseScope work(ExecutionPhase::Work);
        S3::PocoHTTPClient transport(config);
        auto raw_abort = Aws::Http::CreateHttpRequest(
            fmt::format("http://127.0.0.1:{}/bucket/key?uploadId=test", socket.address().port()),
            Aws::Http::HttpMethod::HTTP_DELETE, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
        EXPECT_THROW(transport.MakeRequest(raw_abort, nullptr, nullptr), Exception);
        EXPECT_EQ(counters.requests.load(), 16);
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
    }).get();
}

TEST(OperationCancellation, CleanupDeleteHelpersRetainPhase)
{
    for (const bool batch_supported : {false, true})
    {
        std::async(std::launch::async, [batch_supported]
        {
            ThreadStatus status;
            auto group = ThreadGroup::createForMergeMutate(getContext().context, [] { return CancellationRequest::StopWork; });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("127.0.0.1", 0));
            Poco::Net::HTTPRequestHandlerFactory::Ptr factory = new CancellationHTTPFactory(false, 1);
            auto & counters = static_cast<CancellationHTTPFactory &>(*factory);
            Poco::Net::HTTPServer server(factory, socket, new Poco::Net::HTTPServerParams);
            server.start();
            SCOPE_EXIT({ server.stopAll(true); });
            RemoteHostFilter filter;
            auto config = S3::ClientFactory::instance().createClientConfiguration(
                "us-east-1", filter, 3, {.max_retries = 1, .initial_delay_ms = 1, .max_delay_ms = 1},
                false, false, false, false, {}, {}, "http");
            config.retryStrategy = std::make_shared<S3::Client::RetryStrategy>(config.retry_strategy);
            config.endpointOverride = fmt::format("127.0.0.1:{}", socket.address().port());
            std::shared_ptr<S3::Client> client = S3::Client::create(
                3, {}, std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test", "test"), config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, {.use_virtual_addressing = false});
            S3Capabilities capabilities(batch_supported);
            Strings removed;
            EXPECT_THROW(
                deleteFilesFromS3(client, "bucket", {"first", "second"}, true, capabilities, 1000,
                    {}, {}, {}, {}, &removed),
                Exception);
            EXPECT_EQ(counters.requests.load(), 0);
            {
                ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
                EXPECT_NO_THROW(
                    deleteFilesFromS3(client, "bucket", {"first", "second"}, true, capabilities, 1000,
                        {}, {}, {}, {}, &removed));
            }
            EXPECT_EQ(removed, (Strings{"first", "second"}));
            EXPECT_EQ(counters.requests.load(), batch_supported ? 2 : 3);
            EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
        }).get();
    }
}

TEST(OperationCancellation, CleanupPrerequisitesAndEscalation)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto state = std::make_shared<OperationCancellationState>();
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [state] { return state->getRequest(); });
        ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
        Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("127.0.0.1", 0));
        Poco::Net::HTTPRequestHandlerFactory::Ptr factory = new CancellationHTTPFactory(false, 0);
        auto & counters = static_cast<CancellationHTTPFactory &>(*factory);
        Poco::Net::HTTPServer server(factory, socket, new Poco::Net::HTTPServerParams);
        server.start();
        SCOPE_EXIT({ server.stopAll(true); });
        RemoteHostFilter filter;
        auto config = S3::ClientFactory::instance().createClientConfiguration(
            "us-east-1", filter, 3, {.max_retries = 1, .initial_delay_ms = 1, .max_delay_ms = 1},
            false, false, false, false, {}, {}, "http");
        config.retryStrategy = std::make_shared<S3::Client::RetryStrategy>(config.retry_strategy);
        config.endpointOverride = fmt::format("127.0.0.1:{}", socket.address().port());
        auto client = S3::Client::create(
            3, {}, std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test", "test"), config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, {.use_virtual_addressing = false});
        state->request(CancellationRequest::StopWork);
        ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
        S3::GetObjectRequest read;
        read.SetBucket("bucket");
        read.SetKey("source");
        EXPECT_TRUE(client->GetObject(read).IsSuccess());
        S3::PutObjectRequest write;
        write.SetBucket("bucket");
        write.SetKey("target");
        write.SetBody(std::make_shared<Aws::StringStream>());
        write.SetContentLength(0);
        EXPECT_TRUE(client->PutObject(write).IsSuccess());
        S3::CopyObjectRequest copy;
        copy.SetBucket("bucket");
        copy.SetKey("target");
        copy.SetCopySource("bucket/source");
        EXPECT_TRUE(client->CopyObject(copy).IsSuccess());
        EXPECT_EQ(counters.requests.load(), 3);
        state->request(CancellationRequest::StopAll);
        EXPECT_THROW(client->GetObject(read), Exception);
        EXPECT_THROW(client->PutObject(write), Exception);
        EXPECT_THROW(client->CopyObject(copy), Exception);
        EXPECT_EQ(counters.requests.load(), 3);
    }).get();
}

TEST(OperationCancellation, EscalationInCleanupRetryHookPreventsRedispatch)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto state = std::make_shared<OperationCancellationState>();
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [state] { return state->getRequest(); });
        ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
        Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("127.0.0.1", 0));
        Poco::Net::HTTPRequestHandlerFactory::Ptr factory = new CancellationHTTPFactory;
        auto & counters = static_cast<CancellationHTTPFactory &>(*factory);
        Poco::Net::HTTPServer server(factory, socket, new Poco::Net::HTTPServerParams);
        server.start();
        SCOPE_EXIT({ server.stopAll(true); });
        RemoteHostFilter filter;
        auto config = S3::ClientFactory::instance().createClientConfiguration(
            "us-east-1", filter, 3, {.max_retries = 2, .initial_delay_ms = 1, .max_delay_ms = 1},
            false, false, false, false, {}, {}, "http");
        config.retryStrategy = std::make_shared<S3::Client::RetryStrategy>(config.retry_strategy);
        config.endpointOverride = fmt::format("127.0.0.1:{}", socket.address().port());
        auto client = S3::Client::create(
            3, {}, std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test", "test"), config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, {.use_virtual_addressing = false});
        state->request(CancellationRequest::StopWork);
        ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
        S3::AbortMultipartUploadRequest abort;
        abort.SetBucket("bucket");
        abort.SetKey("key");
        abort.SetUploadId("test");
        size_t callbacks = 0;
        abort.SetRequestRetryHandler([&](const Aws::AmazonWebServiceRequest &)
        {
            ++callbacks;
            state->request(CancellationRequest::StopAll);
        });
        EXPECT_THROW(client->AbortMultipartUpload(abort), Exception);
        EXPECT_EQ(callbacks, 1);
        EXPECT_EQ(counters.requests.load(), 1);
    }).get();
}

TEST(OperationCancellation, CredentialRetriesFollowExecutionPhase)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto state = std::make_shared<OperationCancellationState>();
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [state] { return state->getRequest(); });
        ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
        RemoteHostFilter filter;
        auto config = S3::ClientFactory::instance().createClientConfiguration(
            "us-east-1", filter, 3, {.max_retries = 3}, false, false, false, false, {}, {}, "http");
        /// Construction installs the real credential client's strategy without fetching credentials.
        S3::AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider provider(
            config, 60, "test-role", "unused-token-file", "test-session", "us-east-1");
        Aws::Client::AWSError<Aws::Client::CoreErrors> error(
            Aws::Client::CoreErrors::NETWORK_CONNECTION, "NetworkError", "Retryable test failure", true);
        EXPECT_TRUE(config.retryStrategy->ShouldRetry(error, 0));
        state->request(CancellationRequest::StopWork);
        EXPECT_FALSE(config.retryStrategy->ShouldRetry(error, 0));
        ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
        EXPECT_TRUE(config.retryStrategy->ShouldRetry(error, 0));
        state->request(CancellationRequest::StopAll);
        EXPECT_FALSE(config.retryStrategy->ShouldRetry(error, 0));
    }).get();
}

TEST(OperationCancellation, CleanupRespectsZeroRetries)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [] { return CancellationRequest::StopWork; });
        ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
        Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("127.0.0.1", 0));
        Poco::Net::HTTPRequestHandlerFactory::Ptr factory = new CancellationHTTPFactory;
        auto & counters = static_cast<CancellationHTTPFactory &>(*factory);
        Poco::Net::HTTPServer server(factory, socket, new Poco::Net::HTTPServerParams);
        server.start();
        SCOPE_EXIT({ server.stopAll(true); });
        RemoteHostFilter filter;
        auto config = S3::ClientFactory::instance().createClientConfiguration(
            "us-east-1", filter, 3, {.max_retries = 0}, false, false, false, false, {}, {}, "http");
        config.retryStrategy = std::make_shared<S3::Client::RetryStrategy>(config.retry_strategy);
        config.endpointOverride = fmt::format("127.0.0.1:{}", socket.address().port());
        auto client = S3::Client::create(
            3, {}, std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test", "test"), config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, {.use_virtual_addressing = false});
        S3::AbortMultipartUploadRequest request;
        request.SetBucket("bucket");
        request.SetKey("key");
        request.SetUploadId("test");
        ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
        EXPECT_NO_THROW(
        {
            const auto outcome = client->AbortMultipartUpload(request);
            EXPECT_FALSE(outcome.IsSuccess());
            EXPECT_EQ(outcome.GetError().GetResponseCode(), Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE);
        });
        EXPECT_EQ(counters.requests.load(), 1);
        EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
        EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopWork);
    }).get();
}

TEST(OperationCancellation, CancellationDuringDelayCalculationPreventsRetryDispatch)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto canceled = std::make_shared<std::atomic_bool>(false);
            auto group = ThreadGroup::createForMergeMutate(
                getContext().context, [canceled]
                {
                    return canceled->load() ? CancellationRequest::StopWork : CancellationRequest::None;
                });
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
                    /// The SDK calculates the delay before invoking the request's retry hook.
                    canceled->store(true);
                    return 0;
                }

            private:
                const std::shared_ptr<std::atomic_bool> canceled;
            };

            RemoteHostFilter filter;
            auto config = S3::ClientFactory::instance().createClientConfiguration(
                "us-east-1", filter, 3, {.max_retries = 3}, false, false, false, false, {}, {}, "http");
            config.retryStrategy = std::make_shared<S3::Client::RetryStrategy>(config.retry_strategy);
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
