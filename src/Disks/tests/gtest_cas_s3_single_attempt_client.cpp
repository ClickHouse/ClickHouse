#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Client.h>
#include <IO/S3/URI.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3Defines.h>
#include <IO/S3Settings.h>
#include <Common/RemoteHostFilter.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Settings.h>
#include <base/defines.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include <fmt/format.h>

#include <Poco/AutoPtr.h>
#include <Poco/SharedPtr.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Util/XMLConfiguration.h>

#include <IO/S3Common.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>

/// The single-attempt client clone must cap its connect timeout at the value the mount froze at open,
/// never at the disk's (possibly wider, possibly reloaded, possibly unbounded) own connect timeout.

namespace
{

/// A `PocoHTTPClientConfiguration` that never resolves a real socket: `endpointOverride` points at a
/// port nothing listens on, so a test that never issues a request (every assertion here reads
/// `getClientConfiguration()`, which needs no network) never blocks or flakes on connection refusal.
DB::S3::PocoHTTPClientConfiguration clientConfigurationForTest(long connect_ms)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration cfg = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ true,
        /* s3_slow_all_threads_after_retryable_error = */ true,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ true,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {});
    cfg.endpointOverride = "http://127.0.0.1:1";
    cfg.connectTimeoutMs = connect_ms;
    cfg.requestTimeoutMs = 30000;
    return cfg;
}

DB::S3::ClientSettings clientSettingsForTest()
{
    return DB::S3::ClientSettings{
        .use_virtual_addressing = false,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };
}

/// A `<disk>` config section carrying `connect_timeout_ms`, for driving a reload through the real
/// `applyNewSettings` path (as a live disk's config reload would) rather than swapping the client
/// directly. Explicit static credentials keep the reload from falling through to the EC2 instance
/// metadata credentials provider (no access/secret key configured means "try every other provider"),
/// which would otherwise probe an unreachable metadata endpoint on every reload.
Poco::AutoPtr<Poco::Util::XMLConfiguration> configWithConnectTimeout(long connect_timeout_ms)
{
    std::istringstream xml_stream( // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        "<clickhouse><disk>"
        "<connect_timeout_ms>" + std::to_string(connect_timeout_ms) + "</connect_timeout_ms>"
        "<access_key_id>ACCESS_KEY_ID</access_key_id>"
        "<secret_access_key>SECRET_ACCESS_KEY</secret_access_key>"
        "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(xml_stream);
}

std::shared_ptr<DB::S3ObjectStorage> makeStorageForTest(long connect_ms)
{
    auto client = DB::S3::ClientFactory::instance().create(
        clientConfigurationForTest(connect_ms), clientSettingsForTest(),
        "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{});
    return std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(),
        DB::S3::URI("http://127.0.0.1:1/bucket/"), DB::S3Capabilities{},
        DB::ObjectStorageKeyGeneratorPtr{}, "disk");
}

/// A handler that always answers with a canned, verb-appropriate response after sleeping `delay` --
/// simulating a slow-but-eventually-answering S3 endpoint. The sleep is deliberate test scaffolding for
/// a real elapsed-time discriminator, not a workaround for a race condition. Every request increments
/// `requests_seen`, the only way a caller can prove a client's retry strategy never reissued.
class DelayedResponseRequestHandler : public Poco::Net::HTTPRequestHandler
{
    std::atomic<size_t> & requests_seen;
    std::chrono::milliseconds delay;
    std::function<void(Poco::Net::HTTPServerResponse &)> respond;

public:
    DelayedResponseRequestHandler(
        std::atomic<size_t> & requests_seen_,
        std::chrono::milliseconds delay_,
        std::function<void(Poco::Net::HTTPServerResponse &)> respond_)
        : requests_seen(requests_seen_), delay(delay_), respond(std::move(respond_))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response) override
    {
        ++requests_seen;
        std::this_thread::sleep_for(delay);
        respond(response);
    }
};

class DelayedResponseRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    std::atomic<size_t> & requests_seen;
    std::chrono::milliseconds delay;
    std::function<void(Poco::Net::HTTPServerResponse &)> respond;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new DelayedResponseRequestHandler(requests_seen, delay, respond);
    }

public:
    DelayedResponseRequestHandlerFactory(
        std::atomic<size_t> & requests_seen_,
        std::chrono::milliseconds delay_,
        std::function<void(Poco::Net::HTTPServerResponse &)> respond_)
        : requests_seen(requests_seen_), delay(delay_), respond(std::move(respond_))
    {
    }

    ~DelayedResponseRequestHandlerFactory() override = default;
};

/// A real local HTTP server standing in for S3, one verb at a time: every request gets the same canned
/// response after `delay`. Pointing a genuine `S3ObjectStorage` at it and comparing a `Default` call
/// (succeeds -- the delay is well under the base client's request timeout) against a `SingleAttempt`
/// call with a short caller timeout (times out, and the server counts exactly one request) is what
/// actually discriminates PRODUCTION client selection: no subclass stands between the test and
/// `S3ObjectStorage`'s own verb implementations.
class DelayedResponseServer
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<DelayedResponseRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    std::atomic<size_t> requests_seen{0};

public:
    DelayedResponseServer(std::chrono::milliseconds delay, std::function<void(Poco::Net::HTTPServerResponse &)> respond)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(0))
        , handler_factory(new DelayedResponseRequestHandlerFactory(requests_seen, delay, std::move(respond)))
        , server_params(new Poco::Net::HTTPServerParams())
        , server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    std::string getUrl() const { return "http://" + server_socket->address().toString(); }
    size_t requestsSeen() const { return requests_seen.load(); }
    void resetRequestsSeen() { requests_seen = 0; }
};

/// A genuine `S3ObjectStorage` pointed at `endpoint`. `base_request_timeout_ms` is the base client's
/// request AND connect timeout -- comfortably above the server's simulated delay, so a `Default` call
/// succeeds. No SDK-level retry (`RetryStrategy{.max_retries = 0}`,
/// `s3_slow_all_threads_after_retryable_error = false`): a retry would blur "the single-attempt clone
/// made exactly one request" into "the SDK also tried again".
std::shared_ptr<DB::S3ObjectStorage> makeDispatchStorageForTest(const std::string & endpoint, long base_request_timeout_ms)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration cfg = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ false,
        /* s3_slow_all_threads_after_retryable_error = */ false,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ true,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {});
    cfg.endpointOverride = endpoint;
    cfg.connectTimeoutMs = base_request_timeout_ms;
    cfg.requestTimeoutMs = base_request_timeout_ms;
    /// The adaptive-timeout strategy gives the FIRST attempt a much shorter deadline than
    /// `requestTimeoutMs` and only widens it on a later retry -- with SDK retries disabled above, that
    /// first (short) deadline is the only one this client ever gets, which would time out well under
    /// `server_delay` regardless of `requestTimeoutMs`. Off, so `requestTimeoutMs` governs uniformly.
    cfg.s3_use_adaptive_timeouts = false;
    /// Each `{ }` block below creates and destroys its OWN ephemeral-port server; the default 30 s
    /// keep-alive would let the client pool a persistent connection that can outlive it. If a LATER
    /// block's server happens to be assigned that same now-free port (routine under many back-to-back
    /// server creations within one process), the pooled connection is reused against an unrelated dead
    /// peer and the request fails with "Connection reset by peer" -- reproduced empirically by running
    /// this file's dispatch tests together under `--gtest_repeat`. Disabling keep-alive forces a fresh
    /// connection per request, which is what a short-lived test server should get anyway.
    cfg.http_keep_alive_timeout = 0;
    auto client = DB::S3::ClientFactory::instance().create(
        cfg, clientSettingsForTest(), "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{});
    return std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(),
        DB::S3::URI(endpoint + "/test-bucket/"), DB::S3Capabilities{},
        DB::ObjectStorageKeyGeneratorPtr{}, "disk");
}

DB::ContextPtr contextForTest()
{
    return getContext().context;
}

}

/// Test 6c of the spec: the clone's connect cap is the MIN of the base client's own connect timeout
/// and the requested cap, a configured-zero base is treated as unbounded (never "no limit"), the cache
/// key is the (request timeout, cap) pair, and a reloaded base client cannot widen a clone rebuilt for
/// the same cap.
TEST(S3SingleAttemptClient, ConnectTimeoutIsCappedAndFrozen)
{
    auto storage = makeStorageForTest(20000);
    auto clone = storage->getSingleAttemptClient(/*request_timeout_ms=*/5000, /*connect_timeout_cap_ms=*/5000);
    EXPECT_EQ(clone->getClientConfiguration().connectTimeoutMs, 5000);
    EXPECT_EQ(clone->getClientConfiguration().requestTimeoutMs, 5000);

    auto narrow = makeStorageForTest(1000);
    EXPECT_EQ(narrow->getSingleAttemptClient(5000, 5000)->getClientConfiguration().connectTimeoutMs, 1000);
    /// A base of 0 means unbounded to Poco: it resolves to the cap, never to "no limit".
    EXPECT_EQ(makeStorageForTest(0)->getSingleAttemptClient(5000, 1000)->getClientConfiguration().connectTimeoutMs, 1000);
    /// Two caps under one request timeout are two clones: the cache key is the pair.
    EXPECT_NE(narrow->getSingleAttemptClient(5000, 1000).get(), narrow->getSingleAttemptClient(5000, 500).get());

    /// The reload path replaces the base client with a wider connect timeout, through the real
    /// `applyNewSettings` config-reload path (as `SYSTEM RELOAD CONFIG` would drive it); a clone
    /// rebuilt for the frozen cap 1000 stays at 1000.
    auto reloaded = makeStorageForTest(1000);
    (void)reloaded->getSingleAttemptClient(5000, 1000);
    reloaded->applyNewSettings(*configWithConnectTimeout(5000), "disk", contextForTest(),
                               DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    EXPECT_EQ(reloaded->getSingleAttemptClient(5000, 1000)->getClientConfiguration().connectTimeoutMs, 1000);
}

/// `shutdown()` used to call `DisableRequestProcessing` only on the main client, leaving every cached
/// single-attempt clone (`getSingleAttemptClient`) at its default enabled state. This test verifies only
/// that the flag now propagates to every clone and is restored by `startup()` -- it does NOT prove a
/// disabled clone rejects or interrupts a request: `DisableRequestProcessing` cannot prevent a request's
/// initial dispatch or interrupt one in flight, and a clone that is not currently retrying (every clone
/// here runs `SingleAttemptRetryStrategy`, which never retries) never has an occasion to consult it at
/// all. See the comment on `S3ObjectStorage::shutdown()` for what actually stops a new request after
/// shutdown (CAS engine admission, on a different plane).
TEST(S3SingleAttemptClient, ShutdownDisablesRequestProcessingOnCachedAndFutureClones)
{
    auto storage = makeStorageForTest(20000);
    auto clone = storage->getSingleAttemptClient(/*request_timeout_ms=*/5000, /*connect_timeout_cap_ms=*/5000);
    ASSERT_TRUE(clone->GetHttpClient()->IsRequestProcessingEnabled());

    storage->shutdown();
    EXPECT_FALSE(clone->GetHttpClient()->IsRequestProcessingEnabled())
        << "a clone cached before shutdown() ran must have the flag propagated to it";

    /// A clone for a (timeout, cap) pair never requested before, built WHILE shutdown is in effect, must
    /// come into being with the flag already set -- not just the ones that existed when shutdown() ran.
    auto clone_after_shutdown = storage->getSingleAttemptClient(/*request_timeout_ms=*/6000, /*connect_timeout_cap_ms=*/6000);
    EXPECT_FALSE(clone_after_shutdown->GetHttpClient()->IsRequestProcessingEnabled())
        << "a clone built after shutdown() started must come into being with the flag already set too";

    storage->startup();
    EXPECT_TRUE(clone->GetHttpClient()->IsRequestProcessingEnabled());
    EXPECT_TRUE(clone_after_shutdown->GetHttpClient()->IsRequestProcessingEnabled());

    /// The ordinary case: a clone built with no shutdown in effect is enabled from the start.
    auto clone_after_startup = storage->getSingleAttemptClient(/*request_timeout_ms=*/7000, /*connect_timeout_cap_ms=*/7000);
    EXPECT_TRUE(clone_after_startup->GetHttpClient()->IsRequestProcessingEnabled());
}

/// The freeze computation `openPoolView` uses to build `pool_config.cas_request_budget.connect_timeout_cap_ms`,
/// isolated from any particular verb: the cap is the MIN of the base client's own connect timeout and
/// the attempt timeout, a configured-zero base normalizes to the attempt timeout itself (never "no
/// limit"), and the resulting envelope arithmetic matches `CasRequestBudget::attemptEnvelopeMs`.
TEST(CASEnvelopeWiring, FreezeConnectTimeoutCapSnapshot)
{
    /// A base client with connectTimeoutMs = 1000 and cas_attempt_timeout_ms = 5000: the narrower of
    /// the two wins, and the envelope is attempt + 2 * cap = 7000.
    auto storage = makeStorageForTest(1000);
    const auto cap = DB::ContentAddressedMetadataStorage::freezeConnectTimeoutCapMs(storage, /*cas_attempt_timeout_ms=*/5000);
    ASSERT_TRUE(cap.has_value());
    EXPECT_EQ(*cap, 1000u);
    DB::Cas::CasRequestBudget budget{.attempt_timeout_ms = 5000, .connect_timeout_cap_ms = cap};
    EXPECT_EQ(budget.attemptEnvelopeMs(), 7000u);

    /// A base client with connectTimeoutMs = 0 (Poco "unbounded") and a TTL wide enough for the
    /// resulting envelope (60000, per the spec's test 6e): the cap normalizes to the attempt timeout
    /// itself, never to "no limit" -- a snapshot computing `min(0, attempt)` would report 0 here.
    auto unbounded_storage = makeStorageForTest(0);
    const auto wide_cap = DB::ContentAddressedMetadataStorage::freezeConnectTimeoutCapMs(unbounded_storage, /*cas_attempt_timeout_ms=*/5000);
    ASSERT_TRUE(wide_cap.has_value());
    EXPECT_EQ(*wide_cap, 5000u);
    DB::Cas::CasRequestBudget wide_budget{.attempt_timeout_ms = 5000, .connect_timeout_cap_ms = wide_cap};
    EXPECT_EQ(wide_budget.attemptEnvelopeMs(), 15000u);
    EXPECT_NO_THROW(DB::Cas::validateCasRequestBudget(wide_budget, /*mount_lease_ttl_ms=*/60000, /*mount_renew_period_ms=*/10000,
                                                      /*background_renewal=*/false));

    /// A storage with no S3 client (not exercised here -- every storage above is S3) freezes `nullopt`;
    /// covered directly by `S3ObjectStorage::tryGetS3StorageClient` returning null for a non-S3 storage
    /// and `freezeConnectTimeoutCapMs` short-circuiting on it.
}

/// Every public verb whose retry profile is selectable is proven here to reach the client
/// `S3ObjectStorage::clientForRetryProfile` (private) actually picks for it -- through the storage's OWN
/// verb implementations, never a subclass override standing in for them. Per verb: a `Default` call
/// against a server that answers after `server_delay` succeeds (its client keeps the wide base timeout);
/// the SAME call under `SingleAttempt` with a caller timeout well under `server_delay` times out, and
/// the server counts exactly one request -- proving both that the short-timeout single-attempt clone
/// was selected (not the base client) and that its `SingleAttemptRetryStrategy` performs no
/// SDK-transparent retry.
TEST(CASEnvelopeWiring, ProductionDispatchSelectsTheFrozenSingleAttemptClientPerVerb)
{
    (void)contextForTest(); // getThreadPoolWriter/BlobStorageLogWriter::create fall back to the global context

    constexpr auto server_delay = std::chrono::milliseconds(1000);
    constexpr long base_request_timeout_ms = 10000;
    constexpr uint64_t single_attempt_timeout_ms = 100;

    auto singleAttemptRequest = []
    {
        return DB::ObjectStorageControlRequest{
            .profile = DB::ObjectStorageRetryProfile::SingleAttempt,
            .attempt_timeout_ms = single_attempt_timeout_ms,
            .connect_timeout_cap_ms = single_attempt_timeout_ms};
    };

    /// PUT: writeObject; the profile rides on WriteSettings, not an ObjectStorageControlRequest.
    {
        DelayedResponseServer server(server_delay, [](Poco::Net::HTTPServerResponse & response)
        {
            response.set("ETag", "\"put-etag\"");
            response.setContentLength(0);
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            response.send();
        });
        auto storage = makeDispatchStorageForTest(server.getUrl(), base_request_timeout_ms);

        auto put = [&](DB::ObjectStorageRetryProfile profile, uint64_t timeout_ms)
        {
            DB::WriteSettings write_settings;
            write_settings.object_storage_retry_profile = profile;
            write_settings.object_storage_attempt_timeout_ms = timeout_ms;
            write_settings.object_storage_connect_timeout_cap_ms = timeout_ms;
            auto buffer = storage->writeObject(
                DB::StoredObject("put-key"), DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, write_settings);
            buffer->write('A');
            buffer->finalize();
        };

        EXPECT_NO_THROW(put(DB::ObjectStorageRetryProfile::Default, 0));
        EXPECT_EQ(server.requestsSeen(), 1u);

        server.resetRequestsSeen();
        EXPECT_THROW(put(DB::ObjectStorageRetryProfile::SingleAttempt, single_attempt_timeout_ms), DB::Exception);
        EXPECT_EQ(server.requestsSeen(), 1u);
    }

    /// HEAD: tryGetObjectMetadataWithNativeToken's ObjectStorageControlRequest-taking overload.
    {
        DelayedResponseServer server(server_delay, [](Poco::Net::HTTPServerResponse & response)
        {
            response.set("ETag", "\"head-etag\"");
            response.setContentLength(5);
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            response.send();
        });
        auto storage = makeDispatchStorageForTest(server.getUrl(), base_request_timeout_ms);

        EXPECT_TRUE(storage->tryGetObjectMetadataWithNativeToken(
            "head-key", /*with_tags=*/false, DB::ObjectStorageControlRequest{}).has_value());
        EXPECT_EQ(server.requestsSeen(), 1u);

        server.resetRequestsSeen();
        EXPECT_THROW(
            storage->tryGetObjectMetadataWithNativeToken("head-key", /*with_tags=*/false, singleAttemptRequest()),
            DB::Exception);
        EXPECT_EQ(server.requestsSeen(), 1u);
    }

    /// Conditional DELETE: removeObjectIfTokenMatches's ObjectStorageControlRequest-taking overload.
    {
        DelayedResponseServer server(server_delay, [](Poco::Net::HTTPServerResponse & response)
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_NO_CONTENT);
            response.setContentLength(0);
            response.send();
        });
        auto storage = makeDispatchStorageForTest(server.getUrl(), base_request_timeout_ms);

        auto result = storage->removeObjectIfTokenMatches(
            DB::StoredObject("delete-key"), "\"etag\"", DB::ObjectStorageControlRequest{});
        EXPECT_EQ(result.outcome, DB::ConditionalRemoveOutcome::Removed);
        EXPECT_EQ(server.requestsSeen(), 1u);

        server.resetRequestsSeen();
        EXPECT_THROW(
            storage->removeObjectIfTokenMatches(DB::StoredObject("delete-key"), "\"etag\"", singleAttemptRequest()),
            DB::Exception);
        EXPECT_EQ(server.requestsSeen(), 1u);
    }

    /// Bulk DELETE: removeObjectsIfExistUnderProfile (one DeleteObjects request for the whole batch).
    {
        DelayedResponseServer server(server_delay, [](Poco::Net::HTTPServerResponse & response)
        {
            static const std::string body =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></DeleteResult>";
            response.setContentType("application/xml");
            response.setContentLength(body.size());
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            auto & out = response.send();
            out << body;
            out.flush();
        });
        auto storage = makeDispatchStorageForTest(server.getUrl(), base_request_timeout_ms);

        EXPECT_NO_THROW(storage->removeObjectsIfExistUnderProfile(
            {DB::StoredObject("bulk-delete-key")}, DB::ObjectStorageControlRequest{}));
        EXPECT_EQ(server.requestsSeen(), 1u);

        server.resetRequestsSeen();
        EXPECT_THROW(
            storage->removeObjectsIfExistUnderProfile({DB::StoredObject("bulk-delete-key")}, singleAttemptRequest()),
            DB::Exception);
        EXPECT_EQ(server.requestsSeen(), 1u);
    }

    /// LIST: iterate's ObjectStorageControlRequest-taking overload. The ListObjectsV2 call happens
    /// lazily, on the async iterator's first `isValid()`.
    {
        DelayedResponseServer server(server_delay, [](Poco::Net::HTTPServerResponse & response)
        {
            static const std::string body =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                "<Name>test-bucket</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>"
                "<IsTruncated>false</IsTruncated></ListBucketResult>";
            response.setContentType("application/xml");
            response.setContentLength(body.size());
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            auto & out = response.send();
            out << body;
            out.flush();
        });
        auto storage = makeDispatchStorageForTest(server.getUrl(), base_request_timeout_ms);

        auto default_iterator = storage->iterate("p/", /*max_keys=*/10, /*with_tags=*/false, {}, DB::ObjectStorageControlRequest{});
        EXPECT_NO_THROW(default_iterator->isValid());
        EXPECT_EQ(server.requestsSeen(), 1u);

        server.resetRequestsSeen();
        auto single_attempt_iterator = storage->iterate("p/", /*max_keys=*/10, /*with_tags=*/false, {}, singleAttemptRequest());
        EXPECT_THROW(single_attempt_iterator->isValid(), DB::Exception);
        EXPECT_EQ(server.requestsSeen(), 1u);
    }

    /// GET: readObject; the profile rides on ReadSettings. The request happens lazily, on the buffer's
    /// first read.
    {
        DelayedResponseServer server(server_delay, [](Poco::Net::HTTPServerResponse & response)
        {
            static const std::string body = "hello";
            response.set("ETag", "\"get-etag\"");
            response.setContentType("binary/octet-stream");
            response.setContentLength(body.size());
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            auto & out = response.send();
            out << body;
            out.flush();
        });
        auto storage = makeDispatchStorageForTest(server.getUrl(), base_request_timeout_ms);

        auto get = [&](DB::ObjectStorageRetryProfile profile, uint64_t timeout_ms)
        {
            DB::ReadSettings read_settings;
            read_settings.object_storage_retry_profile = profile;
            read_settings.object_storage_attempt_timeout_ms = timeout_ms;
            read_settings.object_storage_connect_timeout_cap_ms = timeout_ms;
            auto buffer = storage->readObject(DB::StoredObject("get-key"), read_settings);
            std::string content;
            DB::readStringUntilEOF(content, *buffer);
            return content;
        };

        EXPECT_EQ(get(DB::ObjectStorageRetryProfile::Default, 0), "hello");
        EXPECT_EQ(server.requestsSeen(), 1u);

        server.resetRequestsSeen();
        EXPECT_THROW(get(DB::ObjectStorageRetryProfile::SingleAttempt, single_attempt_timeout_ms), DB::Exception);
        EXPECT_EQ(server.requestsSeen(), 1u);
    }
}

/// The test above proves production dispatch selects a short-REQUEST-timeout clone, but every server
/// there answers every request -- it can never tell whether the frozen `connect_timeout_cap_ms` reaches
/// the CONNECTION phase at all, only whether SOME clone with a short deadline was picked. This test
/// closes that gap WITHOUT any wall-clock measurement or stalled connect: every call below goes through
/// production dispatch against an ordinary, immediately-answering server, so it can only prove two
/// clock-free facts. First, that dispatch built (or reused) the single-attempt clone under EXACTLY the
/// (attempt timeout, connect cap) key the request carried -- `hasSingleAttemptClientForTest` only
/// inspects `S3ObjectStorage`'s clone cache, it never creates an entry, so a wrong key or a missing clone
/// fails the assertion immediately rather than timing out. Second, that the clone found under that key
/// actually carries the cap as its `connectTimeoutMs`, while the Default profile's own client keeps the
/// disk's (wider) base connect timeout untouched. Whether Poco's HTTP client actually enforces
/// `connectTimeoutMs` at the socket level is `PocoHTTPClient`/`Poco::Net::HTTPClientSession` behaviour
/// upstream of this class, and is not re-proved here; `S3SingleAttemptClient.ConnectTimeoutIsCappedAndFrozen`
/// above pins the MIN/cache-key arithmetic `getSingleAttemptClient` applies in isolation.
TEST(CASEnvelopeWiring, ProductionDispatchAppliesTheFrozenConnectCapAtConnectTime)
{
    (void)contextForTest(); // getThreadPoolWriter/BlobStorageLogWriter::create fall back to the global context

    constexpr long base_connect_timeout_ms = 2000;
    constexpr uint64_t single_attempt_timeout_ms = 5000;
    constexpr uint64_t single_attempt_connect_cap_ms = 100;

    /// PUT: writeObject; the profile and cap ride on WriteSettings, not an ObjectStorageControlRequest.
    {
        DelayedResponseServer server(std::chrono::milliseconds(0), [](Poco::Net::HTTPServerResponse & response)
        {
            response.set("ETag", "\"put-etag\"");
            response.setContentLength(0);
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            response.send();
        });
        auto storage = makeDispatchStorageForTest(server.getUrl(), base_connect_timeout_ms);

        auto put = [&](DB::ObjectStorageRetryProfile profile, uint64_t attempt_timeout_ms, uint64_t connect_cap_ms)
        {
            DB::WriteSettings write_settings;
            write_settings.object_storage_retry_profile = profile;
            write_settings.object_storage_attempt_timeout_ms = attempt_timeout_ms;
            write_settings.object_storage_connect_timeout_cap_ms = connect_cap_ms;
            auto buffer = storage->writeObject(
                DB::StoredObject("put-key"), DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, write_settings);
            buffer->write('A');
            buffer->finalize();
        };

        EXPECT_NO_THROW(put(DB::ObjectStorageRetryProfile::Default, 0, 0));
        EXPECT_EQ(storage->getS3StorageClient()->getClientConfiguration().connectTimeoutMs, base_connect_timeout_ms)
            << "the Default profile must dispatch on the disk's own client, unchanged";

        EXPECT_NO_THROW(put(DB::ObjectStorageRetryProfile::SingleAttempt, single_attempt_timeout_ms, single_attempt_connect_cap_ms));
        ASSERT_TRUE(storage->hasSingleAttemptClientForTest(single_attempt_timeout_ms, single_attempt_connect_cap_ms))
            << "dispatch must have built the single-attempt clone under exactly this (attempt timeout, cap) key";
        EXPECT_FALSE(storage->hasSingleAttemptClientForTest(single_attempt_timeout_ms, 0))
            << "dispatch must not fall back to an uncapped clone for this attempt timeout";
        EXPECT_EQ(
            storage->getSingleAttemptClient(single_attempt_timeout_ms, single_attempt_connect_cap_ms)
                ->getClientConfiguration().connectTimeoutMs,
            static_cast<long>(single_attempt_connect_cap_ms));
    }

    /// HEAD: tryGetObjectMetadataWithNativeToken's ObjectStorageControlRequest-taking overload.
    {
        DelayedResponseServer server(std::chrono::milliseconds(0), [](Poco::Net::HTTPServerResponse & response)
        {
            response.set("ETag", "\"head-etag\"");
            response.setContentLength(5);
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            response.send();
        });
        auto storage = makeDispatchStorageForTest(server.getUrl(), base_connect_timeout_ms);

        EXPECT_TRUE(storage->tryGetObjectMetadataWithNativeToken(
            "head-key", /*with_tags=*/false, DB::ObjectStorageControlRequest{}).has_value());
        EXPECT_EQ(storage->getS3StorageClient()->getClientConfiguration().connectTimeoutMs, base_connect_timeout_ms)
            << "the Default profile must dispatch on the disk's own client, unchanged";

        EXPECT_TRUE(storage->tryGetObjectMetadataWithNativeToken(
            "head-key", /*with_tags=*/false,
            DB::ObjectStorageControlRequest{
                .profile = DB::ObjectStorageRetryProfile::SingleAttempt,
                .attempt_timeout_ms = single_attempt_timeout_ms,
                .connect_timeout_cap_ms = single_attempt_connect_cap_ms}).has_value());
        ASSERT_TRUE(storage->hasSingleAttemptClientForTest(single_attempt_timeout_ms, single_attempt_connect_cap_ms))
            << "dispatch must have built the single-attempt clone under exactly this (attempt timeout, cap) key";
        EXPECT_FALSE(storage->hasSingleAttemptClientForTest(single_attempt_timeout_ms, 0))
            << "dispatch must not fall back to an uncapped clone for this attempt timeout";
        EXPECT_EQ(
            storage->getSingleAttemptClient(single_attempt_timeout_ms, single_attempt_connect_cap_ms)
                ->getClientConfiguration().connectTimeoutMs,
            static_cast<long>(single_attempt_connect_cap_ms));
    }

    /// Conditional DELETE: removeObjectIfTokenMatches's ObjectStorageControlRequest-taking overload.
    {
        DelayedResponseServer server(std::chrono::milliseconds(0), [](Poco::Net::HTTPServerResponse & response)
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_NO_CONTENT);
            response.setContentLength(0);
            response.send();
        });
        auto storage = makeDispatchStorageForTest(server.getUrl(), base_connect_timeout_ms);

        const auto default_result = storage->removeObjectIfTokenMatches(
            DB::StoredObject("delete-key"), "\"etag\"", DB::ObjectStorageControlRequest{});
        EXPECT_EQ(default_result.outcome, DB::ConditionalRemoveOutcome::Removed);
        EXPECT_EQ(storage->getS3StorageClient()->getClientConfiguration().connectTimeoutMs, base_connect_timeout_ms)
            << "the Default profile must dispatch on the disk's own client, unchanged";

        const auto capped_result = storage->removeObjectIfTokenMatches(
            DB::StoredObject("delete-key"), "\"etag\"",
            DB::ObjectStorageControlRequest{
                .profile = DB::ObjectStorageRetryProfile::SingleAttempt,
                .attempt_timeout_ms = single_attempt_timeout_ms,
                .connect_timeout_cap_ms = single_attempt_connect_cap_ms});
        EXPECT_EQ(capped_result.outcome, DB::ConditionalRemoveOutcome::Removed);
        ASSERT_TRUE(storage->hasSingleAttemptClientForTest(single_attempt_timeout_ms, single_attempt_connect_cap_ms))
            << "dispatch must have built the single-attempt clone under exactly this (attempt timeout, cap) key";
        EXPECT_FALSE(storage->hasSingleAttemptClientForTest(single_attempt_timeout_ms, 0))
            << "dispatch must not fall back to an uncapped clone for this attempt timeout";
        EXPECT_EQ(
            storage->getSingleAttemptClient(single_attempt_timeout_ms, single_attempt_connect_cap_ms)
                ->getClientConfiguration().connectTimeoutMs,
            static_cast<long>(single_attempt_connect_cap_ms));
    }
}

/// `FreezeConnectTimeoutCapSnapshot` above pins the ARITHMETIC of `freezeConnectTimeoutCapMs` in
/// isolation; `ProductionDispatchAppliesTheFrozenConnectCapAtConnectTime` pins that a cap handed
/// DIRECTLY to `S3ObjectStorage` reaches the connect phase. Neither proves the composition
/// `ContentAddressedMetadataStorage::openPoolView` actually performs: freezing the cap from a real S3
/// client (`ContentAddressedMetadataStorage.cpp` ~802) and handing it into `Cas::ObjectStorageBackend`'s
/// constructor (the backend handoff at ~812-822) exactly as a writable Native mount does. This test
/// drives that whole chain end to end -- real client -> freezeConnectTimeoutCapMs -> ObjectStorageBackend
/// -> CasRequests/CasOperation -> the SAME production S3ObjectStorage dispatch the tests above cover --
/// with no recording subclass anywhere in it, and, like the test above, with no wall-clock measurement:
/// both backends' HEAD goes through an ordinary, immediately-answering server.
///
/// A read-only backend (`single_attempt_control_plane_ = false`, matching `openPoolView`'s own choice for
/// a read-only mount) dispatches its read-class requests under the Default profile -- proven here by the
/// storage never having built ANY single-attempt clone afterward, i.e. it used the disk's own client
/// untouched. The SAME derived cap and attempt timeout, handed to a WRITABLE Native backend exactly as
/// `openPoolView` constructs one, must then dispatch under EXACTLY that (attempt timeout, cap) key, and
/// the clone found under that key must carry the cap as its `connectTimeoutMs`: a dropped or corrupted
/// handoff anywhere in the chain would either leave no clone under that key or leave one with the wrong
/// timeout, and either way the assertion below fails immediately rather than by timing out.
TEST(CASEnvelopeWiring, FreezeConnectTimeoutCapReachesTheBackendOverProductionDispatch)
{
    (void)contextForTest();

    constexpr long base_connect_timeout_ms = 2000;
    constexpr uint64_t cas_attempt_timeout_ms = 100;

    DelayedResponseServer server(std::chrono::milliseconds(0), [](Poco::Net::HTTPServerResponse & response)
    {
        response.set("ETag", "\"head-etag\"");
        response.setContentLength(5);
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.send();
    });
    auto storage = makeDispatchStorageForTest(server.getUrl(), base_connect_timeout_ms);

    /// The exact derivation `ContentAddressedMetadataStorage::openPoolView` uses: min(base connect
    /// timeout, attempt timeout) = 100 here, never the wide 2000 ms base timeout.
    const auto cap = DB::ContentAddressedMetadataStorage::freezeConnectTimeoutCapMs(storage, cas_attempt_timeout_ms);
    ASSERT_TRUE(cap.has_value());
    EXPECT_EQ(*cap, cas_attempt_timeout_ms);

    auto uncapped_backend = std::make_shared<DB::Cas::ObjectStorageBackend>(
        storage, DB::Cas::ObjectStorageBackend::Mode::Native,
        /*single_attempt_control_plane_=*/false, /*attempt_timeout_ms_=*/0, /*connect_timeout_cap_ms_=*/0);
    {
        DB::Cas::CasRequests requests(DB::Cas::BackendPtr(uncapped_backend), DB::Cas::Fence::open());
        auto op = requests.admit();
        EXPECT_TRUE(op.head("k", DB::Cas::Retry::once()).has_value());
        EXPECT_FALSE(storage->hasSingleAttemptClientForTest(0, 0))
            << "a read-only (Default-profile) backend must never build a single-attempt clone";
    }

    /// The derived cap, handed to the backend exactly as `openPoolView` constructs it (:812-822) for a
    /// WRITABLE Native mount.
    auto capped_backend = std::make_shared<DB::Cas::ObjectStorageBackend>(
        storage, DB::Cas::ObjectStorageBackend::Mode::Native,
        /*single_attempt_control_plane_=*/true, cas_attempt_timeout_ms, *cap);
    /// Cheap, deterministic corroboration alongside the dispatch-level assertions below: it proves the
    /// constructor argument was stored, not that it reached the S3 client's actual connect timeout, which
    /// only `hasSingleAttemptClientForTest`/`getSingleAttemptClient` below can show.
    EXPECT_EQ(capped_backend->connectTimeoutCapMs(), *cap);
    {
        DB::Cas::CasRequests requests(DB::Cas::BackendPtr(capped_backend), DB::Cas::Fence::open());
        auto op = requests.admit();
        EXPECT_TRUE(op.head("k", DB::Cas::Retry::once()).has_value());
        ASSERT_TRUE(storage->hasSingleAttemptClientForTest(cas_attempt_timeout_ms, *cap))
            << "the WRITABLE backend must dispatch its read-class requests under exactly the frozen "
               "(attempt timeout, cap) key";
        EXPECT_EQ(
            storage->getSingleAttemptClient(cas_attempt_timeout_ms, *cap)->getClientConfiguration().connectTimeoutMs,
            static_cast<long>(*cap))
            << "socket-level enforcement of connectTimeoutMs is PocoHTTPClient behaviour upstream of this "
               "class, not re-proved here";
    }
}

#endif
