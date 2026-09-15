#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <unistd.h>

#include "config.h"

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>
#include <IO/S3Common.h>
#include <Common/tests/gtest_global_context.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>

#include <Poco/Net/HTTPBasicStreamBuf.h>

#include <cstring>
#include <functional>
#include <istream>
#include <mutex>
#include <vector>
#endif

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
#if USE_AWS_S3
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NETWORK_ERROR;
#endif
}

namespace
{

/// Same unique-temp-root convention as the other CAS unit tests, so parallel runs never share a root.
std::shared_ptr<DB::LocalObjectStorage> makeLocalObjectStorageForRetryProfileTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_unit_upstream_slice_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    return std::make_shared<DB::LocalObjectStorage>(DB::LocalObjectStorageSettings("test", root, /*read_only_=*/false));
}

/// Every refusal below is NOT_IMPLEMENTED, and so is the pre-existing refusal of conditional removal,
/// so the code alone cannot tell which one fired. Match a phrase unique to the intended message too.
template <typename F>
void expectThrowsNotImplementedSaying(const std::string & needle, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception saying '" << needle << "'";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NOT_IMPLEMENTED);
        EXPECT_NE(e.message().find(needle), std::string::npos) << "actual message: " << e.message();
    }
}

}

/// The base `IObjectStorage` bodies forward `Default` and refuse `SingleAttempt`: a caller that asked
/// for one attempt has its own deadline, and a transparently retried request would outlive it.
TEST(CASUpstreamSlice, HeadListRemoveOverloadsRefuseSingleAttemptOnTheBaseStorage)
{
    auto local = makeLocalObjectStorageForRetryProfileTest();

    const DB::ObjectStorageControlRequest single_attempt{.profile = DB::ObjectStorageRetryProfile::SingleAttempt};
    const DB::ObjectStorageControlRequest default_profile{.profile = DB::ObjectStorageRetryProfile::Default};

    expectThrowsNotImplementedSaying(
        "single-attempt metadata requests",
        [&] { local->tryGetObjectMetadataWithNativeToken("k", false, single_attempt); });
    expectThrowsNotImplementedSaying(
        "single-attempt listing requests",
        [&] { local->iterate("", 1, false, {}, single_attempt); });
    expectThrowsNotImplementedSaying(
        "single-attempt removal requests",
        [&] { local->removeObjectIfTokenMatches(DB::StoredObject("k"), "e", single_attempt); });

    /// `Default` must keep reaching the ordinary implementation. For removal that is still a refusal,
    /// but the pre-existing one — matching its wording proves the profile overload forwarded.
    EXPECT_NO_THROW(local->tryGetObjectMetadataWithNativeToken("k", false, default_profile));
    EXPECT_NO_THROW(local->iterate("", 1, false, {}, default_profile));
    expectThrowsNotImplementedSaying(
        "Conditional (token-exact) object removal",
        [&] { local->removeObjectIfTokenMatches(DB::StoredObject("k"), "e", default_profile); });
}

#if USE_AWS_S3

namespace
{

/// One scripted answer to a `GetObject`. `fail_mid_body` makes the response stream throw after it has
/// already delivered bytes, which is what drives `ReadBufferFromS3` to reissue the request.
struct ScriptedGetObjectStep
{
    bool ok = true;
    Aws::S3::S3Errors error = Aws::S3::S3Errors::SLOW_DOWN;
    std::string exception_name;
    std::string etag;
    std::string body;
    bool fail_mid_body = false;
};

ScriptedGetObjectStep okStep(const std::string & etag, const std::string & body, bool fail_mid_body = false)
{
    return ScriptedGetObjectStep{
        .ok = true,
        .error = Aws::S3::S3Errors::SLOW_DOWN,
        .exception_name = "",
        .etag = etag,
        .body = body,
        .fail_mid_body = fail_mid_body};
}

ScriptedGetObjectStep throttleStep()
{
    return ScriptedGetObjectStep{
        .ok = false,
        .error = Aws::S3::S3Errors::SLOW_DOWN,
        .exception_name = "SlowDown",
        .etag = "",
        .body = "",
        .fail_mid_body = false};
}

/// `S3Exception::isAccessTokenExpiredError` keys on the error CODE, not the name.
ScriptedGetObjectStep expiredTokenStep()
{
    return ScriptedGetObjectStep{
        .ok = false,
        .error = Aws::S3::S3Errors::ACCESS_DENIED,
        .exception_name = "ExpiredToken",
        .etag = "",
        .body = "",
        .fail_mid_body = false};
}

/// One scripted answer to a control-plane request (HEAD or conditional DELETE). `retryable` is what
/// the SDK's retry strategy consults, so it is what decides whether the client's own attempt loop
/// reissues the request — which is how a single-attempt clone is told apart from the disk client.
struct ScriptedControlStep
{
    bool ok = true;
    Aws::S3::S3Errors error = Aws::S3::S3Errors::SLOW_DOWN;
    std::string exception_name;
    bool retryable = false;
};

ScriptedControlStep controlOk()
{
    return ScriptedControlStep{.ok = true, .error = Aws::S3::S3Errors::SLOW_DOWN, .exception_name = "", .retryable = false};
}

ScriptedControlStep controlExpiredToken()
{
    return ScriptedControlStep{
        .ok = false, .error = Aws::S3::S3Errors::ACCESS_DENIED, .exception_name = "ExpiredToken", .retryable = false};
}

ScriptedControlStep controlThrottle()
{
    return ScriptedControlStep{
        .ok = false, .error = Aws::S3::S3Errors::SLOW_DOWN, .exception_name = "SlowDown", .retryable = true};
}

/// `ReadBufferFromIStream` reads through `Poco::Net::HTTPBasicStreamBuf::readFromDevice`, so a fake
/// response body has to be one of those rather than a plain `std::stringstream`.
class ScriptedBodyStreamBuf : public Poco::Net::HTTPBasicStreamBuf
{
public:
    ScriptedBodyStreamBuf(std::string body_, bool fail_mid_body_)
        : Poco::Net::HTTPBasicStreamBuf(256, std::ios::in), body(std::move(body_)), fail_mid_body(fail_mid_body_)
    {
    }

private:
    int readFromDevice(char * buffer, std::streamsize length) override
    {
        if (fail_mid_body && position > 0)
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "scripted failure part-way through the response body");

        const size_t available = body.size() - position;
        const size_t n = std::min(static_cast<size_t>(length), available);
        std::memcpy(buffer, body.data() + position, n);
        position += n;
        return static_cast<int>(n);
    }

    const std::string body;
    const bool fail_mid_body;
    size_t position = 0;
};

class ScriptedBodyStreamHolder
{
protected:
    ScriptedBodyStreamHolder(std::string body, bool fail_mid_body) : buf(std::move(body), fail_mid_body) { }
    ScriptedBodyStreamBuf buf;
};

/// The holder base is listed first so `buf` is constructed before `std::iostream` is handed its address.
class ScriptedBodyStream : private ScriptedBodyStreamHolder, public std::iostream
{
public:
    ScriptedBodyStream(std::string body, bool fail_mid_body)
        : ScriptedBodyStreamHolder(std::move(body), fail_mid_body), std::iostream(&buf)
    {
    }
};

/// An `S3::Client` whose `GetObject` answers from a script, recording how many times it was called and
/// whether each request carried the native-conditional mark. Clones share the state, so the counters
/// still see the requests issued through the single-attempt clone.
class ScriptedGetObjectClient : public DB::S3::Client
{
private:
    struct State
    {
        std::vector<ScriptedGetObjectStep> script;
        std::vector<ScriptedControlStep> head_script;
        std::vector<ScriptedControlStep> delete_script;

        size_t get_object_calls = 0;
        std::vector<bool> native_conditional_marks;

        /// The `requestTimeoutMs` of the client each request was actually issued through, which is
        /// what proves a request rode the clone built for the bound its caller asked for.
        std::vector<long> head_request_timeouts_ms;
        std::vector<long> delete_request_timeouts_ms;
        /// Every configuration this client was asked to clone with — a request-free way to see which
        /// client a verb selected.
        std::vector<long> clone_request_timeouts_ms;

        std::mutex mutex;
    };

    const std::shared_ptr<State> state;

public:
    ScriptedGetObjectClient() : ScriptedGetObjectClient(std::make_shared<State>(), GetClientConfiguration()) { }

    static DB::S3::PocoHTTPClientConfiguration GetClientConfiguration()
    {
        DB::RemoteHostFilter remote_host_filter;
        /// max_retries is deliberately nonzero: it is the disk client's own attempt loop, and the
        /// only thing that distinguishes it from the single-attempt clone. The two slow-down flags
        /// are off so that loop spins without waiting out a real backoff.
        return DB::S3::ClientFactory::instance().createClientConfiguration(
            "some-region",
            remote_host_filter,
            /* s3_max_redirects = */ 100,
            DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 2},
            /* s3_slow_all_threads_after_network_error = */ false,
            /* s3_slow_all_threads_after_retryable_error = */ false,
            /* enable_s3_requests_logging = */ true,
            /* for_disk_s3 = */ false,
            /* opt_disk_name = */ {},
            /* request_throttler = */ {});
    }

    void script(std::vector<ScriptedGetObjectStep> steps) const
    {
        std::lock_guard lock(state->mutex);
        state->script = std::move(steps);
    }

    size_t getObjectCalls() const
    {
        std::lock_guard lock(state->mutex);
        return state->get_object_calls;
    }

    std::vector<bool> nativeConditionalMarks() const
    {
        std::lock_guard lock(state->mutex);
        return state->native_conditional_marks;
    }

    void scriptHead(std::vector<ScriptedControlStep> steps) const
    {
        std::lock_guard lock(state->mutex);
        state->head_script = std::move(steps);
    }

    void scriptDelete(std::vector<ScriptedControlStep> steps) const
    {
        std::lock_guard lock(state->mutex);
        state->delete_script = std::move(steps);
    }

    std::vector<long> headRequestTimeouts() const
    {
        std::lock_guard lock(state->mutex);
        return state->head_request_timeouts_ms;
    }

    std::vector<long> deleteRequestTimeouts() const
    {
        std::lock_guard lock(state->mutex);
        return state->delete_request_timeouts_ms;
    }

    std::vector<long> cloneRequestTimeouts() const
    {
        std::lock_guard lock(state->mutex);
        return state->clone_request_timeouts_ms;
    }

    std::unique_ptr<DB::S3::Client> cloneWithConfigurationOverride(
        const DB::S3::PocoHTTPClientConfiguration & client_configuration_override) const override
    {
        {
            std::lock_guard lock(state->mutex);
            state->clone_request_timeouts_ms.push_back(client_configuration_override.requestTimeoutMs);
        }
        return std::unique_ptr<DB::S3::Client>(new ScriptedGetObjectClient(state, client_configuration_override));
    }

    Aws::S3::Model::GetObjectOutcome GetObject(const Aws::S3::Model::GetObjectRequest & request) const override
    {
        std::lock_guard lock(state->mutex);

        const auto * marked = dynamic_cast<const DB::S3::RequestWithNativeConditionalMode *>(&request);
        state->native_conditional_marks.push_back(marked != nullptr && marked->isNativeConditional());

        const size_t index = state->get_object_calls++;
        if (index >= state->script.size())
        {
            return Aws::S3::Model::GetObjectOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(
                Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey", "the script has no answer for this request", false));
        }

        const auto & step = state->script[index];
        if (!step.ok)
        {
            return Aws::S3::Model::GetObjectOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(
                step.error, step.exception_name, "scripted error", false));
        }

        Aws::S3::Model::GetObjectResult result;
        result.SetETag(step.etag);
        result.SetContentLength(static_cast<long long>(step.body.size()));
        /// The SDK releases the body through `Aws::Delete`, which frees with the SDK's allocator, so
        /// the stream must come from `Aws::New`; a plain `new` here is an alloc-dealloc mismatch.
        result.ReplaceBody(Aws::New<ScriptedBodyStream>("ScriptedBodyStream", step.body, step.fail_mid_body));
        return Aws::S3::Model::GetObjectOutcome(std::move(result));
    }

    Aws::S3::Model::HeadObjectOutcome HeadObject(const Aws::S3::Model::HeadObjectRequest & /*request*/) const override
    {
        std::lock_guard lock(state->mutex);
        state->head_request_timeouts_ms.push_back(getClientConfiguration().requestTimeoutMs);

        const auto step = nextControlStep(state->head_script, state->head_request_timeouts_ms.size());
        if (!step.ok)
            return Aws::S3::Model::HeadObjectOutcome(makeError(step));

        Aws::S3::Model::HeadObjectResult result;
        /// Any nonzero size: tryGetObjectMetadataImpl reads an all-zero HeadObjectResult as a miss.
        result.SetContentLength(scripted_head_object_size);
        result.SetETag(scripted_head_object_etag);
        return Aws::S3::Model::HeadObjectOutcome(std::move(result));
    }

    Aws::S3::Model::DeleteObjectOutcome DeleteObject(const Aws::S3::Model::DeleteObjectRequest & /*request*/) const override
    {
        std::lock_guard lock(state->mutex);
        state->delete_request_timeouts_ms.push_back(getClientConfiguration().requestTimeoutMs);

        const auto step = nextControlStep(state->delete_script, state->delete_request_timeouts_ms.size());
        if (!step.ok)
            return Aws::S3::Model::DeleteObjectOutcome(makeError(step));

        Aws::S3::Model::DeleteObjectResult result;
        result.SetDeleteMarker(false);
        return Aws::S3::Model::DeleteObjectOutcome(std::move(result));
    }

private:
    static constexpr long long scripted_head_object_size = 7;
    static constexpr const char * scripted_head_object_etag = "\"h1\"";

    /// A script shorter than the number of requests keeps answering with its last step, so a test
    /// that means "this error, however many attempts the client makes" says it in one entry.
    static ScriptedControlStep nextControlStep(const std::vector<ScriptedControlStep> & script, size_t call_number)
    {
        if (script.empty())
            return controlOk();
        return script[std::min(call_number - 1, script.size() - 1)];
    }

    static Aws::Client::AWSError<Aws::S3::S3Errors> makeError(const ScriptedControlStep & step)
    {
        return Aws::Client::AWSError<Aws::S3::S3Errors>(step.error, step.exception_name, "scripted error", step.retryable);
    }

    ScriptedGetObjectClient(std::shared_ptr<State> state_, const DB::S3::PocoHTTPClientConfiguration & client_configuration)
        : DB::S3::Client(
            100,
            DB::S3::ServerSideEncryptionKMSConfig(),
            std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("", ""),
            client_configuration,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            DB::S3::ClientSettings{
                .use_virtual_addressing = true,
                .disable_checksum = false,
                .gcs_issue_compose_request = false,
                .is_s3express_bucket = false,
            })
        , state(std::move(state_))
    {
    }
};

std::shared_ptr<DB::S3ObjectStorage> makeScriptedS3ObjectStorage(
    ScriptedGetObjectClient *& out_client,
    DB::S3ObjectStorage::S3CredentialsRefreshCallback credentials_refresh_callback = {})
{
    auto owned_client = std::make_unique<ScriptedGetObjectClient>();
    out_client = owned_client.get();

    DB::S3::URI uri;
    uri.bucket = "cas-upstream-slice-bucket";
    DB::S3Capabilities capabilities;
    DB::ObjectStorageKeyGeneratorPtr key_generator;

    return std::make_shared<DB::S3ObjectStorage>(
        std::move(owned_client),
        std::make_unique<DB::S3Settings>(),
        std::move(uri),
        capabilities,
        key_generator,
        "cas-upstream-slice-disk",
        /*for_disk_s3_=*/true,
        credentials_refresh_callback);
}

template <typename F>
void expectThrowsCodeSaying(int expected_code, const std::string & needle, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception saying '" << needle << "'";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
        EXPECT_NE(e.message().find(needle), std::string::npos) << "actual message: " << e.message();
    }
}

/// Builds the storage over a refresh callback that vends one fresh scripted client, so a test can
/// both script that client up front and assert the storage ended up holding that exact object.
std::shared_ptr<DB::S3ObjectStorage> makeScriptedS3ObjectStorageWithRefresh(
    ScriptedGetObjectClient *& out_client,
    ScriptedGetObjectClient *& out_refreshed,
    std::function<void(const ScriptedGetObjectClient &)> script_refreshed)
{
    return makeScriptedS3ObjectStorage(
        out_client,
        [&out_refreshed, script_refreshed]() -> std::unique_ptr<const DB::S3::Client>
        {
            auto fresh = std::make_unique<ScriptedGetObjectClient>();
            script_refreshed(*fresh);
            out_refreshed = fresh.get();
            return fresh;
        });
}

}

/// A plain `GET` must carry the same native-conditional mark a `HEAD` does when the read asks for it,
/// so that on a generation-token store both answer with the same incarnation identity.
TEST(CASUpstreamSlice, NativeConditionalReadSettingMarksTheGetRequest)
{
    (void)getContext();

    ScriptedGetObjectClient * marked_client = nullptr;
    auto marked_storage = makeScriptedS3ObjectStorage(marked_client);
    marked_client->script({okStep("\"e1\"", "AAAA")});

    DB::ReadSettings marked_settings;
    marked_settings.object_storage_request_mode = DB::ObjectStorageRequestMode::NativeConditional;
    marked_storage->readSmallObjectAndGetObjectMetadata(DB::StoredObject("k"), marked_settings, 1 << 20);

    ASSERT_EQ(marked_client->nativeConditionalMarks().size(), 1u);
    EXPECT_TRUE(marked_client->nativeConditionalMarks().at(0));

    ScriptedGetObjectClient * plain_client = nullptr;
    auto plain_storage = makeScriptedS3ObjectStorage(plain_client);
    plain_client->script({okStep("\"e1\"", "AAAA")});

    plain_storage->readSmallObjectAndGetObjectMetadata(DB::StoredObject("k"), DB::ReadSettings{}, 1 << 20);

    ASSERT_EQ(plain_client->nativeConditionalMarks().size(), 1u);
    EXPECT_FALSE(plain_client->nativeConditionalMarks().at(0));
}

/// The buffer's own retry loop can straddle a replacement of the object: the first response delivers
/// some of the old incarnation's bytes to the consumer before its stream breaks mid-body, and the
/// reissue answers with a different ETag. The bytes handed back are then from neither incarnation
/// alone. `buffer_size` is pinned to 2 so the first fill (of "AAAA"'s 4 bytes) completes and is
/// exposed to the consumer before the second fill hits the scripted mid-body failure - with the
/// default (much larger) buffer, that failure happens inside the very first fill, before any byte of
/// "e1" ever reaches the consumer, which is the "nothing to mix with" case covered below instead.
TEST(CASUpstreamSlice, ReadSmallObjectThrowsWhenAReissueAnswersWithADifferentETag)
{
    (void)getContext();

    ScriptedGetObjectClient * client = nullptr;
    auto storage = makeScriptedS3ObjectStorage(client);
    client->script({okStep("\"e1\"", "AAAA", /*fail_mid_body=*/true), okStep("\"e2\"", "BBBB")});

    DB::ReadSettings read_settings;
    read_settings.object_storage_request_mode = DB::ObjectStorageRequestMode::NativeConditional;
    read_settings.remote_fs_settings.buffer_size = 2;
    /// Default profile here: the buffer's own multi-attempt loop is what straddles the replacement.
    expectThrowsCodeSaying(
        DB::ErrorCodes::CANNOT_READ_ALL_DATA,
        "response identity changed",
        [&] { storage->readSmallObjectAndGetObjectMetadata(DB::StoredObject("k"), read_settings, 1 << 20); });

    EXPECT_EQ(client->getObjectCalls(), 2u);
}

/// Scoped to an identity change that actually mixed bytes: with the default (large) buffer, "e1"'s
/// mid-body failure happens inside its very first fill attempt, before any byte crosses into the
/// consumer's buffer - so the reissue under a different ETag is an ordinary retry of a request that
/// never delivered anything, not a coherence problem, even though the ETag changed.
TEST(CASUpstreamSlice, ReadSmallObjectAcceptsAReissueThatDeliveredNoBytesEvenWithADifferentETag)
{
    (void)getContext();

    ScriptedGetObjectClient * client = nullptr;
    auto storage = makeScriptedS3ObjectStorage(client);
    client->script({okStep("\"e1\"", "AAAA", /*fail_mid_body=*/true), okStep("\"e2\"", "BBBB")});

    const auto result = storage->readSmallObjectAndGetObjectMetadata(DB::StoredObject("k"), DB::ReadSettings{}, 1 << 20);
    EXPECT_EQ(result.data, "BBBB");
    EXPECT_EQ(result.metadata.etag, "\"e2\"");
    EXPECT_EQ(client->getObjectCalls(), 2u);
}

/// Scoped to an identity CHANGE: a reissue is ordinary, and refusing every retried read would turn a
/// dropped connection into a hard error.
TEST(CASUpstreamSlice, ReadSmallObjectAcceptsAReissueThatAnswersWithTheSameETag)
{
    (void)getContext();

    ScriptedGetObjectClient * client = nullptr;
    auto storage = makeScriptedS3ObjectStorage(client);
    client->script({okStep("\"e1\"", "AAAA", /*fail_mid_body=*/true), okStep("\"e1\"", "AAAA")});

    const auto result = storage->readSmallObjectAndGetObjectMetadata(DB::StoredObject("k"), DB::ReadSettings{}, 1 << 20);
    EXPECT_EQ(result.data, "AAAA");
    EXPECT_EQ(client->getObjectCalls(), 2u);
}

/// Under `SingleAttempt` the read must not retry at all: the caller owns the retry decision and its
/// own deadline. A throttle answer is retryable, so an unpinned buffer would reissue it.
TEST(CASUpstreamSlice, SingleAttemptProfileIssuesExactlyOneGetOnThrottle)
{
    (void)getContext();

    ScriptedGetObjectClient * client = nullptr;
    auto storage = makeScriptedS3ObjectStorage(client);
    client->script({throttleStep(), okStep("\"e1\"", "AAAA")});

    DB::ReadSettings read_settings;
    read_settings.object_storage_retry_profile = DB::ObjectStorageRetryProfile::SingleAttempt;
    EXPECT_ANY_THROW(storage->readSmallObjectAndGetObjectMetadata(DB::StoredObject("k"), read_settings, 1 << 20));

    EXPECT_EQ(client->getObjectCalls(), 1u);
}

TEST(CASUpstreamSlice, SingleAttemptClientCarriesTheRequestedTimeout)
{
    (void)getContext();

    ScriptedGetObjectClient * client = nullptr;
    auto storage = makeScriptedS3ObjectStorage(client);

    const auto base_timeout = storage->getS3StorageClient()->getClientConfiguration().requestTimeoutMs;

    auto kept = storage->getSingleAttemptClient(0);
    EXPECT_EQ(kept->getClientConfiguration().requestTimeoutMs, base_timeout);

    auto bounded = storage->getSingleAttemptClient(1234);
    EXPECT_EQ(bounded->getClientConfiguration().requestTimeoutMs, 1234);
    EXPECT_EQ(bounded->getClientConfiguration().retry_strategy.max_retries, 0u);
    /// The cached clone is keyed by the timeout too, so one built for another bound is never served.
    EXPECT_NE(bounded.get(), kept.get());
}

/// The buffer installs a refreshed client in itself only. A single-attempt read never retries, so that
/// copy is never used; what makes the caller's next attempt sign with the new credentials is the disk
/// client having been replaced.
TEST(CASUpstreamSlice, ExpiredTokenOnSingleAttemptReadInstallsTheRefreshedClientIntoTheStorage)
{
    (void)getContext();

    ScriptedGetObjectClient * expired_client = nullptr;
    const DB::S3::Client * refreshed_client = nullptr;
    auto storage = makeScriptedS3ObjectStorage(
        expired_client,
        [&]() -> std::unique_ptr<const DB::S3::Client>
        {
            auto fresh = std::make_unique<ScriptedGetObjectClient>();
            refreshed_client = fresh.get();
            return fresh;
        });
    expired_client->script({expiredTokenStep()});

    const auto * client_before = storage->getS3StorageClient().get();

    DB::ReadSettings read_settings;
    read_settings.object_storage_retry_profile = DB::ObjectStorageRetryProfile::SingleAttempt;
    EXPECT_ANY_THROW(storage->readSmallObjectAndGetObjectMetadata(DB::StoredObject("k"), read_settings, 1 << 20));

    ASSERT_NE(refreshed_client, nullptr);
    EXPECT_NE(storage->getS3StorageClient().get(), client_before);
    EXPECT_EQ(storage->getS3StorageClient().get(), refreshed_client);
}

/// `refreshAndRetryOnExpiredCredentials` on the HEAD path: the vended credentials expire, the callback
/// hands over a fresh client, and the request is reissued through it. Installing that client into the
/// storage is what stops the next request repeating the failure.
TEST(CASUpstreamSlice, NativeTokenHeadRecoversFromAnExpiredTokenAndInstallsTheRefreshedClient)
{
    (void)getContext();

    ScriptedGetObjectClient * expired = nullptr;
    ScriptedGetObjectClient * refreshed = nullptr;
    auto storage = makeScriptedS3ObjectStorageWithRefresh(
        expired, refreshed, [](const ScriptedGetObjectClient & fresh) { fresh.scriptHead({controlOk()}); });
    /// Installing the refreshed client drops the storage's last reference to this one, so the raw
    /// pointer would dangle before the assertions below read its counters.
    const auto expired_owner = storage->getS3StorageClient();
    expired->scriptHead({controlExpiredToken()});

    const auto metadata = storage->tryGetObjectMetadataWithNativeToken(
        "k", /*with_tags=*/false, DB::ObjectStorageControlRequest{.profile = DB::ObjectStorageRetryProfile::Default});

    ASSERT_TRUE(metadata.has_value());
    ASSERT_NE(refreshed, nullptr);
    EXPECT_EQ(storage->getS3StorageClient().get(), refreshed);
    EXPECT_EQ(expired->headRequestTimeouts().size(), 1u);
    EXPECT_EQ(refreshed->headRequestTimeouts().size(), 1u);
}

/// The same for the conditional DELETE, which is the other verb that issues inline.
TEST(CASUpstreamSlice, ConditionalRemoveRecoversFromAnExpiredTokenAndInstallsTheRefreshedClient)
{
    (void)getContext();

    ScriptedGetObjectClient * expired = nullptr;
    ScriptedGetObjectClient * refreshed = nullptr;
    auto storage = makeScriptedS3ObjectStorageWithRefresh(
        expired, refreshed, [](const ScriptedGetObjectClient & fresh) { fresh.scriptDelete({controlOk()}); });
    /// See the HEAD test: the storage's last reference to this client goes away when the refreshed
    /// one is installed.
    const auto expired_owner = storage->getS3StorageClient();
    expired->scriptDelete({controlExpiredToken()});

    const auto result = storage->removeObjectIfTokenMatches(
        DB::StoredObject("k"), "e", DB::ObjectStorageControlRequest{.profile = DB::ObjectStorageRetryProfile::Default});

    EXPECT_EQ(result.outcome, DB::ConditionalRemoveOutcome::Removed);
    ASSERT_NE(refreshed, nullptr);
    EXPECT_EQ(storage->getS3StorageClient().get(), refreshed);
    EXPECT_EQ(expired->deleteRequestTimeouts().size(), 1u);
    EXPECT_EQ(refreshed->deleteRequestTimeouts().size(), 1u);
}

/// The client the conditional DELETE selects is what decides whether the SDK reissues a throttled
/// request. The `Default` half is what makes "exactly one" mean something: the disk client here does
/// retry a throttle, so a single attempt is a property of the clone, not of the fake.
///
/// Only the DELETE is counted. `S3::Client::HeadObject` does not use the SDK attempt loop at all — it
/// calls the virtual once and returns — so a HEAD is one request under either profile, and what the
/// profile changes for it is the transport bound, which the timeout test below pins.
TEST(CASUpstreamSlice, SingleAttemptConditionalRemoveIssuesExactlyOneRequestOnThrottle)
{
    (void)getContext();

    ScriptedGetObjectClient * retrying = nullptr;
    auto retrying_storage = makeScriptedS3ObjectStorage(retrying);
    retrying->scriptDelete({controlThrottle()});

    EXPECT_ANY_THROW(retrying_storage->removeObjectIfTokenMatches(
        DB::StoredObject("k"), "e", DB::ObjectStorageControlRequest{.profile = DB::ObjectStorageRetryProfile::Default}));
    EXPECT_EQ(retrying->deleteRequestTimeouts().size(), 3u);   /// max_retries = 2, so three attempts

    ScriptedGetObjectClient * client = nullptr;
    auto storage = makeScriptedS3ObjectStorage(client);
    client->scriptDelete({controlThrottle()});

    EXPECT_ANY_THROW(storage->removeObjectIfTokenMatches(
        DB::StoredObject("k"), "e", DB::ObjectStorageControlRequest{.profile = DB::ObjectStorageRetryProfile::SingleAttempt}));
    EXPECT_EQ(client->deleteRequestTimeouts().size(), 1u);
}

/// The reservation the caller budgets for an attempt is only real if the transport is built to it, so
/// the two verbs must ride a clone carrying the timeout they asked for — and two different bounds must
/// coexist, or every alternation between verbs would rebuild a whole S3 client.
TEST(CASUpstreamSlice, HeadAndRemoveUnderSingleAttemptRideTheClientBoundToTheRequestedTimeout)
{
    (void)getContext();

    ScriptedGetObjectClient * client = nullptr;
    auto storage = makeScriptedS3ObjectStorage(client);

    storage->tryGetObjectMetadataWithNativeToken(
        "k", false, DB::ObjectStorageControlRequest{.profile = DB::ObjectStorageRetryProfile::SingleAttempt, .attempt_timeout_ms = 4321});
    ASSERT_EQ(client->headRequestTimeouts().size(), 1u);
    EXPECT_EQ(client->headRequestTimeouts().at(0), 4321);

    storage->removeObjectIfTokenMatches(DB::StoredObject("k"), "e",
        DB::ObjectStorageControlRequest{.profile = DB::ObjectStorageRetryProfile::SingleAttempt, .attempt_timeout_ms = 8765});
    ASSERT_EQ(client->deleteRequestTimeouts().size(), 1u);
    EXPECT_EQ(client->deleteRequestTimeouts().at(0), 8765);

    EXPECT_EQ(client->cloneRequestTimeouts(), (std::vector<long>{4321, 8765}));

    /// Asking again for a bound already built must reuse that clone rather than evict the other one.
    storage->tryGetObjectMetadataWithNativeToken(
        "k", false, DB::ObjectStorageControlRequest{.profile = DB::ObjectStorageRetryProfile::SingleAttempt, .attempt_timeout_ms = 4321});
    EXPECT_EQ(client->cloneRequestTimeouts(), (std::vector<long>{4321, 8765}));
    EXPECT_EQ(client->headRequestTimeouts().at(1), 4321);
}

/// `iterate` issues nothing itself, so its client selection is observed through the clone it causes.
/// The async iterator fetches its first batch lazily, so constructing one sends no request.
TEST(CASUpstreamSlice, IterateUnderSingleAttemptSelectsTheClientBoundToTheRequestedTimeout)
{
    (void)getContext();

    ScriptedGetObjectClient * client = nullptr;
    auto storage = makeScriptedS3ObjectStorage(client);

    (void)storage->iterate("p", 1, false, {}, DB::ObjectStorageControlRequest{.profile = DB::ObjectStorageRetryProfile::Default});
    EXPECT_TRUE(client->cloneRequestTimeouts().empty());

    (void)storage->iterate("p", 1, false, {},
        DB::ObjectStorageControlRequest{.profile = DB::ObjectStorageRetryProfile::SingleAttempt, .attempt_timeout_ms = 4321});
    EXPECT_EQ(client->cloneRequestTimeouts(), (std::vector<long>{4321}));
}

#endif
