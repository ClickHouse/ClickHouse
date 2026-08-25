#include <gtest/gtest.h>
#include <IO/ReadBufferFromString.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/tests/cas_test_helpers.h>

#include "config.h"

#if USE_AWS_S3
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/config/AWSProfileConfigLoader.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>

#include <IO/S3Common.h>
#include <IO/S3/Client.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Common/tests/gtest_global_context.h>

#include <mutex>
#include <sstream>
#endif

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

#if USE_AWS_S3
namespace DB::S3RequestSetting
{
extern const S3RequestSettingsUInt64 max_single_part_upload_size;
extern const S3RequestSettingsUInt64 min_upload_part_size;
}

namespace DB::S3AuthSetting
{
extern const S3AuthSettingsUInt64 gcs_max_conditional_put_bytes;
}
#endif

namespace
{
/// A `LocalObjectStorage` that records which of the two metadata-read virtuals a caller reached, so a
/// test can prove `nativeHead` calls `tryGetObjectMetadataWithNativeToken` specifically -- reverting
/// that one line back to `tryGetObjectMetadata` makes `NativeHeadUsesNativeTokenMetadataApi` fail.
class RecordingObjectStorage : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    mutable int ordinary_calls = 0;
    mutable int native_calls = 0;

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        ++ordinary_calls;
        return DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadataWithNativeToken(const std::string & path, bool with_tags) const override
    {
        ++native_calls;
        return DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
    }
};

/// Same unique-temp-root convention as `DB::Cas::tests::makeLocalObjectStorageForTest`, but returning
/// the concrete recording type so the test can read its call counters.
std::shared_ptr<RecordingObjectStorage> makeRecordingObjectStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_unit_native_head_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<RecordingObjectStorage>(std::move(settings));
}

/// A `LocalObjectStorage` that answers the bucket-versioning probe with a value the test chooses, so
/// the three outcomes `checkPoolPreconditions` distinguishes — verified disabled, verified enabled,
/// and unverifiable — can each be driven exactly. The base `IObjectStorage` default answers only the
/// third.
class VersioningObjectStorage : public DB::LocalObjectStorage
{
public:
    VersioningObjectStorage(DB::LocalObjectStorageSettings settings_, std::optional<bool> versioned_)
        : DB::LocalObjectStorage(std::move(settings_)), versioned(versioned_)
    {
    }

    std::optional<bool> isBucketVersioningEnabled() const override { return versioned; }

private:
    const std::optional<bool> versioned;
};

std::shared_ptr<VersioningObjectStorage> makeVersioningObjectStorageForTest(std::optional<bool> versioned)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_unit_versioning_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<VersioningObjectStorage>(std::move(settings), versioned);
}

/// Every refusal reached from these mount gates is `NOT_IMPLEMENTED`, so the code alone cannot tell
/// which one fired. Match a phrase unique to the intended message as well, or a test asserting the
/// unverifiable-versioning refusal would pass on the enabled-bucket refusal and vice versa.
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

/// `ObjectStorageBackend::nativeHead` must route through `tryGetObjectMetadataWithNativeToken` (the
/// hook that lets a GCS-native client read a generation token), not the ordinary `tryGetObjectMetadata`.
TEST(CASBackendGeneration, NativeHeadUsesNativeTokenMetadataApi)
{
    auto storage = makeRecordingObjectStorageForTest();
    auto b = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);

    ASSERT_EQ(b->putIfAbsent("p/native-head/key", "v1").outcome, PutOutcome::Done);

    /// putIfAbsent's own HEAD-fallback stamping path calls the ordinary API (untouched by this task);
    /// reset the counters so only nativeHead's call, below, is observed.
    storage->ordinary_calls = 0;
    storage->native_calls = 0;

    const auto hr = b->head("p/native-head/key");
    ASSERT_TRUE(hr.exists);
    EXPECT_EQ(storage->native_calls, 1);
    EXPECT_EQ(storage->ordinary_calls, 0);
}

/// Every Token{...} the backend mints must carry native_token_type instead of a hardcoded
/// TokenType::ETag (Task 5). Mode::Native over a LocalObjectStorage has no write-time ETag, so
/// putIfAbsent's PutResult falls back to a HEAD internally — that HEAD is also a stamping site,
/// so the assertion below exercises both the direct-etag and the HEAD-fallback mint paths.
TEST(CASBackendGeneration, StampedTokenTypeFollowsNativeKind)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(TokenType::Generation);

    const auto put = b->putIfAbsent("p/gen/tok", "v1");
    EXPECT_EQ(put.token.type, TokenType::Generation);

    const auto hr = b->head("p/gen/tok");
    ASSERT_TRUE(hr.exists);
    EXPECT_EQ(hr.token.type, TokenType::Generation);
}

/// A generation-dialect (GCS) mount needs bucket versioning to be VERIFIABLY off: a token-exact
/// DELETE against a versioned bucket archives a noncurrent generation, so GC would delete objects it
/// believes it reclaimed. A probe that cannot answer therefore refuses the mount rather than
/// assuming the safe answer.
TEST(CASBackendGeneration, CheckPoolPreconditionsFailsClosedOnUnverifiableVersioning)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        makeVersioningObjectStorageForTest(std::nullopt), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(TokenType::Generation);

    expectThrowsNotImplementedSaying("could not VERIFY", [&] { b->checkPoolPreconditions(); });
}

TEST(CASBackendGeneration, CheckPoolPreconditionsRejectsEnabledVersioning)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        makeVersioningObjectStorageForTest(true), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(TokenType::Generation);

    expectThrowsNotImplementedSaying("VERSIONING enabled", [&] { b->checkPoolPreconditions(); });
}

/// The one accepting case: a probe that answered, and answered "disabled".
TEST(CASBackendGeneration, CheckPoolPreconditionsAcceptsVerifiedDisabledVersioning)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        makeVersioningObjectStorageForTest(false), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(TokenType::Generation);

    EXPECT_NO_THROW(b->checkPoolPreconditions());
}

/// The ETag-dialect (AWS-compatible) backend never consults bucket versioning at all — the check is
/// a silent no-op for any backend that is not Native + TokenType::Generation. Driven over a storage
/// whose probe is unverifiable, which is what a generation-dialect backend now refuses: dropping the
/// dialect guard from checkPoolPreconditions would fail this test.
TEST(CASBackendGeneration, CheckPoolPreconditionsNoOpOnEtagDialect)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        makeVersioningObjectStorageForTest(std::nullopt), ObjectStorageBackend::Mode::Native);
    ASSERT_EQ(b->nativeTokenType(), TokenType::ETag);

    EXPECT_NO_THROW(b->checkPoolPreconditions());
}

/// A writable generation-dialect (GCS) mount may not skip the mutating capability battery: that
/// battery is the only proof that a token-exact DELETE actually carries its generation precondition.
TEST(CASBackendGeneration, CheckSkipAccessCheckSupportRejectsGenerationDialect)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(TokenType::Generation);

    expectThrowsNotImplementedSaying("skip_access_check=true is not supported", [&] { b->checkSkipAccessCheckSupport(); });
}

/// Scoped to the generation dialect: an ETag-dialect Native backend and the emulated backend keep the
/// pre-existing skip_access_check behaviour, so widening the refusal would fail this test.
TEST(CASBackendGeneration, CheckSkipAccessCheckSupportAllowsEtagAndEmulatedBackends)
{
    auto etag = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    ASSERT_EQ(etag->nativeTokenType(), TokenType::ETag);
    EXPECT_NO_THROW(etag->checkSkipAccessCheckSupport());

    auto emulated = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    EXPECT_NO_THROW(emulated->checkSkipAccessCheckSupport());
}

/// GCS enforces NO preconditions on CompleteMultipartUpload (measured 2026-07-03), so a conditional
/// write on a generation-token store must never take the multipart path. conditionalWriteSettings
/// must force the single-PUT path when the backend's native token kind is Generation, and stay a
/// no-op otherwise (ETag dialect).
TEST(CASBackendGeneration, ListTokensDisabledOnGenerationStores)
{
    /// XML LIST bodies carry MD5-style ETags that the dialect cannot rewrite to generations; a
    /// list-derived token on a generation store is a poisoned If-Match (live GC on GCS died there).
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    EXPECT_TRUE(b->supportsListTokens());
    b->setNativeTokenTypeForTest(TokenType::Generation);
    EXPECT_FALSE(b->supportsListTokens());
    b->setNativeTokenTypeForTest(TokenType::ETag);
    EXPECT_TRUE(b->supportsListTokens());
}

TEST(CASBackendGeneration, ConditionalWriteSettingsForceSinglePutOnGenerationStores)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(TokenType::Generation);
    const auto ws = b->conditionalWriteSettingsForTest();
    EXPECT_EQ(ws.object_storage_request_mode, DB::ObjectStorageRequestMode::NativeConditional);
    EXPECT_TRUE(ws.s3_force_single_part_upload);
    EXPECT_EQ(ws.object_storage_retry_profile, DB::ObjectStorageRetryProfile::SingleAttempt);
    EXPECT_EQ(ws.s3_max_unexpected_write_error_retries_override, 1u);
    ASSERT_TRUE(ws.s3_check_objects_after_upload_override.has_value());
    EXPECT_FALSE(*ws.s3_check_objects_after_upload_override);

    b->setNativeTokenTypeForTest(TokenType::ETag);
    const auto ws2 = b->conditionalWriteSettingsForTest();
    EXPECT_EQ(ws2.object_storage_request_mode, DB::ObjectStorageRequestMode::NativeConditional);
    EXPECT_FALSE(ws2.s3_force_single_part_upload);
    EXPECT_EQ(ws2.object_storage_retry_profile, DB::ObjectStorageRetryProfile::SingleAttempt);
    EXPECT_EQ(ws2.s3_max_unexpected_write_error_retries_override, 1u);
    ASSERT_TRUE(ws2.s3_check_objects_after_upload_override.has_value());
    EXPECT_FALSE(*ws2.s3_check_objects_after_upload_override);
}

/// C1: the three token-policy helpers are the single source of truth for how a Native-mode backend
/// mints a HEAD/PUT token, gates a LIST token, and compares tokens. Characterizes the behavior the
/// scattered call sites have today so the consolidation stays byte-for-byte behavior-preserving.
TEST(CASBackendGeneration, TokenPolicyHelpersAreConsistentWithDialect)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);

    /// ETag dialect: head/put tokens carry ETag; list surfaces the same-typed token for a non-empty etag.
    ASSERT_EQ(b->nativeTokenType(), TokenType::ETag);
    EXPECT_EQ(b->tokenForHead("abc").type, TokenType::ETag);
    EXPECT_EQ(b->tokenForHead("abc"), (Token{"abc", TokenType::ETag}));
    ASSERT_TRUE(b->tokenForList("abc").has_value());
    EXPECT_EQ(*b->tokenForList("abc"), b->tokenForHead("abc"));   /// list token == head token (same etag)
    EXPECT_FALSE(b->tokenForList("").has_value());                /// empty etag => no list token

    /// Generation dialect (GCS): head token flips to Generation; list tokens are disabled wholesale
    /// (poisoned If-Match), so tokenForList is always nullopt regardless of the etag.
    b->setNativeTokenTypeForTest(TokenType::Generation);
    EXPECT_EQ(b->tokenForHead("g1").type, TokenType::Generation);
    EXPECT_FALSE(b->tokenForList("g1").has_value());

    /// tokenMatches is exact identity (value AND type) — a same-value/different-type token never matches.
    EXPECT_TRUE(ObjectStorageBackend::tokenMatches(Token{"x", TokenType::ETag}, Token{"x", TokenType::ETag}));
    EXPECT_FALSE(ObjectStorageBackend::tokenMatches(Token{"x", TokenType::ETag}, Token{"x", TokenType::Emulated}));
}

#if USE_AWS_S3

namespace
{

/// Minimal S3 double for the CasObjectStorageBackend generation-token write battery: just enough of
/// `DB::S3::Client` to drive a real `WriteBufferFromS3` end to end (`PutObject`, multipart upload, and
/// `HeadObject`). `GetObject` is not overridden: reading a written body back verifies against
/// `objects` directly (see the tests below), rather than through the considerably more involved
/// `ReadBufferFromS3` read path (range/retry/prefetch machinery), which this fake does not attempt to
/// support.
class FakeGenerationS3Client : public DB::S3::Client
{
private:
    struct State
    {
        std::string next_put_etag = "1000";
        bool put_returns_no_etag = false;
        std::string next_head_etag;

        size_t put_object_calls = 0;
        size_t head_object_calls = 0;
        size_t create_multipart_calls = 0;
        size_t upload_part_calls = 0;
        size_t complete_multipart_calls = 0;
        size_t abort_multipart_calls = 0;

        std::map<std::string, std::string> objects;
        std::map<int, std::string> multipart_parts;
        std::mutex mutex;
    };

    const std::shared_ptr<State> state;

public:

    FakeGenerationS3Client()
        : FakeGenerationS3Client(std::make_shared<State>(), GetClientConfiguration())
    {
    }

    static DB::S3::PocoHTTPClientConfiguration GetClientConfiguration()
    {
        DB::RemoteHostFilter remote_host_filter;
        return DB::S3::ClientFactory::instance().createClientConfiguration(
            "some-region",
            remote_host_filter,
            /* s3_max_redirects = */ 100,
            DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
            /* s3_slow_all_threads_after_network_error = */ true,
            /* s3_slow_all_threads_after_retryable_error = */ true,
            /* enable_s3_requests_logging = */ true,
            /* for_disk_s3 = */ false,
            /* opt_disk_name = */ {},
            /* request_throttler = */ {});
    }

    /// The response ETag/generation the NEXT successful PutObject returns; empty means the response
    /// carries no ETag at all (SetETag never called) -- the "broken/lying remote" case Step 7 guards.
    std::string & next_put_etag;
    bool & put_returns_no_etag;
    std::string & next_head_etag;

    size_t & put_object_calls;
    size_t & head_object_calls;
    size_t & create_multipart_calls;
    size_t & upload_part_calls;
    size_t & complete_multipart_calls;
    size_t & abort_multipart_calls;

    std::map<std::string, std::string> & objects;
    std::map<int, std::string> & multipart_parts;
    std::mutex & mutex;

    std::unique_ptr<DB::S3::Client> cloneWithConfigurationOverride(
        const DB::S3::PocoHTTPClientConfiguration & client_configuration_override) const override
    {
        return std::unique_ptr<DB::S3::Client>(new FakeGenerationS3Client(state, client_configuration_override));
    }

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest & request) const override
    {
        std::lock_guard lock(mutex);
        ++put_object_calls;
        std::stringstream data;
        data << request.GetBody()->rdbuf();
        objects[request.GetKey()] = data.str();

        Aws::S3::Model::PutObjectResult result;
        if (!put_returns_no_etag)
            result.SetETag(next_put_etag);
        return result;
    }

    Aws::S3::Model::HeadObjectOutcome HeadObject(const Aws::S3::Model::HeadObjectRequest & request) const override
    {
        std::lock_guard lock(mutex);
        ++head_object_calls;
        Aws::S3::Model::HeadObjectOutcome outcome;
        Aws::S3::Model::HeadObjectResult result(outcome.GetResultWithOwnership());
        auto it = objects.find(request.GetKey());
        result.SetContentLength(it == objects.end() ? 0 : it->second.size());
        if (!next_head_etag.empty())
            result.SetETag(next_head_etag);
        return result;
    }

    Aws::S3::Model::CreateMultipartUploadOutcome CreateMultipartUpload(
        const Aws::S3::Model::CreateMultipartUploadRequest & /*request*/) const override
    {
        std::lock_guard lock(mutex);
        ++create_multipart_calls;
        multipart_parts.clear();
        Aws::S3::Model::CreateMultipartUploadResult result;
        result.SetUploadId("publish-upload");
        return result;
    }

    Aws::S3::Model::UploadPartOutcome UploadPart(const Aws::S3::Model::UploadPartRequest & request) const override
    {
        std::lock_guard lock(mutex);
        ++upload_part_calls;
        std::stringstream data;
        data << request.GetBody()->rdbuf();
        multipart_parts[request.GetPartNumber()] = data.str();

        Aws::S3::Model::UploadPartResult result;
        result.SetETag("part-" + std::to_string(request.GetPartNumber()));
        return result;
    }

    Aws::S3::Model::CompleteMultipartUploadOutcome CompleteMultipartUpload(
        const Aws::S3::Model::CompleteMultipartUploadRequest & request) const override
    {
        std::lock_guard lock(mutex);
        ++complete_multipart_calls;
        String body;
        for (const auto & [part_number, part] : multipart_parts)
        {
            (void)part_number;
            body += part;
        }
        objects[request.GetKey()] = std::move(body);

        Aws::S3::Model::CompleteMultipartUploadResult result;
        if (!put_returns_no_etag)
            result.SetETag(next_put_etag);
        return result;
    }

    Aws::S3::Model::AbortMultipartUploadOutcome AbortMultipartUpload(
        const Aws::S3::Model::AbortMultipartUploadRequest & /*request*/) const override
    {
        std::lock_guard lock(mutex);
        ++abort_multipart_calls;
        multipart_parts.clear();
        return Aws::S3::Model::AbortMultipartUploadResult{};
    }

private:
    FakeGenerationS3Client(
        std::shared_ptr<State> state_,
        const DB::S3::PocoHTTPClientConfiguration & client_configuration)
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
        , next_put_etag(state->next_put_etag)
        , put_returns_no_etag(state->put_returns_no_etag)
        , next_head_etag(state->next_head_etag)
        , put_object_calls(state->put_object_calls)
        , head_object_calls(state->head_object_calls)
        , create_multipart_calls(state->create_multipart_calls)
        , upload_part_calls(state->upload_part_calls)
        , complete_multipart_calls(state->complete_multipart_calls)
        , abort_multipart_calls(state->abort_multipart_calls)
        , objects(state->objects)
        , multipart_parts(state->multipart_parts)
        , mutex(state->mutex)
    {
    }

};

std::shared_ptr<DB::S3ObjectStorage> makeGenerationS3ObjectStorageForTest(
    FakeGenerationS3Client *& out_client,
    bool force_multipart = false,
    std::optional<UInt64> conditional_put_cap = {})
{
    auto owned_client = std::make_unique<FakeGenerationS3Client>();
    out_client = owned_client.get();

    DB::S3::URI uri;
    uri.bucket = "cas-generation-bucket";
    DB::S3Capabilities capabilities;
    DB::ObjectStorageKeyGeneratorPtr key_generator;

    auto settings = std::make_unique<DB::S3Settings>();
    if (force_multipart)
    {
        settings->request_settings[DB::S3RequestSetting::max_single_part_upload_size] = 0;
        settings->request_settings[DB::S3RequestSetting::min_upload_part_size] = 64;
    }

    if (conditional_put_cap)
        settings->auth_settings[DB::S3AuthSetting::gcs_max_conditional_put_bytes] = *conditional_put_cap;

    return std::make_shared<DB::S3ObjectStorage>(
        std::move(owned_client), std::move(settings), std::move(uri), capabilities, key_generator, "cas-generation-disk");
}

}

/// The "generation-token write kind" battery (Task 3, Step 2): a real WriteBufferFromS3 over a fake
/// S3 client, so the single-PUT cap enforcement and exact-token attribution are exercised for real,
/// not merely characterized through settings. Suite name deliberately starts with "CASBackendGeneration"
/// and every test name below contains "SinglePut", matching this plan's gtest filter.
class CASBackendGenerationS3 : public ::testing::Test
{
protected:
    FakeGenerationS3Client * client = nullptr;
    std::shared_ptr<ObjectStorageBackend> backend;

    void SetUp() override
    {
        (void)getContext();   /// see S3ObjectStorageConditionalOpsTest::SetUp in gtest_writebuffer_s3.cpp
    }

    /// A fresh backend, native token type forced to Generation unless overridden (the ETag dialect
    /// is needed to prove the generation-only quote handling does not touch it).
    std::shared_ptr<ObjectStorageBackend> makeBackend(TokenType token_type = TokenType::Generation)
    {
        auto storage = makeGenerationS3ObjectStorageForTest(client);
        auto b = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
        b->setNativeTokenTypeForTest(token_type);
        return b;
    }
};

TEST(CASBackendGeneration, PublishBlobAboveFormerGenerationCapUsesOrdinaryMultipart)
{
    (void)getContext();
    FakeGenerationS3Client * client = nullptr;
    auto storage = makeGenerationS3ObjectStorageForTest(
        client, /*force_multipart=*/true, /*conditional_put_cap=*/16);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);
    backend.setNativeTokenTypeForTest(TokenType::Generation);

    const String payload(1024, 'x');
    backend.publishBlob(BlobPublishRequest{
        .destination_key = "p/gen/publish-multipart",
        .publication = StreamingBlobPublication{
            .payload_size = payload.size(),
            .fresh_envelope = "fresh",
            .open_payload = [payload]
            {
                return std::make_unique<DB::ReadBufferFromOwnString>(payload);
            }}});

    EXPECT_EQ(client->put_object_calls, 0u);
    EXPECT_EQ(client->create_multipart_calls, 1u);
    EXPECT_GT(client->upload_part_calls, 0u);
    EXPECT_EQ(client->complete_multipart_calls, 1u);
    EXPECT_EQ(client->abort_multipart_calls, 0u);
    EXPECT_EQ(client->head_object_calls, 0u);
    EXPECT_EQ(client->objects.at("p/gen/publish-multipart"), "fresh" + payload);
}

TEST(CASBackendGeneration, PublishBlobSucceedsWithoutResponseGeneration)
{
    (void)getContext();
    FakeGenerationS3Client * client = nullptr;
    auto storage = makeGenerationS3ObjectStorageForTest(
        client, /*force_multipart=*/false, /*conditional_put_cap=*/1);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);
    backend.setNativeTokenTypeForTest(TokenType::Generation);
    client->put_returns_no_etag = true;

    const String payload = "payload";
    EXPECT_NO_THROW(backend.publishBlob(BlobPublishRequest{
        .destination_key = "p/gen/publish-no-generation",
        .publication = StreamingBlobPublication{
            .payload_size = payload.size(),
            .fresh_envelope = "fresh",
            .open_payload = [payload]
            {
                return std::make_unique<DB::ReadBufferFromOwnString>(payload);
            }}}));

    EXPECT_EQ(client->put_object_calls, 1u);
    EXPECT_EQ(client->head_object_calls, 0u);
    EXPECT_EQ(client->objects.at("p/gen/publish-no-generation"), "freshpayload");
}

/// The moved cap, end to end: a conditional write on a generation store stays in ONE PUT up to the
/// cap the OBJECT STORAGE carries, and refuses rather than silently taking the multipart path above
/// it -- GCS enforces no precondition on CompleteMultipartUpload.
TEST(CASBackendGeneration, ConditionalWriteHonoursTheObjectStorageConditionalPutCap)
{
    (void)getContext();
    FakeGenerationS3Client * client = nullptr;
    auto storage = makeGenerationS3ObjectStorageForTest(
        client, /*force_multipart=*/false, /*conditional_put_cap=*/64);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);
    backend.setNativeTokenTypeForTest(TokenType::Generation);

    const String small(32, 'a');
    EXPECT_NO_THROW(backend.casPut("p/gen/under-cap", small, std::nullopt, ObjectMeta{}));
    EXPECT_EQ(client->put_object_calls, 1u);
    EXPECT_EQ(client->create_multipart_calls, 0u);
    const auto single_attempt_client = storage->getSingleAttemptClient();
    EXPECT_NE(dynamic_cast<const FakeGenerationS3Client *>(single_attempt_client.get()), nullptr);
    EXPECT_NE(
        dynamic_cast<const DB::S3::SingleAttemptRetryStrategy *>(
            single_attempt_client->getClientConfiguration().retryStrategy.get()),
        nullptr);

    const String large(4096, 'b');
    try
    {
        backend.casPut("p/gen/over-cap", large, std::nullopt, ObjectMeta{});
        FAIL() << "a conditional write above the cap must refuse, not go multipart";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NOT_IMPLEMENTED);
    }
    EXPECT_EQ(client->create_multipart_calls, 0u);
}

/// ---- The transport-quoting seam ----
///
/// A GCS generation reaches this layer through the SDK's ETag field, and the HTTP boundary fills that
/// field with an ETag-shaped, QUOTED value. Every test above this point feeds the write path an
/// UNQUOTED generation (`next_put_etag = "778899"`), and the HTTP-layer tests assert the field is
/// quoted -- each half self-consistent, neither crossing the seam between them. Nothing checked what
/// the CAS layer receives in the shape production actually produces, which is why a mount that could
/// never succeed passed every unit test. These three tests are that crossing.

TEST_F(CASBackendGenerationS3, WriteEmptyGenerationThrows)
{
    backend = makeBackend();
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::CORRUPTED_DATA,
        [&] { backend->tokenFromWriteResult("p/gen/no-etag", String{}); });
}

TEST_F(CASBackendGenerationS3, WriteNonNumericGenerationThrows)
{
    backend = makeBackend();
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::CORRUPTED_DATA,
        [&] { backend->tokenFromWriteResult("p/gen/bad-etag", "\"d41d8cd98f00b204e9800998ecf8427e\""); });
}

/// A mutable conditional write whose response generation arrives quoted -- exactly what
/// `applyGcsConditionalDialectToResponse` produces -- must yield an UNQUOTED, all-digits token.
/// Before the fix this threw CORRUPTED_DATA, so every GCS CAS write failed and no pool could mount.
TEST_F(CASBackendGenerationS3, WriteGenerationTokenStripsTransportQuoting)
{
    backend = makeBackend();
    const Token tok = backend->tokenFromWriteResult("p/gen/quoted-write", "\"1783078552147137\"");
    EXPECT_EQ(tok, (Token{"1783078552147137", TokenType::Generation}));
}

/// The same crossing on the read side: a marked HEAD whose ETag field carries a quoted generation
/// must mint the same unquoted token, so a token observed by HEAD compares equal to one returned by
/// the write that created it.
TEST_F(CASBackendGenerationS3, HeadGenerationTokenStripsTransportQuoting)
{
    backend = makeBackend();
    client->objects["p/gen/quoted-head"] = "body";
    client->next_head_etag = "\"1783078552147137\"";

    const auto hr = backend->head("p/gen/quoted-head");
    ASSERT_TRUE(hr.exists);
    EXPECT_EQ(hr.token, (Token{"1783078552147137", TokenType::Generation}));
}

/// The bound on that stripping. An ETag-dialect token IS the quoted ETag, and the quotes are required
/// syntax when it goes back out as `If-Match`, so the AWS-compatible path must keep them verbatim.
/// This is the test that fails if the quote handling is ever made unconditional.
TEST_F(CASBackendGenerationS3, EtagDialectKeepsTransportQuotingVerbatim)
{
    backend = makeBackend(TokenType::ETag);
    client->objects["p/etag/quoted-head"] = "body";
    client->next_head_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";

    const auto hr = backend->head("p/etag/quoted-head");
    ASSERT_TRUE(hr.exists);
    EXPECT_EQ(hr.token, (Token{"\"d41d8cd98f00b204e9800998ecf8427e\"", TokenType::ETag}));
}

/// A successful HEAD on a generation-dialect backend whose response carries no ETag/generation at all must not mint a token
/// from it -- there is no follow-up HEAD to patch this over, so nativeHead must refuse it directly.
TEST_F(CASBackendGenerationS3, HeadMissingGenerationThrows)
{
    backend = makeBackend();
    client->objects["p/gen/no-generation-head"] = "body";
    /// next_head_etag stays empty: SetETag is never called, so the response carries no ETag field.

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { backend->head("p/gen/no-generation-head"); });
}

/// An ordinary AWS-style ETag reaching a generation-dialect backend through a successful HEAD (a proxy dropping
/// x-goog-generation, a service regression) must not be minted as a generation token either.
TEST_F(CASBackendGenerationS3, HeadNonNumericGenerationThrows)
{
    backend = makeBackend();
    client->objects["p/gen/bad-etag-head"] = "body";
    client->next_head_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { backend->head("p/gen/bad-etag-head"); });
}

#endif
