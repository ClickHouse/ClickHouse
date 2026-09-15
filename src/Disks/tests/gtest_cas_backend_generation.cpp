#include <gtest/gtest.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Disks/WriteMode.h>
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
#include <Common/logger_useful.h>
#include <Poco/StreamChannel.h>

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

/// Captures what `ObjectStorageBackend` logs at WARNING and above, so a test can assert both that a
/// warning was raised and that none was. Same shape as the capture in gtest_cas_settings.cpp.
class ScopedBackendLogCapture
{
public:
    ScopedBackendLogCapture()
        : logger(getLogger("CasObjectStorageBackend"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel("warning");
    }

    ~ScopedBackendLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    String captured() const { return stream.str(); }

private:
    LoggerPtr logger;
    std::ostringstream stream;
    Poco::AutoPtr<Poco::StreamChannel> channel;
    /// `shared=true` is load-bearing: `AutoPtr(ptr)` would steal a reference the fixture never owned.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

/// Every refusal reached from these mount gates is `NOT_IMPLEMENTED`, so the code alone cannot tell
/// which one fired. Match a phrase unique to the intended message as well, or a test asserting the
/// enabled-versioning refusal would pass on the skip-access-check refusal and vice versa.
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

    /// Placed through the object storage: a Native write over a local storage has no response
    /// incarnation to attribute itself to. Native passes the key verbatim, so this is the object the
    /// HEAD below reads -- anchored under the storage's own root, since a bare relative key would
    /// resolve beside the test process.
    const String key = DB::Cas::tests::nativeKeyUnder(storage, "p/native-head/key");
    {
        auto out = storage->writeObject(
            DB::StoredObject(key), DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteSettings{});
        DB::writeString(String("v1"), *out);
        out->finalize();
    }

    /// Only nativeHead's own call may be observed.
    storage->ordinary_calls = 0;
    storage->native_calls = 0;

    DB::Cas::tests::OperationForTest op(*b);
    const auto hr = (*op).head(key, Retry::once());
    ASSERT_TRUE(hr.has_value());
    EXPECT_EQ(storage->native_calls, 1);
    EXPECT_EQ(storage->ordinary_calls, 0);
}

/// Every token the backend mints carries native_token_type rather than a hardcoded Dialect::ETag.
/// The HEAD mint is the site exercised here; the write-response mint has its own tests over the fake
/// S3 client below, which is the only place a Native write can produce a response incarnation.
TEST(CASBackendGeneration, StampedTokenTypeFollowsNativeKind)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    auto b = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(Dialect::Generation);

    /// A local file's etag is its mtime in nanoseconds, which is also a valid generation value.
    const String key = DB::Cas::tests::nativeKeyUnder(storage, "p/gen/tok");
    {
        auto out = storage->writeObject(DB::StoredObject(key), DB::WriteMode::Rewrite);
        DB::writeString(String("v1"), *out);
        out->finalize();
    }

    DB::Cas::tests::OperationForTest op(*b);
    const auto hr = (*op).head(key, Retry::once());
    ASSERT_TRUE(hr.has_value());
    EXPECT_EQ(hr->etag.dialect(), Dialect::Generation);
    EXPECT_EQ(b->dialect(), Dialect::Generation);
}

/// A generation-dialect (GCS) mount wants bucket versioning to be verifiably off: a token-exact
/// DELETE against a versioned bucket archives a noncurrent generation, so GC would delete objects it
/// believes it reclaimed. A probe that cannot answer is not evidence of a versioned bucket, though:
/// the usual cause is a credential without permission to read the bucket configuration, and
/// refusing on it turns a missing IAM grant into a hard outage. So the mount proceeds, and says
/// loudly what it could not verify and how the operator can.
TEST(CASBackendGeneration, CheckPoolPreconditionsWarnsAndContinuesOnUnverifiableVersioning)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        makeVersioningObjectStorageForTest(std::nullopt), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(Dialect::Generation);

    ScopedBackendLogCapture capture;
    EXPECT_NO_THROW(b->checkPoolPreconditions());

    const auto logged = capture.captured();
    EXPECT_NE(logged.find("could not VERIFY"), String::npos) << logged;
    EXPECT_NE(logged.find("versioning"), String::npos) << logged;
    EXPECT_NE(logged.find("storage.buckets.get"), String::npos) << logged;
}

TEST(CASBackendGeneration, CheckPoolPreconditionsRejectsEnabledVersioning)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        makeVersioningObjectStorageForTest(true), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(Dialect::Generation);

    expectThrowsNotImplementedSaying("VERSIONING enabled", [&] { b->checkPoolPreconditions(); });
}

/// The fully verified case: a probe that answered, and answered "disabled". Nothing to warn about.
TEST(CASBackendGeneration, CheckPoolPreconditionsAcceptsVerifiedDisabledVersioningSilently)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        makeVersioningObjectStorageForTest(false), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(Dialect::Generation);

    ScopedBackendLogCapture capture;
    EXPECT_NO_THROW(b->checkPoolPreconditions());
    EXPECT_TRUE(capture.captured().empty()) << capture.captured();
}

/// The ETag-dialect (AWS-compatible) backend never consults bucket versioning at all — the check is
/// a silent no-op for any backend that is not Native + Dialect::Generation. Driven over a storage
/// whose probe is unverifiable, which is what a generation-dialect backend warns about: dropping the
/// dialect guard from checkPoolPreconditions would fail the silence assertion.
TEST(CASBackendGeneration, CheckPoolPreconditionsNoOpOnEtagDialect)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        makeVersioningObjectStorageForTest(std::nullopt), ObjectStorageBackend::Mode::Native);
    ASSERT_EQ(b->nativeTokenType(), Dialect::ETag);

    ScopedBackendLogCapture capture;
    EXPECT_NO_THROW(b->checkPoolPreconditions());
    EXPECT_TRUE(capture.captured().empty()) << capture.captured();
}

/// A writable generation-dialect (GCS) mount may not skip the mutating capability battery: that
/// battery is the only proof that a token-exact DELETE actually carries its generation precondition.
TEST(CASBackendGeneration, CheckSkipAccessCheckSupportRejectsGenerationDialect)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(Dialect::Generation);

    expectThrowsNotImplementedSaying("skip_access_check=true is not supported", [&] { b->checkSkipAccessCheckSupport(); });
}

/// Scoped to the generation dialect: an ETag-dialect Native backend and the emulated backend keep the
/// pre-existing skip_access_check behaviour, so widening the refusal would fail this test.
TEST(CASBackendGeneration, CheckSkipAccessCheckSupportAllowsEtagAndEmulatedBackends)
{
    auto etag = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    ASSERT_EQ(etag->nativeTokenType(), Dialect::ETag);
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
    b->setNativeTokenTypeForTest(Dialect::Generation);
    EXPECT_FALSE(b->supportsListTokens());
    b->setNativeTokenTypeForTest(Dialect::ETag);
    EXPECT_TRUE(b->supportsListTokens());
}

TEST(CASBackendGeneration, ConditionalWriteSettingsForceSinglePutOnGenerationStores)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    b->setNativeTokenTypeForTest(Dialect::Generation);
    const auto ws = b->conditionalWriteSettingsForTest();
    EXPECT_EQ(ws.object_storage_request_mode, DB::ObjectStorageRequestMode::NativeConditional);
    EXPECT_TRUE(ws.s3_force_single_part_upload);
    EXPECT_EQ(ws.object_storage_retry_profile, DB::ObjectStorageRetryProfile::SingleAttempt);
    EXPECT_EQ(ws.s3_max_unexpected_write_error_retries_override, 1u);
    ASSERT_TRUE(ws.s3_check_objects_after_upload_override.has_value());
    EXPECT_FALSE(*ws.s3_check_objects_after_upload_override);

    b->setNativeTokenTypeForTest(Dialect::ETag);
    const auto ws2 = b->conditionalWriteSettingsForTest();
    EXPECT_EQ(ws2.object_storage_request_mode, DB::ObjectStorageRequestMode::NativeConditional);
    EXPECT_FALSE(ws2.s3_force_single_part_upload);
    EXPECT_EQ(ws2.object_storage_retry_profile, DB::ObjectStorageRetryProfile::SingleAttempt);
    EXPECT_EQ(ws2.s3_max_unexpected_write_error_retries_override, 1u);
    ASSERT_TRUE(ws2.s3_check_objects_after_upload_override.has_value());
    EXPECT_FALSE(*ws2.s3_check_objects_after_upload_override);
}

/// `tokenForHead` and `tokenMatches` were deleted with the `Token` type they built: minting an
/// incarnation and comparing it against a precondition are now `CasRequests::mint`/`tryMint` and
/// `CasRequests::valueFor`, always stamped with the observing backend's own dialect, so no caller can
/// construct or compare one by hand any more. The remaining piece, `tokenForList`'s ETag/Generation
/// gating, stays pinned by `CASBackendGeneration.ListTokensDisabledOnGenerationStores` above;
/// dialect-aware value validation is `CASBackendGrammar.GenerationDialectAcceptsOnlyCanonicalPositiveDecimal`
/// and the cross-backend precondition guard `CASObjectStorageBackend.EmuTokenSurvivesProcessRestartAcrossRecreate`,
/// both in gtest_cas_backend.cpp.

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
    std::shared_ptr<ObjectStorageBackend> makeBackend(Dialect token_type = Dialect::Generation)
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
    backend.setNativeTokenTypeForTest(Dialect::Generation);

    const String payload(1024, 'x');
    DB::Cas::tests::OperationForTest op(backend);
    (*op).publish(BlobPublishRequest{
        .destination_key = "p/gen/publish-multipart",
        .publication = StreamingBlobPublication{
            .payload_size = payload.size(),
            .fresh_envelope = "fresh",
            .open_payload = [payload]
            {
                return std::make_unique<DB::ReadBufferFromOwnString>(payload);
            }}}, Retry::once());

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
    backend.setNativeTokenTypeForTest(Dialect::Generation);
    client->put_returns_no_etag = true;

    const String payload = "payload";
    DB::Cas::tests::OperationForTest op(backend);
    EXPECT_NO_THROW((*op).publish(BlobPublishRequest{
        .destination_key = "p/gen/publish-no-generation",
        .publication = StreamingBlobPublication{
            .payload_size = payload.size(),
            .fresh_envelope = "fresh",
            .open_payload = [payload]
            {
                return std::make_unique<DB::ReadBufferFromOwnString>(payload);
            }}}, Retry::once()));

    EXPECT_EQ(client->put_object_calls, 1u);
    EXPECT_EQ(client->head_object_calls, 0u);
    EXPECT_EQ(client->objects.at("p/gen/publish-no-generation"), "freshpayload");
}

/// The write-response half of the incarnation grammar: a write response that carries no ETag at all
/// must not fall back to a HEAD -- there is no HEAD that can attribute the write with certainty, since
/// the object it would read back might not even be the one this call just wrote. This is the
/// ETag-dialect sibling of PublishBlobSucceedsWithoutResponseGeneration above: a publication has no
/// incarnation to attribute in the first place, so it is unaffected by this guard. Default (ETag)
/// dialect here, deliberately NOT stamped Generation, whose own two cases are covered by
/// CASBackendGenerationS3.WriteEmptyGenerationIsUnresolvedNotThrown and WriteNonNumericGenerationIsUnresolvedNotThrown.
TEST(CASBackendGrammar, NamelessWriteResponseIsUnresolvedNotThrown)
{
    (void)getContext();
    FakeGenerationS3Client * client = nullptr;
    auto storage = makeGenerationS3ObjectStorageForTest(client);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);
    ASSERT_EQ(backend.nativeTokenType(), Dialect::ETag);
    client->put_returns_no_etag = true;

    /// A 2xx write reply carrying no usable incarnation is an ambiguity the engine settles by a
    /// resolve read (CasRequests::writeLoop), never an immediate corruption verdict; the mock's
    /// resolve GET finds nothing at the key, so the create-if-absent precondition is still
    /// satisfiable and a single-attempt policy reports GaveUp{Unresolved} rather than throwing.
    DB::Cas::tests::OperationForTest op(backend);
    const WriteResult result = (*op).create("p/gen/nameless-write", "v", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
    EXPECT_EQ(client->put_object_calls, 1u);
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
    backend.setNativeTokenTypeForTest(Dialect::Generation);

    const String small(32, 'a');
    DB::Cas::tests::OperationForTest op(backend);
    EXPECT_NO_THROW((*op).create("p/gen/under-cap", small, Retry::once()));
    EXPECT_EQ(client->put_object_calls, 1u);
    EXPECT_EQ(client->create_multipart_calls, 0u);
    const auto single_attempt_client = storage->getSingleAttemptClient(/*request_timeout_ms=*/0);
    EXPECT_NE(dynamic_cast<const FakeGenerationS3Client *>(single_attempt_client.get()), nullptr);
    EXPECT_NE(
        dynamic_cast<const DB::S3::SingleAttemptRetryStrategy *>(
            single_attempt_client->getClientConfiguration().retryStrategy.get()),
        nullptr);

    const String large(4096, 'b');
    try
    {
        (*op).create("p/gen/over-cap", large, Retry::once());
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

TEST_F(CASBackendGenerationS3, WriteEmptyGenerationIsUnresolvedNotThrown)
{
    backend = makeBackend();
    client->next_put_etag = "";
    /// The write may well have landed -- an empty response value says nothing about that -- so this is
    /// the resolve-by-reading class, not the corrupt-response one (CasRequests::writeLoop): a 2xx
    /// carrying a value no grammar accepts is an ambiguity, never an immediate corruption verdict. The
    /// mock's resolve GET finds nothing at the key either, so the create-if-absent precondition is
    /// still satisfiable and a single-attempt policy reports GaveUp{Unresolved} rather than throwing.
    DB::Cas::tests::OperationForTest op(*backend);
    const WriteResult result = (*op).create("p/gen/no-etag", "v", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
}

TEST_F(CASBackendGenerationS3, WriteNonNumericGenerationIsUnresolvedNotThrown)
{
    backend = makeBackend();
    /// An MD5-shaped ETag where a generation belongs: the store answered, but not with an incarnation
    /// this dialect can use; see WriteEmptyGenerationIsUnresolvedNotThrown for why this settles as an
    /// ambiguity (GaveUp{Unresolved}) rather than a thrown exception.
    client->next_put_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";
    DB::Cas::tests::OperationForTest op(*backend);
    const WriteResult result = (*op).create("p/gen/bad-etag", "v", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
}

/// A mutable conditional write whose response generation arrives quoted -- exactly what
/// `applyGcsConditionalDialectToResponse` produces -- must yield an UNQUOTED, all-digits token.
/// Before the fix this threw CORRUPTED_DATA, so every GCS CAS write failed and no pool could mount.
TEST_F(CASBackendGenerationS3, WriteGenerationTokenStripsTransportQuoting)
{
    backend = makeBackend();
    client->next_put_etag = "\"1783078552147137\"";
    DB::Cas::tests::OperationForTest op(*backend);
    const WriteResult put = (*op).create("p/gen/quoted-write", "v", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(put));
    const Etag & minted = std::get<Committed>(put).etag;
    EXPECT_EQ(minted.dialect(), Dialect::Generation);
    EXPECT_EQ(PersistedEtag::capture(minted).value, "1783078552147137");
}

/// The same crossing on the read side: a marked HEAD whose ETag field carries a quoted generation
/// must mint the same unquoted token, so a token observed by HEAD compares equal to one returned by
/// the write that created it.
TEST_F(CASBackendGenerationS3, HeadGenerationTokenStripsTransportQuoting)
{
    backend = makeBackend();
    client->objects["p/gen/quoted-head"] = "body";
    client->next_head_etag = "\"1783078552147137\"";

    DB::Cas::tests::OperationForTest op(*backend);
    const auto hr = (*op).head("p/gen/quoted-head", Retry::once());
    ASSERT_TRUE(hr.has_value());
    EXPECT_EQ(hr->etag.dialect(), Dialect::Generation);
    EXPECT_EQ(PersistedEtag::capture(hr->etag).value, "1783078552147137");
}

/// The bound on that stripping. An ETag-dialect token IS the quoted ETag, and the quotes are required
/// syntax when it goes back out as `If-Match`, so the AWS-compatible path must keep them verbatim.
/// This is the test that fails if the quote handling is ever made unconditional.
TEST_F(CASBackendGenerationS3, EtagDialectKeepsTransportQuotingVerbatim)
{
    backend = makeBackend(Dialect::ETag);
    client->objects["p/etag/quoted-head"] = "body";
    client->next_head_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";

    DB::Cas::tests::OperationForTest op(*backend);
    const auto hr = (*op).head("p/etag/quoted-head", Retry::once());
    ASSERT_TRUE(hr.has_value());
    EXPECT_EQ(hr->etag.dialect(), Dialect::ETag);
    EXPECT_EQ(PersistedEtag::capture(hr->etag).value, "\"d41d8cd98f00b204e9800998ecf8427e\"");
}

/// A successful HEAD on a generation-dialect backend whose response carries no ETag/generation at all must not mint a token
/// from it -- there is no follow-up HEAD to patch this over, so nativeHead must refuse it directly.
TEST_F(CASBackendGenerationS3, HeadMissingGenerationThrows)
{
    backend = makeBackend();
    client->objects["p/gen/no-generation-head"] = "body";
    /// next_head_etag stays empty: SetETag is never called, so the response carries no ETag field.

    DB::Cas::tests::OperationForTest op(*backend);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (*op).head("p/gen/no-generation-head", Retry::once()); });
}

/// An ordinary AWS-style ETag reaching a generation-dialect backend through a successful HEAD (a proxy dropping
/// x-goog-generation, a service regression) must not be minted as a generation token either.
TEST_F(CASBackendGenerationS3, HeadNonNumericGenerationThrows)
{
    backend = makeBackend();
    client->objects["p/gen/bad-etag-head"] = "body";
    client->next_head_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";

    DB::Cas::tests::OperationForTest op(*backend);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (*op).head("p/gen/bad-etag-head", Retry::once()); });
}

#endif
