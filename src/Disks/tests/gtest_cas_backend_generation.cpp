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
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>

#include <IO/S3Common.h>
#include <IO/S3/Client.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Common/tests/gtest_global_context.h>

#include <sstream>
#endif

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

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
/// must force the single-PUT path (and raise the single-part cap to conditional_single_put_cap) when
/// the backend's native token kind is Generation, and stay a no-op otherwise (ETag dialect).
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
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native,
        /*token_producing_single_put_cap=*/123);
    b->setNativeTokenTypeForTest(TokenType::Generation);
    const auto ws = b->conditionalWriteSettingsForTest();
    EXPECT_TRUE(ws.s3_force_single_part_upload);
    EXPECT_EQ(ws.s3_single_part_upload_max_bytes_override, 123u);

    b->setNativeTokenTypeForTest(TokenType::ETag);
    const auto ws2 = b->conditionalWriteSettingsForTest();
    EXPECT_FALSE(ws2.s3_force_single_part_upload);
    EXPECT_EQ(ws2.s3_single_part_upload_max_bytes_override, 0u);
}

/// Write-settings decomposition: tokenProducingWriteSettings is the layer conditionalWriteSettings
/// builds on. It always marks the write NativeConditional (dialect-agnostic -- the bit only takes
/// effect when Client::BuildHttpRequest's GCS-capability gate lets it through), and it alone decides
/// the single-PUT/cap forcing; conditionalWriteSettings must not duplicate or override that decision.
TEST(CASBackendGeneration, TokenProducingWriteSettingsMarksNativeConditionalAndForcesSinglePutOnlyOnGenerationStores)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native,
        /*token_producing_single_put_cap=*/321);

    const auto ws_etag = b->tokenProducingWriteSettingsForTest();
    EXPECT_EQ(ws_etag.object_storage_request_mode, DB::ObjectStorageRequestMode::NativeConditional);
    EXPECT_FALSE(ws_etag.s3_force_single_part_upload);
    EXPECT_EQ(ws_etag.s3_single_part_upload_max_bytes_override, 0u);

    b->setNativeTokenTypeForTest(TokenType::Generation);
    const auto ws_gen = b->tokenProducingWriteSettingsForTest();
    EXPECT_EQ(ws_gen.object_storage_request_mode, DB::ObjectStorageRequestMode::NativeConditional);
    EXPECT_TRUE(ws_gen.s3_force_single_part_upload);
    EXPECT_EQ(ws_gen.s3_single_part_upload_max_bytes_override, 321u);

    /// conditionalWriteSettings must layer on top, not replace: same request mode and cap, plus the
    /// precondition-specific retry policy tokenProducingWriteSettings deliberately omits.
    const auto ws_cond = b->conditionalWriteSettingsForTest();
    EXPECT_EQ(ws_cond.object_storage_request_mode, DB::ObjectStorageRequestMode::NativeConditional);
    EXPECT_TRUE(ws_cond.s3_force_single_part_upload);
    EXPECT_EQ(ws_cond.s3_single_part_upload_max_bytes_override, 321u);
    EXPECT_EQ(ws_cond.object_storage_retry_profile, DB::ObjectStorageRetryProfile::SingleAttempt);
    EXPECT_EQ(ws_cond.s3_max_unexpected_write_error_retries_override, 1u);
    ASSERT_TRUE(ws_cond.s3_check_objects_after_upload_override.has_value());
    EXPECT_FALSE(*ws_cond.s3_check_objects_after_upload_override);
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

/// LocalObjectStorage ignores every WriteSettings cap/force-single-part field (it has no multipart
/// concept at all), so it cannot exercise the ACTUAL enforcement -- only a real WriteBufferFromS3
/// (over a mocked S3 client) can. The behavioral inversion of the old
/// "ResurrectIsNotBoundByTheSinglePutCap" contract (a resurrect on a generation store now IS bound by
/// the cap, because it is a token-producing write like any other) lives in the CASBackendGenerationS3
/// fixture below, alongside the rest of the "generation-token write kind" battery.

#if USE_AWS_S3

namespace
{

/// Minimal S3 double for the CasObjectStorageBackend generation-token write battery: just enough of
/// `DB::S3::Client` to drive a real `WriteBufferFromS3` end to end (PutObject, HeadObject).
/// CreateMultipartUpload/UploadPart/CompleteMultipartUpload are deliberately NOT overridden: every
/// test below that expects `NOT_IMPLEMENTED` relies on WriteBufferFromS3::createMultipartUpload
/// throwing BEFORE any multipart request is ever built, so those requests must never reach the wire in
/// the first place -- an unimplemented override would only matter if that invariant broke. `GetObject`
/// is likewise not overridden: reading a written body back verifies against `objects` directly (see
/// the tests below), rather than through the considerably more involved `ReadBufferFromS3` read path
/// (range/retry/prefetch machinery), which this fake does not attempt to support.
class FakeGenerationS3Client : public DB::S3::Client
{
public:
    FakeGenerationS3Client()
        : DB::S3::Client(
            100,
            DB::S3::ServerSideEncryptionKMSConfig(),
            std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("", ""),
            GetClientConfiguration(),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            DB::S3::ClientSettings{
                .use_virtual_addressing = true,
                .disable_checksum = false,
                .gcs_issue_compose_request = false,
                .is_s3express_bucket = false,
            })
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
    std::string next_put_etag = "1000";
    bool put_returns_no_etag = false;

    mutable size_t put_object_calls = 0;
    mutable size_t head_object_calls = 0;

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest & request) const override
    {
        ++put_object_calls;
        std::stringstream data;
        data << request.GetBody()->rdbuf();
        objects[request.GetKey()] = data.str();

        Aws::S3::Model::PutObjectOutcome outcome;
        Aws::S3::Model::PutObjectResult result(outcome.GetResultWithOwnership());
        if (!put_returns_no_etag)
            result.SetETag(next_put_etag);
        return result;
    }

    /// The ETag field a HeadObject response carries; empty means SetETag is never called, which is
    /// what every test written before the quoting seam was understood relied on.
    std::string next_head_etag;

    Aws::S3::Model::HeadObjectOutcome HeadObject(const Aws::S3::Model::HeadObjectRequest & request) const override
    {
        ++head_object_calls;
        Aws::S3::Model::HeadObjectOutcome outcome;
        Aws::S3::Model::HeadObjectResult result(outcome.GetResultWithOwnership());
        auto it = objects.find(request.GetKey());
        result.SetContentLength(it == objects.end() ? 0 : it->second.size());
        if (!next_head_etag.empty())
            result.SetETag(next_head_etag);
        return result;
    }

    mutable std::map<std::string, std::string> objects;
};

std::shared_ptr<DB::S3ObjectStorage> makeGenerationS3ObjectStorageForTest(FakeGenerationS3Client *& out_client)
{
    auto owned_client = std::make_unique<FakeGenerationS3Client>();
    out_client = owned_client.get();

    DB::S3::URI uri;
    uri.bucket = "cas-generation-bucket";
    DB::S3Capabilities capabilities;
    DB::ObjectStorageKeyGeneratorPtr key_generator;

    return std::make_shared<DB::S3ObjectStorage>(
        std::move(owned_client), std::make_unique<DB::S3Settings>(), std::move(uri), capabilities, key_generator, "cas-generation-disk");
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

    /// A fresh backend with the given cap, native token type forced to Generation unless overridden
    /// (the ETag dialect is needed to prove the generation-only quote handling does not touch it).
    std::shared_ptr<ObjectStorageBackend> makeBackend(uint64_t cap, TokenType token_type = TokenType::Generation)
    {
        auto storage = makeGenerationS3ObjectStorageForTest(client);
        auto b = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native, cap);
        b->setNativeTokenTypeForTest(token_type);
        return b;
    }
};

/// NOTE: putIfAbsent/casPut/putOverwrite (compare/create writes) are NOT covered end to end here.
/// conditionalWriteSettings() selects the SingleAttempt object-storage retry profile, and
/// S3ObjectStorage::writeObject resolves that to getSingleAttemptClient(), which ALWAYS constructs a
/// genuine DB::S3::Client via Client::cloneWithConfigurationOverride (pre-existing behavior, RFC
/// cas-s3-timeout-retry-control) -- discarding any derived mock's overrides. A compare/create write
/// therefore cannot be driven through a subclassed fake client this way; their settings-level
/// contract (NativeConditional mode, cap forcing) is characterized above, and their production
/// request-marking is covered by WBS3Test/S3ObjectStorageConditionalOpsTest. Resurrection uses
/// tokenProducingWriteSettings(), which does NOT select SingleAttempt, so it stays on client.get()
/// (this fixture's fake) and gets full end-to-end coverage below.

/// Trap 1's inversion target: resurrection is UNCONDITIONAL, but it is still a token-producing write,
/// so it is now bound by the very same single-PUT cap a conditional write is -- the opposite of the
/// contract this test used to pin (see the comment above this fixture).
TEST_F(CASBackendGenerationS3, ResurrectAboveSinglePutCapThrowsNotImplementedBeforeAnyPut)
{
    const String payload(1024, 'x');   /// far above a 16-byte cap
    backend = makeBackend(/*cap=*/16);
    /// Seed the condemned incarnation directly (bypassing casPut/putIfAbsent, which route through
    /// conditionalWriteSettings' SingleAttempt profile -- S3ObjectStorage::getSingleAttemptClient
    /// always clones a genuine DB::S3::Client, so it cannot be driven through this fake).
    client->objects["p/gen/res"] = "original";

    DB::ReadBufferFromOwnString in{payload};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED,
        [&] { backend->resurrect(in, payload.size(), "p/gen/res", String("HDR")); });
    EXPECT_EQ(client->put_object_calls, 0u);
}

/// The companion positive case: a resurrect body that FITS the cap (header + payload together) still
/// completes as a single PUT and is attributed the response's generation directly -- no follow-up HEAD.
TEST_F(CASBackendGenerationS3, ResurrectAtSinglePutCapUsesOnePutAndReturnsResponseGeneration)
{
    const String header = "HDR";
    const String payload(61, 'x');
    backend = makeBackend(/*cap=*/header.size() + payload.size());
    /// Seed the condemned incarnation directly -- see the comment in the above-cap test for why
    /// putIfAbsent itself cannot be used here.
    client->objects["p/gen/res-ok"] = "original";
    client->next_put_etag = "778899";

    DB::ReadBufferFromOwnString in{payload};
    const Token tok = backend->resurrect(in, payload.size(), "p/gen/res-ok", header);
    EXPECT_EQ(tok, (Token{"778899", TokenType::Generation}));
    EXPECT_EQ(client->put_object_calls, 1u);
    EXPECT_EQ(client->head_object_calls, 0u);

    /// Read the written body directly off the fake's object store rather than through
    /// `backend->get(...)`: this fake implements just enough of `DB::S3::Client` to drive the WRITE
    /// path (PutObject/HeadObject), not the considerably more involved `ReadBufferFromS3` read path
    /// (range/retry/prefetch machinery), so a full round trip through `get()` is out of scope here.
    EXPECT_EQ(client->objects.at("p/gen/res-ok"), header + payload);
}

/// Step 7: a successful PUT whose response carries no generation at all is an exception, never a
/// silently-empty token and never a follow-up HEAD.
TEST_F(CASBackendGenerationS3, ResurrectMissingGenerationOnSuccessThrows)
{
    const String payload(8, 'x');
    backend = makeBackend(/*cap=*/1024);
    client->put_returns_no_etag = true;

    DB::ReadBufferFromOwnString in{payload};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { backend->resurrect(in, payload.size(), "p/gen/no-etag", String("H")); });
}

/// Step 7: a successful PUT whose response ETag is not purely numeric (an AWS-style ETag rather than a
/// GCS generation) is likewise an exception on a generation-dialect backend.
TEST_F(CASBackendGenerationS3, ResurrectNonNumericGenerationOnSuccessThrows)
{
    const String payload(8, 'x');
    backend = makeBackend(/*cap=*/1024);
    client->next_put_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";

    DB::ReadBufferFromOwnString in{payload};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { backend->resurrect(in, payload.size(), "p/gen/bad-etag", String("H")); });
}

/// ---- The transport-quoting seam ----
///
/// A GCS generation reaches this layer through the SDK's ETag field, and the HTTP boundary fills that
/// field with an ETag-shaped, QUOTED value. Every test above this point feeds the write path an
/// UNQUOTED generation (`next_put_etag = "778899"`), and the HTTP-layer tests assert the field is
/// quoted -- each half self-consistent, neither crossing the seam between them. Nothing checked what
/// the CAS layer receives in the shape production actually produces, which is why a mount that could
/// never succeed passed every unit test. These three tests are that crossing.

/// A token-producing write whose response generation arrives quoted -- exactly what
/// `applyGcsConditionalDialectToResponse` produces -- must yield an UNQUOTED, all-digits token.
/// Before the fix this threw CORRUPTED_DATA, so every GCS CAS write failed and no pool could mount.
TEST_F(CASBackendGenerationS3, WriteGenerationTokenStripsTransportQuoting)
{
    const String header = "HDR";
    const String payload(61, 'x');
    backend = makeBackend(/*cap=*/header.size() + payload.size());
    client->objects["p/gen/quoted-write"] = "original";
    client->next_put_etag = "\"1783078552147137\"";

    DB::ReadBufferFromOwnString in{payload};
    const Token tok = backend->resurrect(in, payload.size(), "p/gen/quoted-write", header);
    EXPECT_EQ(tok, (Token{"1783078552147137", TokenType::Generation}));
}

/// The same crossing on the read side: a marked HEAD whose ETag field carries a quoted generation
/// must mint the same unquoted token, so a token observed by HEAD compares equal to one returned by
/// the write that created it.
TEST_F(CASBackendGenerationS3, HeadGenerationTokenStripsTransportQuoting)
{
    backend = makeBackend(/*cap=*/1024);
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
    backend = makeBackend(/*cap=*/1024, TokenType::ETag);
    client->objects["p/etag/quoted-head"] = "body";
    client->next_head_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";

    const auto hr = backend->head("p/etag/quoted-head");
    ASSERT_TRUE(hr.exists);
    EXPECT_EQ(hr.token, (Token{"\"d41d8cd98f00b204e9800998ecf8427e\"", TokenType::ETag}));
}

/// The HEAD-side twin of ResurrectMissingGenerationOnSuccessThrows: a successful HEAD on a
/// generation-dialect backend whose response carries no ETag/generation at all must not mint a token
/// from it -- there is no follow-up HEAD to patch this over, so nativeHead must refuse it directly.
TEST_F(CASBackendGenerationS3, HeadMissingGenerationThrows)
{
    backend = makeBackend(/*cap=*/1024);
    client->objects["p/gen/no-generation-head"] = "body";
    /// next_head_etag stays empty: SetETag is never called, so the response carries no ETag field.

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { backend->head("p/gen/no-generation-head"); });
}

/// The HEAD-side twin of ResurrectNonNumericGenerationOnSuccessThrows: an ordinary AWS-style ETag
/// reaching a generation-dialect backend through a successful HEAD (a proxy dropping
/// x-goog-generation, a service regression) must not be minted as a generation token either.
TEST_F(CASBackendGenerationS3, HeadNonNumericGenerationThrows)
{
    backend = makeBackend(/*cap=*/1024);
    client->objects["p/gen/bad-etag-head"] = "body";
    client->next_head_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { backend->head("p/gen/bad-etag-head"); });
}

#endif
