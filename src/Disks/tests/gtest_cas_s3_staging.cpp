#include <gtest/gtest.h>
#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <IO/WriteSettings.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskCommitTransactionOptions.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Common/SipHash.h>

#include <Poco/AutoPtr.h>
#include <Poco/Exception.h>
#include <Poco/Util/XMLConfiguration.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <mutex>
#include <sstream>
#include <string>

#include "config.h"
#if USE_AWS_S3
#include <IO/S3Common.h>
#include <aws/s3/S3Errors.h>
#endif

/// `staging_backend` defaults to `local`; explicit `s3` selection requires native-copy capability
/// on writable mounts.

namespace DB::ContentAddressedSetting
{
    extern const ContentAddressedSettingsString staging_backend;
}

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

/// Build a `Poco::Util::XMLConfiguration` with `inner_xml` nested under a `<disk>` element (mirrors
/// the shape a real CAS disk config has under `storage_configuration.disks.<name>`, so
/// `config_prefix = "disk"` reads exactly like the disk factory's `config_prefix`).
Poco::AutoPtr<Poco::Util::XMLConfiguration> configWithDiskSection(const std::string & inner_xml)
{
    std::istringstream xml_stream( // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        "<clickhouse><disk>" + inner_xml + "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(xml_stream);
}

/// A local test store whose copy-mode capability is configurable. Its ordinary `copyObject`
/// implementation remains the real local implementation; only the advertised transport capability
/// differs so mount selection can be tested independently of a live S3 service.
class FakeNativeCopyObjectStorage final : public DB::LocalObjectStorage
{
public:
    FakeNativeCopyObjectStorage(DB::LocalObjectStorageSettings settings_, bool native_only_copy_supported_)
        : DB::LocalObjectStorage(std::move(settings_))
        , native_only_copy_supported(native_only_copy_supported_)
    {
    }

    bool supportsCopyMode(DB::ObjectStorageCopyMode mode) const override
    {
        return mode == DB::ObjectStorageCopyMode::Default
            || (mode == DB::ObjectStorageCopyMode::NativeOnly && native_only_copy_supported);
    }

private:
    const bool native_only_copy_supported;
};

std::shared_ptr<FakeNativeCopyObjectStorage> makeFakeNativeCopyStorage(bool native_only_copy_supported)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_s3_staging_native_copy_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<FakeNativeCopyObjectStorage>(std::move(settings), native_only_copy_supported);
}

/// A fake object-store sink for Task 4 of the S3-native staging plan (`DB::Cas::CaContentWriteBuffer`'s
/// S3-staging constructor): an in-memory `WriteBufferFromFileBase` that records every byte written to
/// it, plus whether `cancelImpl`/`finalizeImpl` ran. This is enough to prove the S3-staging mode
/// streams to the SINK (not to a local temp file) while hashing, without needing a real object storage
/// — the end-to-end wiring (`writeFile` choosing this mode, the promote path) lands in later tasks.
class FakeStagingSink : public DB::WriteBufferFromFileBase
{
public:
    explicit FakeStagingSink(std::string key_)
        : DB::WriteBufferFromFileBase(/*buf_size=*/8192, nullptr, 0), key(std::move(key_))
    {
    }

    void sync() override {}
    std::string getFileName() const override { return key; }

    const std::string & writtenBytes() const { return written; }
    bool wasCancelled() const { return cancelled; }
    bool wasFinalizedForTest() const { return did_finalize; }

protected:
    void nextImpl() override
    {
        if (!offset())
            return;
        written.append(working_buffer.begin(), offset());
    }

    void finalizeImpl() override
    {
        next();
        did_finalize = true;
    }

    void cancelImpl() noexcept override
    {
        cancelled = true;
    }

private:
    std::string key;
    std::string written;
    bool cancelled = false;
    bool did_finalize = false;
};

/// Records whether each unconditional publication used verbatim native copy or a retagged stream.
/// Stream reads are counted separately so condemned-destination tests can prove they read only the
/// writer-owned staging object.
class RecordingStagingBackend : public DB::Cas::InMemoryBackend
{
public:
    struct CopyCall
    {
        std::string from;
        std::string to;
        bool server_side_copy;
    };

    std::vector<CopyCall> copy_calls;

    void publishBlob(const DB::Cas::BlobPublishRequest & request) override
    {
        if (const auto * copy = std::get_if<DB::Cas::VerbatimStagedBlobPublication>(&request.publication))
            copy_calls.push_back({copy->object_key, request.destination_key, true});
        else
            copy_calls.push_back({String{}, request.destination_key, false});
        DB::Cas::InMemoryBackend::publishBlob(request);
    }

    /// Every key read as a stream, with a count. Republishing opens its source with `getStream`, so
    /// this counts exactly those reads -- and deliberately not the materializing
    /// `get`, which the assertions themselves use to inspect bodies.
    std::map<String, size_t> reads_of;

    using DB::Cas::InMemoryBackend::getStream;
    std::optional<DB::Cas::GetStreamResult> getStream(const String & key, DB::Cas::Range range) override
    {
        ++reads_of[key];
        return DB::Cas::InMemoryBackend::getStream(key, range);
    }


    size_t streamingPublicationCount() const
    {
        size_t n = 0;
        for (const CopyCall & c : copy_calls)
            n += c.server_side_copy ? 0 : 1;
        return n;
    }
};

/// Models an ETag store faithfully enough for the staged-envelope regressions: a blob token is a
/// deterministic digest of the complete object bytes, so copying the same staging object again would
/// reproduce the same token. Each script injects a different ambiguity transition from the design.
class EtagFaithfulPublicationBackend final : public DB::Cas::InMemoryBackend
{
public:
    enum class FaultScript : uint8_t
    {
        CopyLandsThenCondemned,
        CopyLandsThenDeletedBeforeAbsentRetry,
        FirstCondemnedStreamLandsThenDeleted,
    };

    explicit EtagFaithfulPublicationBackend(FaultScript script_) : script(script_) {}

    DB::Cas::HeadResult head(const String & key) override
    {
        DB::Cas::HeadResult result = DB::Cas::InMemoryBackend::head(key);
        if (result.exists && isBlobBodyKey(key))
        {
            const auto body = DB::Cas::InMemoryBackend::get(key);
            chassert(body.has_value());
            result.token = DB::Cas::Token{sipHash128String(body->bytes), DB::Cas::TokenType::ETag};
        }
        return result;
    }

    DB::Cas::DeleteOutcome deleteExact(const String & key, const DB::Cas::Token & token) override
    {
        if (!isBlobBodyKey(key))
            return DB::Cas::InMemoryBackend::deleteExact(key, token);

        const DB::Cas::HeadResult current = head(key);
        if (!current.exists)
            return DB::Cas::DeleteOutcome{.kind = DB::Cas::DeleteOutcome::Kind::NotFound};
        if (current.token != token)
            return DB::Cas::DeleteOutcome{.kind = DB::Cas::DeleteOutcome::Kind::TokenMismatch};
        return DB::Cas::InMemoryBackend::deleteExact(key, DB::Cas::InMemoryBackend::head(key).token);
    }

    void publishBlob(const DB::Cas::BlobPublishRequest & request) override
    {
        const bool is_copy = std::holds_alternative<DB::Cas::VerbatimStagedBlobPublication>(request.publication);
        if (is_copy)
            ++copy_publications;
        else
            ++streaming_publications;

        if (!fault_fired
            && ((script == FaultScript::CopyLandsThenCondemned && is_copy)
                || (script == FaultScript::CopyLandsThenDeletedBeforeAbsentRetry && is_copy)
                || (script == FaultScript::FirstCondemnedStreamLandsThenDeleted && !is_copy)))
        {
            fault_fired = true;
            DB::Cas::InMemoryBackend::publishBlob(request);
            queued_delete_token = head(request.destination_key).token;

            if (script != FaultScript::CopyLandsThenCondemned)
                first_delete = deleteExact(request.destination_key, queued_delete_token);

            throw Poco::TimeoutException("ETag-faithful staged publication response lost");
        }

        DB::Cas::InMemoryBackend::publishBlob(request);
    }

    FaultScript script;
    bool fault_fired = false;
    size_t copy_publications = 0;
    size_t streaming_publications = 0;
    DB::Cas::Token queued_delete_token;
    DB::Cas::DeleteOutcome first_delete;

private:
    static bool isBlobBodyKey(const String & key)
    {
        return key.find("/blobs/") != String::npos && !key.ends_with(".meta");
    }
};

DB::Cas::PoolPtr openStagingPool(const std::shared_ptr<RecordingStagingBackend> & b)
{
    return DB::Cas::Pool::open(b, DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// A build whose owning manifest namespace / final ref name are `ns`/`ref` (mirrors gtest_cas_build's
/// `startBuildFor`: promote/stageManifest derive the namespace by splitting `intended_ref` on the LAST '/').
DB::Cas::PartWriteTxnPtr startStagingBuild(const DB::Cas::PoolPtr & s, const DB::Cas::RootNamespace & ns, const String & ref)
{
    DB::Cas::PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    return s->beginPartWrite(info);
}

/// Stage a one-blob manifest and precommit it (so the EDGE-BEFORE-OBSERVE fail-closed check in
/// `PartWriteTxn::ensureBlobPresent` holds), returning the build ready for `putBlob` on `hash`.
DB::Cas::PartWriteTxnPtr precommittedBuildFor(
    const DB::Cas::PoolPtr & s, const DB::Cas::RootNamespace & ns, const String & ref,
    const DB::UInt128 & hash, uint64_t blob_size)
{
    DB::Cas::PartWriteTxnPtr build = startStagingBuild(s, ns, ref);
    const DB::Cas::ManifestId id = build->stageManifest({DB::Cas::tests::blobEntryFor("col.bin", hash, blob_size)});
    build->precommitAdd(ns, ref, id);
    return build;
}

DB::Cas::BlobSource reReadableStagedSource(
    const DB::Cas::BackendPtr & backend, const std::string & staging_key, uint64_t payload_size, uint64_t header_len)
{
    DB::Cas::BlobSource source;
    source.size = payload_size;
    source.server_side_copy_from = staging_key;
    source.open = [backend, staging_key, header_len]() -> std::unique_ptr<DB::ReadBuffer>
    {
        auto staged = backend->getStream(staging_key);
        if (!staged)
            throw DB::Exception(DB::ErrorCodes::FILE_DOESNT_EXIST, "staging object {} is absent", staging_key);

        String encoded_header(header_len, '\0');
        staged->stream->readStrict(encoded_header.data(), encoded_header.size());
        const DB::Cas::EnvelopeHeader decoded
            = DB::Cas::decodeEnvelopeHeader(encoded_header, encoded_header.size(), DB::Cas::ObjectKind::Blob);
        if (decoded.header_len != header_len)
            throw DB::Exception(
                DB::ErrorCodes::CORRUPTED_DATA,
                "staging object {} uses envelope length {}, expected {}",
                staging_key,
                decoded.header_len,
                header_len);
        return std::move(staged->stream);
    };
    return source;
}

String stagedBytes(uint64_t header_len, const String & payload, DB::UInt128 tag)
{
    DB::Cas::EnvelopeHeader header;
    header.kind = DB::Cas::ObjectKind::Blob;
    header.incarnation_tag = tag;
    return DB::Cas::encodeEnvelopeHeader(header, static_cast<uint32_t>(header_len)) + payload;
}

}

TEST(CASS3Staging, StagedCopyCondemnedRetryRetagsBeforeQueuedDelete)
{
    auto backend = std::make_shared<EtagFaithfulPublicationBackend>(
        EtagFaithfulPublicationBackend::FaultScript::CopyLandsThenCondemned);
    auto store = DB::Cas::Pool::open(
        backend, DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const String payload = "etag-copy-condemned-retry";
    const DB::Cas::BlobRef ref = DB::Cas::tests::idOf(payload);
    const String staging_key = "p/staging/mount1/etag-condemned.tmp";
    const String staging_bytes = stagedBytes(store->poolMeta().blob_header_len, payload, DB::UInt128{101});
    backend->putIfAbsent(staging_key, staging_bytes);
    DB::Cas::tests::writeMetaClean(*backend, store->layout(), DB::Cas::tests::u128Of(payload), payload.size());
    DB::Cas::tests::condemnMeta(*backend, store->layout(), DB::Cas::tests::u128Of(payload), 31);
    auto build = precommittedBuildFor(
        store, DB::Cas::RootNamespace{"srv1/etag-condemned"}, "part",
        DB::Cas::tests::u128Of(payload), payload.size());

    build->putBlob(
        ref,
        reReadableStagedSource(backend, staging_key, payload.size(), store->poolMeta().blob_header_len));

    EXPECT_EQ(backend->copy_publications, 1u);
    EXPECT_EQ(backend->streaming_publications, 1u);
    EXPECT_EQ(
        backend->deleteExact(store->layout().blobKey(ref), backend->queued_delete_token).kind,
        DB::Cas::DeleteOutcome::Kind::TokenMismatch);
    const auto current = backend->get(store->layout().blobKey(ref));
    ASSERT_TRUE(current.has_value());
    EXPECT_NE(current->bytes, staging_bytes);
    EXPECT_EQ(current->bytes.substr(store->poolMeta().blob_header_len), payload);
}

TEST(CASS3Staging, StagedCopyDeletedBeforeAbsentRetryRetagsBeforeQueuedDelete)
{
    auto backend = std::make_shared<EtagFaithfulPublicationBackend>(
        EtagFaithfulPublicationBackend::FaultScript::CopyLandsThenDeletedBeforeAbsentRetry);
    auto store = DB::Cas::Pool::open(
        backend, DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const String payload = "etag-copy-deleted-before-retry";
    const DB::Cas::BlobRef ref = DB::Cas::tests::idOf(payload);
    const String staging_key = "p/staging/mount1/etag-deleted.tmp";
    const String staging_bytes = stagedBytes(store->poolMeta().blob_header_len, payload, DB::UInt128{202});
    backend->putIfAbsent(staging_key, staging_bytes);
    auto build = precommittedBuildFor(
        store, DB::Cas::RootNamespace{"srv1/etag-deleted"}, "part",
        DB::Cas::tests::u128Of(payload), payload.size());

    build->putBlob(
        ref,
        reReadableStagedSource(backend, staging_key, payload.size(), store->poolMeta().blob_header_len));

    EXPECT_EQ(backend->first_delete.kind, DB::Cas::DeleteOutcome::Kind::Deleted);
    EXPECT_EQ(backend->copy_publications, 1u)
        << "the absent retry must not copy the original staged envelope again";
    EXPECT_EQ(backend->streaming_publications, 1u);
    EXPECT_EQ(
        backend->deleteExact(store->layout().blobKey(ref), backend->queued_delete_token).kind,
        DB::Cas::DeleteOutcome::Kind::TokenMismatch)
        << "the second queued exact delete for the copied ETag must miss the retagged replacement";
    const auto current = backend->get(store->layout().blobKey(ref));
    ASSERT_TRUE(current.has_value());
    EXPECT_NE(current->bytes, staging_bytes);
    EXPECT_EQ(current->bytes.substr(store->poolMeta().blob_header_len), payload);
}

TEST(CASS3Staging, FirstCondemnedAttemptThenAbsentRetryNeverRecopies)
{
    auto backend = std::make_shared<EtagFaithfulPublicationBackend>(
        EtagFaithfulPublicationBackend::FaultScript::FirstCondemnedStreamLandsThenDeleted);
    auto store = DB::Cas::Pool::open(
        backend, DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const String payload = "etag-first-condemned-then-absent";
    const DB::Cas::BlobRef ref = DB::Cas::tests::idOf(payload);
    const String staging_key = "p/staging/mount1/etag-first-condemned.tmp";
    const String staging_bytes = stagedBytes(store->poolMeta().blob_header_len, payload, DB::UInt128{303});
    backend->putIfAbsent(staging_key, staging_bytes);
    backend->putIfAbsent(store->layout().blobKey(ref), staging_bytes);
    DB::Cas::tests::writeMetaClean(*backend, store->layout(), DB::Cas::tests::u128Of(payload), payload.size());
    DB::Cas::tests::condemnMeta(*backend, store->layout(), DB::Cas::tests::u128Of(payload), 37);
    const DB::Cas::Token original_staged_etag = backend->head(store->layout().blobKey(ref)).token;
    auto build = precommittedBuildFor(
        store, DB::Cas::RootNamespace{"srv1/etag-first-condemned"}, "part",
        DB::Cas::tests::u128Of(payload), payload.size());

    build->putBlob(
        ref,
        reReadableStagedSource(backend, staging_key, payload.size(), store->poolMeta().blob_header_len));

    EXPECT_EQ(backend->first_delete.kind, DB::Cas::DeleteOutcome::Kind::Deleted);
    EXPECT_EQ(backend->copy_publications, 0u)
        << "a first condemned publication and every later absent retry must stream, never copy";
    EXPECT_EQ(backend->streaming_publications, 2u);
    EXPECT_EQ(
        backend->deleteExact(store->layout().blobKey(ref), original_staged_etag).kind,
        DB::Cas::DeleteOutcome::Kind::TokenMismatch);
    const auto current = backend->get(store->layout().blobKey(ref));
    ASSERT_TRUE(current.has_value());
    EXPECT_NE(current->bytes, staging_bytes);
    EXPECT_EQ(current->bytes.substr(store->poolMeta().blob_header_len), payload);
}

TEST(CASS3Staging, ParsesS3BackendFromConfig)
{
    auto config = configWithDiskSection("<staging_backend>s3</staging_backend>");

    EXPECT_EQ(DB::ContentAddressedMetadataStorage::parseStagingBackend(*config, "disk"), DB::Cas::StagingBackend::S3);
}

TEST(CASS3Staging, DefaultConfigParsesToLocalBackend)
{
    /// No `staging_backend` key at all — the OFF BY DEFAULT arm.
    auto config = configWithDiskSection("<scratch_path>/tmp/whatever</scratch_path>");

    EXPECT_EQ(DB::ContentAddressedMetadataStorage::parseStagingBackend(*config, "disk"), DB::Cas::StagingBackend::Local);
}

TEST(CASS3Staging, UnknownBackendValueThrows)
{
    auto config = configWithDiskSection("<staging_backend>nfs</staging_backend>");
    EXPECT_THROW(DB::ContentAddressedMetadataStorage::parseStagingBackend(*config, "disk"), DB::Exception);
}

TEST(CASS3Staging, DefaultConstructedStorageReportsLocal)
{
    /// Constructed with no staging-related args at all (mirrors the existing gtest call sites, e.g.
    /// gtest_ca_wiring.cpp, which stop at `context_`): the accessors must reflect the same
    /// byte-for-byte-current-behavior defaults the config parser produces above.
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "cas_s3_staging_default_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);

    EXPECT_EQ(storage->stagingBackend(), DB::Cas::StagingBackend::Local);
}

TEST(CASS3Staging, DefaultObjectStorageRejectsNativeOnlyCopyMode)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();

    EXPECT_TRUE(storage->supportsCopyMode(DB::ObjectStorageCopyMode::Default));
    EXPECT_FALSE(storage->supportsCopyMode(DB::ObjectStorageCopyMode::NativeOnly));
}

/// Task 4 of the S3-native staging plan: `CaContentWriteBuffer`'s S3-staging constructor streams
/// directly to an already-opened object-store sink while hashing, instead of spilling to a local temp
/// file (see the constructor's doc comment in ContentAddressedWriteBuffers.h). These two tests
/// exercise the buffer directly over a `FakeStagingSink` — no real object storage, disk, or
/// `ContentAddressedTransaction` needed; `writeFile` choosing this mode is exercised together with the
/// promote path in later tasks (S3 mode is off by default and not enabled by any existing test).

TEST(CASS3Staging, ContentWriteBufferS3ModeStreamsToSinkAndFinalizes)
{
    const std::string staging_key = "staging/mount1/abc123.tmp";
    auto * sink_ptr = new FakeStagingSink(staging_key);
    std::unique_ptr<DB::WriteBufferFromFileBase> sink(sink_ptr);

    std::string got_hash_hex;
    size_t got_size = 0;
    std::string got_key;
    int on_finalized_calls = 0;

    /// S3-native staging fix 2026-07-11: the S3 constructor takes a fixed-length envelope header that is
    /// written to the sink FIRST, UNHASHED and excluded from the reported size. A distinctive 256-byte
    /// filler stands in for the real CABL header here (this test exercises the buffer mechanics, not the
    /// envelope encoder).
    const std::string envelope_header(256, 'H');

    auto buf = std::make_unique<DB::Cas::CaContentWriteBuffer>(
        std::move(sink),
        staging_key,
        envelope_header,
        DB::Cas::BlobHashAlgo::CityHash128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string & hash_hex, size_t size, const std::string & key)
        {
            ++on_finalized_calls;
            got_hash_hex = hash_hex;
            got_size = size;
            got_key = key;
        });

    /// Write in two chunks (exercises more than one nextImpl flush) and finalize.
    const std::string payload_part1(4000, 'x');
    const std::string payload_part2(1234, 'y');
    buf->write(payload_part1.data(), payload_part1.size());
    buf->write(payload_part2.data(), payload_part2.size());
    buf->finalize();

    const std::string payload = payload_part1 + payload_part2;

    /// (a) the sink received the ENVELOPE HEADER FIRST, then EXACTLY the payload bytes — the staging
    /// object holds `[header][payload]` so the promote can stay a verbatim server-side copy.
    EXPECT_EQ(sink_ptr->writtenBytes(), envelope_header + payload);
    EXPECT_TRUE(sink_ptr->wasFinalizedForTest());
    EXPECT_FALSE(sink_ptr->wasCancelled());

    /// (b) on_finalized fired exactly once with the correct cityHash128 hex, size, and staging key.
    /// The pool-wide content hash is the STREAMING `HashingWriteBuffer` convention (chunked
    /// cityHash128, block = 2048 B), which diverges from a one-shot `CityHash_v1_0_2::CityHash128`
    /// call for a payload spanning more than one block (see `gtest_cas_part_write.cpp`'s
    /// `CopyForwardMultiBlockPayloadVerifies`, which documents and exercises the same divergence).
    /// This payload (5234 bytes) spans multiple 2048-byte blocks, so the expected hash must be
    /// recomputed with the SAME streaming convention via `HashingReadBuffer`, not a one-shot call.
    DB::ReadBufferFromMemory expected_in(payload.data(), payload.size());
    DB::HashingReadBuffer expected_hashing(expected_in);
    expected_hashing.ignoreAll();
    const std::string expected_hash_hex = getHexUIntLowercase(expected_hashing.getHash());
    EXPECT_EQ(on_finalized_calls, 1);
    EXPECT_EQ(got_hash_hex, expected_hash_hex);
    EXPECT_EQ(got_size, payload.size());
    EXPECT_EQ(got_key, staging_key);
    EXPECT_EQ(buf->getFileName(), staging_key);
}

TEST(CASS3Staging, ContentWriteBufferS3ModeCancelCancelsSinkAndSkipsFinalize)
{
    const std::string staging_key = "staging/mount1/cancelled.tmp";
    auto * sink_ptr = new FakeStagingSink(staging_key);
    std::unique_ptr<DB::WriteBufferFromFileBase> sink(sink_ptr);

    bool on_finalized_called = false;

    auto buf = std::make_unique<DB::Cas::CaContentWriteBuffer>(
        std::move(sink),
        staging_key,
        /*envelope_header=*/std::string(256, 'H'),
        DB::Cas::BlobHashAlgo::CityHash128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string &, size_t, const std::string &)
        {
            on_finalized_called = true;
        });

    const std::string payload = "some bytes that must never be promoted";
    buf->write(payload.data(), payload.size());
    buf->cancel();

    /// (c) cancel() before finalize cancels the sink and on_finalized is NEVER called — no partial
    /// finalize (no promote-worthy hash/size is ever handed to the transaction for cancelled bytes).
    EXPECT_TRUE(sink_ptr->wasCancelled());
    EXPECT_FALSE(sink_ptr->wasFinalizedForTest());
    EXPECT_FALSE(on_finalized_called);

    /// The buffer's destructor calls cancel() again (defensive backstop) — already-cancelled, so this
    /// must stay a no-op: still no on_finalized call, and no attempt to fs::remove a remote key.
    buf.reset();
    EXPECT_FALSE(on_finalized_called);
}

/// The ordinary staged cases below pin the same mandatory-`HEAD` selection used by the ETag-faithful
/// regressions: native verbatim copy only after a first absent observation, no publication for a live
/// body, and retagged streaming for `Condemned`.

/// (a) Fresh blob key ⇒ the first-plus-absent native copy publishes verbatim and records `Materialized`.
TEST(CASS3Staging, PromoteViaServerSideCopyCreatesFreshBlobMaterializedProof)
{
    auto backend = std::make_shared<RecordingStagingBackend>();
    auto store = openStagingPool(backend);
    const DB::Cas::RootNamespace ns{"srv1/nsA"};
    const std::string ref = "part_a";

    const std::string payload(300, 'a');
    const DB::UInt128 hash = DB::Cas::tests::u128Of(payload);
    const DB::Cas::BlobRef blob_id{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)};
    const std::string blob_key = store->layout().blobKey(blob_id);
    const std::string staging_key = "p/staging/mount1/aaa.tmp";
    const std::string staging_bytes = stagedBytes(
        store->poolMeta().blob_header_len, payload, DB::UInt128{0xA});
    backend->putIfAbsent(staging_key, staging_bytes);

    auto build = precommittedBuildFor(store, ns, ref, hash, payload.size());
    const DB::Cas::PutBlobResult bref = build->putBlob(
        blob_id,
        reReadableStagedSource(backend, staging_key, payload.size(), store->poolMeta().blob_header_len));

    /// Exactly one native verbatim publication from staging to the blob key.
    ASSERT_EQ(backend->copy_calls.size(), 1u);
    EXPECT_TRUE(backend->copy_calls[0].server_side_copy);
    EXPECT_EQ(backend->copy_calls[0].from, staging_key);
    EXPECT_EQ(backend->copy_calls[0].to, blob_key);
    EXPECT_EQ(backend->streamingPublicationCount(), 0u);

    /// Successful publication records materialized evidence; the backend still owns the destination token.
    EXPECT_EQ(build->dependencyProof(blob_id), DB::Cas::BlobDependencyProof::Materialized);
    const DB::Cas::HeadResult hr = backend->head(blob_key);
    ASSERT_TRUE(hr.exists);
    EXPECT_FALSE(hr.token.empty());
    EXPECT_EQ(bref.size, payload.size());

    /// The promoted blob body IS the staging bytes (server-side copy moved them verbatim).
    const auto got = backend->get(blob_key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, staging_bytes);
}

/// (b) Blob key already exists and is `Clean` ⇒ the writer observes it without publication.
TEST(CASS3Staging, PromoteOverExistingCleanBlobAdoptsAndNeverOverwrites)
{
    auto backend = std::make_shared<RecordingStagingBackend>();
    auto store = openStagingPool(backend);
    const DB::Cas::RootNamespace ns{"srv1/nsB"};
    const std::string ref = "part_b";

    const std::string payload(300, 'b');
    const DB::UInt128 hash = DB::Cas::tests::u128Of(payload);
    const DB::Cas::BlobRef blob_id{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)};
    const std::string blob_key = store->layout().blobKey(blob_id);
    const std::string staging_key = "p/staging/mount1/bbb.tmp";
    backend->putIfAbsent(
        staging_key,
        stagedBytes(store->poolMeta().blob_header_len, payload, DB::UInt128{0xB}));

    /// A pre-existing, well-formed, CLEAN blob (envelope + payload) already at the content key.
    backend->putIfAbsent(
        blob_key,
        stagedBytes(store->poolMeta().blob_header_len, payload, DB::UInt128{0xBB}));
    DB::Cas::tests::writeMetaClean(*backend, store->layout(), hash, payload.size());
    const DB::Cas::HeadResult before = backend->head(blob_key);
    ASSERT_TRUE(before.exists);

    auto build = precommittedBuildFor(store, ns, ref, hash, payload.size());
    build->putBlob(
        blob_id,
        reReadableStagedSource(backend, staging_key, payload.size(), store->poolMeta().blob_header_len));

    /// Mandatory `HEAD` observes the live body, so no transport call is made.
    EXPECT_TRUE(backend->copy_calls.empty());
    EXPECT_EQ(backend->streamingPublicationCount(), 0u);

    /// The existing incarnation is untouched: same token, same bytes.
    const DB::Cas::HeadResult after = backend->head(blob_key);
    EXPECT_EQ(after.token, before.token);

    /// Observing the existing incarnation records materialized evidence without retaining its token.
    EXPECT_EQ(build->dependencyProof(blob_id), DB::Cas::BlobDependencyProof::Materialized);
}

/// (c) Blob key exists but is CONDEMNED ⇒ the writer republishes its OWN staging PAYLOAD
/// under a FRESH-tagged envelope header — NEVER a read/copy of the condemned blob key
/// and the replacement body DIFFERS from the condemned incarnation
/// (INV-NO-RETURN: a verbatim copy would reproduce identical bytes ⇒ identical ETag ⇒ the queued
/// exact-token delete of the condemned incarnation would kill the live resurrection = data loss).
TEST(CASS3Staging, PublishOverCondemnedBlobUsesFreshTagNotVerbatim)
{
    auto backend = std::make_shared<RecordingStagingBackend>();
    auto store = openStagingPool(backend);
    const DB::Cas::RootNamespace ns{"srv1/nsC"};
    const std::string ref = "part_c";

    const std::string payload(300, 'c');
    const DB::UInt128 hash = DB::Cas::tests::u128Of(payload);
    const DB::Cas::BlobRef blob_id{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)};
    const std::string blob_key = store->layout().blobKey(blob_id);
    const std::string staging_key = "p/staging/mount1/ccc.tmp";

    /// The staging object holds `[header][payload]` (as `writeFile` now emits it). The staging header is
    /// a fixed 256-byte CABL envelope with its OWN incarnation_tag.
    DB::Cas::EnvelopeHeader staging_h;
    staging_h.kind = DB::Cas::ObjectKind::Blob;
    staging_h.incarnation_tag = DB::UInt128(0xC0FFEE);   /// the create-time tag
    const std::string staging_header = DB::Cas::encodeEnvelopeHeader(
        staging_h, static_cast<uint32_t>(store->poolMeta().blob_header_len));
    ASSERT_EQ(staging_header.size(), store->poolMeta().blob_header_len);
    const std::string staging_bytes = staging_header + payload;
    backend->putIfAbsent(staging_key, staging_bytes);

    /// Seed the condemned blob body = EXACTLY what a verbatim promote of this staging object would have
    /// produced (the writer's OWN create, later observed condemned). This is the adversarial shape: a
    /// verbatim republication WOULD reproduce these identical bytes ⇒ identical ETag ⇒ collision.
    backend->putIfAbsent(blob_key, staging_bytes);
    DB::Cas::tests::writeMetaClean(*backend, store->layout(), hash, /*size=*/payload.size());
    DB::Cas::tests::condemnMeta(*backend, store->layout(), hash, /*condemn_round=*/5);
    const DB::Cas::HeadResult before = backend->head(blob_key);
    ASSERT_TRUE(before.exists);

    auto build = precommittedBuildFor(store, ns, ref, hash, payload.size());
    build->putBlob(
        blob_id,
        reReadableStagedSource(backend, staging_key, payload.size(), store->poolMeta().blob_header_len));

    /// A present `Condemned` destination selects exactly one retagged streaming publication. It never
    /// attempts the verbatim-copy transport.
    ASSERT_EQ(backend->copy_calls.size(), 1u);
    EXPECT_FALSE(backend->copy_calls[0].server_side_copy);
    EXPECT_EQ(backend->copy_calls[0].to, blob_key);
    /// INV: republication reads the STAGING object and never the condemned blob key. Asserted on the
    /// reads themselves rather than on a source argument, because the caller now opens the reader.
    EXPECT_GT(backend->reads_of[staging_key], 0u) << "republication must read the writer's own staging object";
    EXPECT_EQ(backend->reads_of[blob_key], 0u) << "the condemned blob key must never be read";
    EXPECT_EQ(backend->streamingPublicationCount(), 1u);

    /// The incarnation token is REFRESHED (a fresh incarnation displaced the condemned one).
    const DB::Cas::HeadResult after = backend->head(blob_key);
    EXPECT_NE(after.token, before.token);
    ASSERT_TRUE(after.exists);

    const auto got = backend->get(blob_key);
    ASSERT_TRUE(got.has_value());
    const uint64_t header_len = store->poolMeta().blob_header_len;

    /// INV-NO-RETURN — THE fresh-tag property: the replacement body is NOT byte-identical to the
    /// condemned incarnation (a verbatim copy would have been). The PAYLOAD is preserved exactly (the
    /// writer read it from OUR staging object, skipping the staging header), but the envelope HEADER
    /// differs — the writer minted a FRESH incarnation_tag — so on a real content-addressed store the
    /// replacement ETag differs and the queued exact-token delete of the condemned incarnation cannot
    /// match the live replacement.
    EXPECT_NE(got->bytes, staging_bytes);
    ASSERT_GE(got->bytes.size(), header_len);
    EXPECT_EQ(got->bytes.substr(header_len), payload);              /// payload preserved
    EXPECT_NE(got->bytes.substr(0, header_len), staging_header);    /// header freshly re-tagged

    /// The republication recorded materialized evidence and flipped the meta back to `Clean`.
    EXPECT_EQ(build->dependencyProof(blob_id), DB::Cas::BlobDependencyProof::Materialized);
    const auto lm = DB::Cas::tests::loadMetaForTest(*backend, store->layout(), hash);
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(lm->meta.state, DB::Cas::MetaState::Clean);
}

/// ===========================================================================================
/// Task 6 of the S3-native staging plan: staging cleanup after commit, read-your-writes over an S3
/// pending blob, and the mount-lease-scoped sweeper (`CASStagingSweeper.h`).
///
/// The wiring-level tests below drive the real metadata storage and transaction over a local test
/// store that advertises native copy. Its real `getType` stays `Local`, so the CAS core uses
/// `EmulatedSingleProcess`; these cases stop before a native-mode staged publication is required.

namespace
{

/// Construct a `ContentAddressedMetadataStorage` with `staging_backend=s3` over `object_storage`,
/// mirroring `DefaultConstructedStorageReportsLocal`'s settings defaults for every
/// field this test suite does not care about — only `server_root_id` (the mount identity that names
/// the staging prefix) and `staging_backend` differ.
std::shared_ptr<DB::ContentAddressedMetadataStorage> makeS3StagingMetadataStorageForTest(
    const DB::ObjectStoragePtr & object_storage, const std::string & server_root_id)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto scratch = std::filesystem::temp_directory_path()
        / ("cas_s3_staging_wiring_" + server_root_id + "_" + unique);
    auto settings = DB::Cas::tests::makeSettingsForTest(server_root_id, scratch);
    settings[DB::ContentAddressedSetting::staging_backend] = "s3";
    settings.validate();
    return std::make_shared<DB::ContentAddressedMetadataStorage>(
        object_storage, "pool", "srv1", /*disk_name_=*/"", /*context_=*/nullptr, settings);
}

/// Mirrors gtest_ca_wiring.cpp's helper of the same shape.
void writeThroughS3Transaction(DB::ContentAddressedTransaction & tx, const std::string & path, const std::string & bytes)
{
    auto buf = tx.writeFile(path, 65536, DB::WriteMode::Rewrite, {});
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
}

}

TEST(CASS3Staging, WritableS3StagingRequiresNativeOnlyCopy)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountNative");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    writeThroughS3Transaction(
        ca_tx,
        "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin",
        "native-staging");

    DB::RelativePathsWithMetadata staged;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged, /*max_keys=*/0);
    EXPECT_EQ(staged.size(), 1u);
}

TEST(CASS3Staging, UnsupportedNativeOnlyCopyDoesNotFallBackToLocal)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/false);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountUnsupported");

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED, [&]
    {
        metadata_storage->startup();
    });
}

/// (a) A successful commit removes the S3 staging object of a pending blob it staged. Uses the B189
/// orphan shape (the pending blob's entry is unlinked before commit) so `publishStaging` never calls
/// `putBlob` for it — only `cleanupPendingTempFiles`'s Task 6 branch ever touches this staging object,
/// which is exactly the seam this test targets.
TEST(CASS3Staging, SuccessfulCommitRemovesOrphanedS3StagingObject)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountA");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);

    /// orphan.bin forces the S3-staging blob path (a ".bin" suffix always stays a blob, per
    /// `partFileMustStayBlob`); it is unlinked below before commit.
    writeThroughS3Transaction(ca_tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/orphan.bin", std::string(300, 'x'));
    /// checksums.txt is small and NOT blob-forcing: an INLINE entry that gives the part's PartWriteTxn a real
    /// (non-orphaned) manifest entry, so `publishStaging` takes its normal path (not the early-return
    /// mutable-only/no-PartWriteTxn branch).
    writeThroughS3Transaction(ca_tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/checksums.txt", "sums");

    DB::RelativePathsWithMetadata staged_before;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged_before, /*max_keys=*/0);
    ASSERT_EQ(staged_before.size(), 1u) << "exactly orphan.bin's S3 staging object should exist pre-commit";

    tx->unlinkFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/orphan.bin", false, false);

    tx->commit(DB::NoCommitOptions{});

    DB::RelativePathsWithMetadata staged_after;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged_after, /*max_keys=*/0);
    EXPECT_TRUE(staged_after.empty())
        << "cleanupPendingTempFiles must remove the orphaned S3 staging object after a successful commit";
}

/// (b) Read-your-writes over an S3 pending blob (before commit) returns the staged bytes from the S3
/// staging object, not a local temp file.
TEST(CASS3Staging, ReadYourWritesReturnsStagedBytesFromS3StagingObject)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountB");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);

    const std::string path = "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin";
    const std::string payload(5000, 'z');
    writeThroughS3Transaction(ca_tx, path, payload);

    auto read_buf = tx->tryReadFileInFlight(path, DB::ReadSettings{}, {});
    ASSERT_NE(read_buf, nullptr);
    std::string got;
    DB::readStringUntilEOF(got, *read_buf);
    EXPECT_EQ(got, payload);
}

/// (c) `sweepOwnMountStaging` removes only objects under the given mount prefix and leaves a DIFFERENT
/// mount's staging objects untouched (the lease-fence — `CASStagingSweeper.h`).
TEST(CASStagingSweeper, RemovesOnlyObjectsUnderGivenMountPrefix)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    const std::string root = storage->getCommonKeyPrefix();

    auto put = [&](const std::string & key, const std::string & bytes)
    {
        auto buf = storage->writeObject(DB::StoredObject(key), DB::WriteMode::Rewrite);
        buf->write(bytes.data(), bytes.size());
        buf->finalize();
    };

    put(root + "/p/staging/mountA/one.tmp", "a1");
    put(root + "/p/staging/mountA/two.tmp", "a2");
    put(root + "/p/staging/mountB/three.tmp", "b1");   /// a DIFFERENT mount's staging — must survive

    DB::Cas::sweepOwnMountStaging(*storage, root + "/p/staging/mountA/");

    EXPECT_FALSE(storage->exists(DB::StoredObject(root + "/p/staging/mountA/one.tmp")));
    EXPECT_FALSE(storage->exists(DB::StoredObject(root + "/p/staging/mountA/two.tmp")));
    EXPECT_TRUE(storage->exists(DB::StoredObject(root + "/p/staging/mountB/three.tmp")));
}

/// (d) GC's blob-discovery LISTs ONLY `Layout::blobsPrefix()` (`<pool>/blobs/`) — a top-level prefix
/// strictly disjoint from the S3-staging area (`<pool>/staging/<mount_id>/`), so a staging object can
/// never be listed, HEAD'd, or condemned as an orphan blob by GC's fold (`CasGc.cpp`, `CasFsck.cpp`).
/// This is a prefix-separation assertion (the GC fold itself is not unit-testable in isolation from a
/// full round — see `gtest_cas_gc_fold.cpp` for that machinery); it pins the invariant a refactor that
/// nested `staging/` under `blobs/` (or vice versa) would violate.
TEST(CASS3Staging, GcBlobDiscoveryPrefixExcludesStagingObjects)
{
    const DB::Cas::Layout layout("p");
    const std::string blobs_prefix = layout.blobsPrefix();
    const std::string staging_prefix = "p/staging/mountA/";
    const std::string staging_key = staging_prefix + "aaa.tmp";

    EXPECT_EQ(blobs_prefix, "p/blobs/");
    EXPECT_FALSE(staging_prefix.starts_with(blobs_prefix));
    EXPECT_FALSE(blobs_prefix.starts_with(staging_prefix));
    EXPECT_FALSE(staging_key.starts_with(blobs_prefix));
}

#if USE_AWS_S3

namespace
{

/// A `LocalObjectStorage` that reports the GCS generation dialect
/// (`conditionalOpsUseGenerationTokens() == true`) and a non-`Local` `getType()`, so
/// `ContentAddressedMetadataStorage::openPoolView` builds its backend in `Mode::Native` with
/// `native_token_type == TokenType::Generation`. The fake also advertises native copy so generation
/// token mode can exercise explicit S3 staging without endpoint/provider heuristics.
///
/// Holds every object entirely in memory, keyed by the BARE CAS key exactly as `Backend` hands it to
/// `object_storage` (e.g. `"pool/_probe/<hex>/token"`). Native mode never asks `object_storage` to
/// resolve that key against anything (`ContentAddressedMetadataStorage::physicalKey` is a documented
/// no-op for Native, since a real S3 client resolves a bucket-relative key against its own bucket/prefix
/// configuration internally) -- so this fake never needs a notion of "resolve a key to a location" at
/// all, unlike a real filesystem-backed object storage would. That sidesteps the class of bug a
/// resolve-then-strip round trip through the real `LocalObjectStorage` file/list implementation is prone
/// to (a key is a key, with no round trip to get wrong), and it never touches the real filesystem, so it
/// cannot leak files into the test process's working directory either.
///
/// A writable Native-mode mount always runs the mandatory capability battery (`CasProbe.cpp`), which
/// requires REAL conditional-write enforcement: the precondition is evaluated when a write completes,
/// signaled by an `S3Exception` carrying the canonical `PreconditionFailed` name (see
/// `ObjectStorageBackend::finalizeConditionalWrite`). `writeObject` therefore buffers bytes in memory and
/// defers both the precondition check and the commit to `finalize`, mirroring how a real object store
/// only commits -- and only then can reject -- on PUT completion.
class FakeGenerationObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    DB::ObjectStorageType getType() const override { return DB::ObjectStorageType::S3; }
    bool conditionalOpsUseGenerationTokens() const override { return true; }
    std::optional<bool> isBucketVersioningEnabled() const override { return false; }
    bool supportsRetryProfile(DB::ObjectStorageRetryProfile) const override { return true; }
    bool supportsCopyMode(DB::ObjectStorageCopyMode mode) const override
    {
        return mode == DB::ObjectStorageCopyMode::Default || mode == DB::ObjectStorageCopyMode::NativeOnly;
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object,
        DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> /*attributes*/,
        size_t /*buf_size*/,
        const DB::WriteSettings & write_settings) override
    {
        if (mode != DB::WriteMode::Rewrite)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "FakeGenerationObjectStorage only supports Rewrite");
        return std::make_unique<ConditionalWriteBuffer>(
            *this, object.remote_path, write_settings.object_storage_write_if_none_match,
            write_settings.object_storage_write_if_match);
    }

    bool exists(const DB::StoredObject & object) const override
    {
        std::lock_guard lock(mutex);
        return objects.contains(object.remote_path);
    }

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject & object,
        const DB::ReadSettings & /*read_settings*/,
        std::optional<size_t> /*read_hint*/,
        bool /*use_external_buffer*/,
        bool /*restrict_seek*/) const override
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(object.remote_path);
        if (it == objects.end())
            /// `RESOURCE_NOT_FOUND`, not a plain `DB::Exception`: `Backend::probeSentinelRaw`'s Native
            /// path (`CasObjectStorageBackend.cpp`) classifies absence ONLY from a caught `S3Exception`
            /// carrying `NO_SUCH_KEY`/`RESOURCE_NOT_FOUND` (or the matching exception name) -- anything
            /// else, including an unrecognized exception TYPE, falls through to `ProbeOutcome::
            /// Indeterminate` (fail-closed). A bodyless HEAD on a real absent S3 key throws exactly this
            /// code, since the SDK cannot parse a `<Code>` from a body that was never sent.
            throw DB::S3Exception("FakeGenerationObjectStorage: object does not exist",
                                   Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
        /// Copies the bytes, matching a real remote read (no shared ownership with the stored entry, so
        /// a later overwrite of this key cannot mutate bytes a caller is still reading).
        return std::make_unique<DB::ReadBufferFromOwnMemoryFile>(object.remote_path, it->second.bytes);
    }

    DB::ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = tryGetObjectMetadata(path, with_tags);
        if (!metadata)
            throw DB::S3Exception("FakeGenerationObjectStorage: object does not exist",
                                   Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
        return *metadata;
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool /*with_tags*/) const override
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(path);
        if (it == objects.end())
            return std::nullopt;
        DB::ObjectMetadata metadata;
        metadata.size_bytes = it->second.bytes.size();
        metadata.etag = std::to_string(it->second.generation);
        return metadata;
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadataWithNativeToken(const std::string & path, bool with_tags) const override
    {
        return tryGetObjectMetadata(path, with_tags);
    }

    void removeObjectIfExists(const DB::StoredObject & object) override
    {
        std::lock_guard lock(mutex);
        objects.erase(object.remote_path);
    }

    void removeObjectsIfExist(const DB::StoredObjects & objects_to_remove) override
    {
        std::lock_guard lock(mutex);
        for (const auto & object : objects_to_remove)
            objects.erase(object.remote_path);
    }

    /// `path` is a bare CAS-relative prefix (e.g. `"pool/"` or `"pool/_probe/<hex>"`) -- exactly what
    /// `Backend::list`'s Native-mode path passes through unchanged (it applies no prefix stripping of
    /// its own there) and expects back on every listed key. A plain string-prefix scan over the
    /// in-memory keys already IS that key space, so there is no separate "physical" representation to
    /// resolve to or strip back off.
    void listObjects(const std::string & path, DB::RelativePathsWithMetadata & children, size_t max_keys) const override
    {
        std::lock_guard lock(mutex);
        for (const auto & [key, entry] : objects)
        {
            if (!key.starts_with(path))
                continue;
            DB::ObjectMetadata metadata;
            metadata.size_bytes = entry.bytes.size();
            metadata.etag = std::to_string(entry.generation);
            children.push_back(std::make_shared<DB::RelativePathWithMetadata>(key, std::move(metadata)));
            if (max_keys != 0 && children.size() >= max_keys)
                break;
        }
    }

    DB::ConditionalRemoveResult removeObjectIfTokenMatches(const DB::StoredObject & object, const std::string & etag) override
    {
        std::lock_guard lock(mutex);
        DB::ConditionalRemoveResult result;
        auto it = objects.find(object.remote_path);
        if (it == objects.end())
        {
            result.outcome = DB::ConditionalRemoveOutcome::NotFound;
            return result;
        }
        if (std::to_string(it->second.generation) != etag)
        {
            result.outcome = DB::ConditionalRemoveOutcome::TokenMismatch;
            return result;
        }
        objects.erase(it);
        result.outcome = DB::ConditionalRemoveOutcome::Removed;
        return result;
    }

    /// Checks the write-once/exact-token precondition against the current generation and, on success,
    /// stores `bytes` and mints the next generation. Throws an `S3Exception` naming `PreconditionFailed`
    /// on a lost condition -- the one signal `finalizeConditionalWrite` classifies as
    /// `PutOutcome::PreconditionFailed` rather than an ordinary failure.
    void commitConditionalWrite(const std::string & key, const std::string & bytes,
                                 const std::string & if_none_match, const std::string & if_match)
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(key);
        const bool exists_now = it != objects.end();
        if (!if_none_match.empty() && exists_now)
            throw DB::S3Exception("FakeGenerationObjectStorage: if-none-match precondition failed",
                                   Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed");
        if (!if_match.empty() && (!exists_now || std::to_string(it->second.generation) != if_match))
            throw DB::S3Exception("FakeGenerationObjectStorage: if-match precondition failed",
                                   Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed");

        objects[key] = Entry{bytes, next_generation++};
    }

private:
    struct Entry
    {
        std::string bytes;
        uint64_t generation;
    };

    /// Buffers the whole body in memory (like `FakeStagingSink` above) so the entry is committed exactly
    /// once, at `finalize`, and only after the precondition has been checked.
    class ConditionalWriteBuffer final : public DB::WriteBufferFromFileBase
    {
    public:
        ConditionalWriteBuffer(FakeGenerationObjectStorage & storage_, std::string key_,
                                std::string if_none_match_, std::string if_match_)
            : DB::WriteBufferFromFileBase(/*buf_size=*/8192, nullptr, 0)
            , storage(storage_), key(std::move(key_))
            , if_none_match(std::move(if_none_match_)), if_match(std::move(if_match_))
        {
        }

        void sync() override {}
        std::string getFileName() const override { return key; }

    protected:
        void nextImpl() override
        {
            if (!offset())
                return;
            buffered.append(working_buffer.begin(), offset());
        }

        void finalizeImpl() override
        {
            next();
            storage.commitConditionalWrite(key, buffered, if_none_match, if_match);
        }

    private:
        FakeGenerationObjectStorage & storage;
        std::string key;
        std::string if_none_match;
        std::string if_match;
        std::string buffered;
    };

    mutable std::mutex mutex;
    std::map<std::string, Entry> objects;
    uint64_t next_generation = 1;
};

std::shared_ptr<FakeGenerationObjectStorage> makeFakeGenerationObjectStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_s3_staging_generation_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<FakeGenerationObjectStorage>(std::move(settings));
}

}

TEST(CASS3Staging, GenerationBackendMayUseNativeOnlyCopy)
{
    auto object_storage = makeFakeGenerationObjectStorageForTest();
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountGenerationNative");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    writeThroughS3Transaction(
        ca_tx,
        "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin",
        "generation-native-staging");

    DB::RelativePathsWithMetadata staged;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged, /*max_keys=*/0);
    EXPECT_EQ(staged.size(), 1u);
}

#endif
