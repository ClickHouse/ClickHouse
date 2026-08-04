#include <gtest/gtest.h>
#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasProbe.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskCommitTransactionOptions.h>
#include <IO/ReadHelpers.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <sstream>
#include <string>

/// Task 0 of the S3-native staging plan: pure config plumbing, ZERO behavior change.
/// `staging_backend` (default `local`) is parsed
/// from the CAS disk config; the parsed `StagingBackend` is exposed via
/// `ContentAddressedMetadataStorage::stagingBackend()`. `::conditionalCopySupported()` is a stored
/// bool, defaulting to `false` until a later task wires the mount-time capability probe.
///
/// The global constraint (OFF BY DEFAULT) is the DEFAULT arm below: absent config keys must parse to
/// `StagingBackend::Local` with `conditionalCopySupported()==false`.

namespace DB::ContentAddressedSetting
{
    extern const ContentAddressedSettingsString staging_backend;
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

/// A test-only `LocalObjectStorage` subclass whose `copyObjectConditional` is configurable, so the
/// Task 3 selection logic (`DB::Cas::probeConditionalCopy`) can be exercised without a live S3/RustFS
/// backend (live enforcement is Task 7). `LocalObjectStorage` already implements every OTHER pure
/// virtual (`writeObject`, `removeObjectIfExists`, `exists`, `copyObject`, ...) against real files
/// under a fresh temp root, so overriding just `copyObjectConditional` is enough to fake either an
/// ENFORCING or a NON-ENFORCING backend; a THROWING (default `NOT_IMPLEMENTED`) backend needs no
/// fake at all — a plain `LocalObjectStorage` already exercises that path (see
/// `DefaultCopyObjectConditionalThrowsNotImplemented` above).
class FakeConditionalCopyObjectStorage : public DB::LocalObjectStorage
{
public:
    enum class Mode
    {
        /// Real write-once semantics: creates the destination iff it was absent; a destination that
        /// already exists is REJECTED (created=false), no bytes touched.
        Enforcing,
        /// A backend that silently ignores `If-None-Match`: every call overwrites the destination
        /// and reports created=true, even when the destination already existed.
        NonEnforcing,
    };

    FakeConditionalCopyObjectStorage(DB::LocalObjectStorageSettings settings_, Mode mode_)
        : DB::LocalObjectStorage(std::move(settings_)), mode(mode_)
    {
    }

    DB::ConditionalCopyResult copyObjectConditional(
        const DB::StoredObject & object_from,
        const DB::StoredObject & object_to,
        const DB::ReadSettings & read_settings,
        const DB::WriteSettings & write_settings,
        std::optional<DB::ObjectAttributes> object_to_attributes) override
    {
        ++call_count;
        if (mode == Mode::Enforcing && exists(object_to))
            return {.created = false, .dest_etag = {}};

        copyObject(object_from, object_to, read_settings, write_settings, object_to_attributes);
        return {.created = true, .dest_etag = "fake-etag"};
    }

    int callCount() const { return call_count; }

private:
    Mode mode;
    int call_count = 0;
};

/// Build a `FakeConditionalCopyObjectStorage` rooted at a fresh, unique temp directory (mirrors
/// `DB::Cas::tests::makeLocalObjectStorageForTest`).
std::shared_ptr<FakeConditionalCopyObjectStorage> makeFakeConditionalCopyStorage(FakeConditionalCopyObjectStorage::Mode mode)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_s3_staging_probe_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<FakeConditionalCopyObjectStorage>(std::move(settings), mode);
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

/// Task 5 of the S3-native staging plan: the promote path (`PartWriteTxn::putBlob` with
/// `BlobSource::server_side_copy_from` set) drives a WRITE-ONCE conditional server-side copy through
/// the SAME condemn/resurrect gate the streaming path uses. This backend is a `DB::Cas::InMemoryBackend`
/// (which models conditional create, so it honors both the write-once `promoteStaged` and the
/// unconditional `resurrect` contracts) that RECORDS every server-side-copy call so a test can
/// assert the copy source/destination and the conditional-vs-unconditional distinction — in particular
/// that a condemned-blob RESURRECT copies FROM the staging key, NEVER from the condemned blob key
/// (`feedback_ca_resurrect_invariant`), and that a live blob is NEVER unconditionally overwritten.
class RecordingStagingBackend : public DB::Cas::InMemoryBackend
{
public:
    struct CopyCall
    {
        std::string from;
        std::string to;
        bool conditional;   /// true = promoteStaged (write-once); false = resurrect (unconditional)
    };

    std::vector<CopyCall> copy_calls;

    DB::Cas::PutResult promoteStaged(const String & staging_key, const String & blob_key) override
    {
        copy_calls.push_back({staging_key, blob_key, /*conditional=*/true});
        return DB::Cas::InMemoryBackend::promoteStaged(staging_key, blob_key);
    }

    DB::Cas::Token resurrect(DB::ReadBuffer & payload, uint64_t payload_size, const String & blob_key,
                             const String & fresh_header) override
    {
        /// The source is no longer an argument -- the caller opens the reader -- so `from` is recorded
        /// as empty here and the "never the condemned key" invariant is asserted through `reads_of`
        /// below, which counts what was actually READ. That is the stronger check: it observes the I/O
        /// rather than a parameter the backend was told about.
        copy_calls.push_back({String{}, blob_key, /*conditional=*/false});
        return DB::Cas::InMemoryBackend::resurrect(payload, payload_size, blob_key, fresh_header);
    }

    /// Every key READ AS A STREAM, with a count. The resurrect opens its source with `getStream`, so
    /// this counts exactly the reads that path performs -- and deliberately not the materializing
    /// `get`, which the assertions themselves use to inspect bodies.
    std::map<String, size_t> reads_of;

    using DB::Cas::InMemoryBackend::getStream;
    std::optional<DB::Cas::GetStreamResult> getStream(const String & key, DB::Cas::Range range) override
    {
        ++reads_of[key];
        return DB::Cas::InMemoryBackend::getStream(key, range);
    }


    /// Every unconditional (resurrect) copy this backend saw — empty iff no live/condemned body was ever
    /// overwritten. `assertNeverOverwritesLiveBlob` reads this to enforce invariant (d).
    size_t unconditionalCopyCount() const
    {
        size_t n = 0;
        for (const CopyCall & c : copy_calls)
            n += c.conditional ? 0 : 1;
        return n;
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
/// `observeAndAdmit` holds), returning the build ready for a `putBlob` promote of `hash`.
DB::Cas::PartWriteTxnPtr precommittedBuildFor(
    const DB::Cas::PoolPtr & s, const DB::Cas::RootNamespace & ns, const String & ref,
    const DB::UInt128 & hash, uint64_t blob_size)
{
    DB::Cas::PartWriteTxnPtr build = startStagingBuild(s, ns, ref);
    const DB::Cas::ManifestId id = build->stageManifest({DB::Cas::tests::blobEntryFor("col.bin", hash, blob_size)});
    build->precommitAdd(ns, ref, id);
    return build;
}

/// A `BlobSource` that promotes via the S3 server-side-copy path (no local `open`).
DB::Cas::BlobSource serverSideCopySource(const std::string & staging_key, uint64_t size)
{
    DB::Cas::BlobSource source;
    source.size = size;
    source.server_side_copy_from = staging_key;
    return source;
}

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

TEST(CASS3Staging, DefaultConstructedStorageReportsLocalAndNoConditionalCopy)
{
    /// Constructed with no staging-related args at all (mirrors the existing gtest call sites, e.g.
    /// gtest_ca_wiring.cpp, which stop at `context_`): the accessors must reflect the same
    /// byte-for-byte-current-behavior defaults the config parser produces above.
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "cas_s3_staging_default_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);

    EXPECT_EQ(storage->stagingBackend(), DB::Cas::StagingBackend::Local);
    EXPECT_FALSE(storage->conditionalCopySupported());
}

/// Task 2 of the S3-native staging plan: `IObjectStorage::copyObjectConditional` (write-once
/// conditional server-side copy) — the interface-level contract. Backends without an enforced,
/// native conditional copy MUST NOT override the default: it fail-closes with `NOT_IMPLEMENTED`,
/// exactly like the existing `IObjectStorage::removeObjectIfTokenMatches` default (never silently
/// falls back to an unconditional overwrite). `LocalObjectStorage` (used by
/// `makeLocalObjectStorageForTest`) does not override `copyObjectConditional`, so it exercises the
/// base-class default directly. Live 412-vs-created S3 semantics are covered by the Task 7
/// integration test (with_rustfs); this is deliberately just the fail-closed contract test.
TEST(CASS3Staging, DefaultCopyObjectConditionalThrowsNotImplemented)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();

    const DB::StoredObject from{"cas_s3_staging_conditional_copy_from"};
    const DB::StoredObject to{"cas_s3_staging_conditional_copy_to"};

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED, [&]
    {
        storage->copyObjectConditional(from, to, DB::ReadSettings{}, DB::WriteSettings{});
    });
}

/// Task 3 of the S3-native staging plan: the mount-time capability probe (`DB::Cas::probeConditionalCopy`)
/// for the OPTIONAL conditional-copy capability (distinct from the mandatory `runCapabilityProbe`
/// battery). These three tests cover the fail-close SELECTION logic with fakes — live 412-vs-created
/// enforcement against a real backend is Task 7 (with_rustfs integration test).

TEST(CASS3Staging, ProbeConditionalCopyReturnsTrueForEnforcingBackend)
{
    auto storage = makeFakeConditionalCopyStorage(FakeConditionalCopyObjectStorage::Mode::Enforcing);

    EXPECT_TRUE(DB::Cas::probeConditionalCopy(*storage, "probe_prefix"));
    /// Both the "fresh destination" and the "already-existing destination" conditional copies ran.
    EXPECT_EQ(storage->callCount(), 2);
}

TEST(CASS3Staging, ProbeConditionalCopyReturnsFalseForNonEnforcingBackend)
{
    auto storage = makeFakeConditionalCopyStorage(FakeConditionalCopyObjectStorage::Mode::NonEnforcing);

    /// The backend silently overwrites the destination on the second call (created=true again) —
    /// it does not enforce If-None-Match, so the probe must fail closed.
    EXPECT_FALSE(DB::Cas::probeConditionalCopy(*storage, "probe_prefix"));
}

TEST(CASS3Staging, ProbeConditionalCopyReturnsFalseWhenCopyObjectConditionalThrows)
{
    /// A plain `LocalObjectStorage` does not override `copyObjectConditional` at all — it falls
    /// through to the base-class default, which throws NOT_IMPLEMENTED (exactly what a real backend
    /// without conditional-copy support does). The probe must never propagate this: it fails closed.
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();

    EXPECT_FALSE(DB::Cas::probeConditionalCopy(*storage, "probe_prefix"));
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

/// Task 5 of the S3-native staging plan: the promote path. `PartWriteTxn::putBlob` with
/// `BlobSource::server_side_copy_from` set drives a WRITE-ONCE conditional SERVER-SIDE COPY through the
/// SAME condemn/resurrect gate as the streaming path (spec 2026-07-11-cas-s3-native-staging §5/§9). The
/// four cases below use `RecordingStagingBackend` (an emulated backend that models conditional create
/// and records every server-side-copy call). Live 412-vs-created enforcement against a real backend is
/// Task 7 (with_rustfs integration test).

/// (a) Fresh blob key ⇒ the write-once conditional copy CREATES it; the tokened Blob dep is recorded at
/// the copy's destination token (the new incarnation token). No unconditional copy is ever issued.
TEST(CASS3Staging, PromoteViaServerSideCopyCreatesFreshBlobTokenedDep)
{
    auto backend = std::make_shared<RecordingStagingBackend>();
    auto store = openStagingPool(backend);
    const DB::Cas::RootNamespace ns{"srv1/nsA"};
    const std::string ref = "part_a";

    const DB::UInt128 hash = DB::Cas::tests::u128Of("payload-A");
    const DB::Cas::BlobRef blob_id{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)};
    const std::string blob_key = store->layout().blobKey(blob_id);
    const std::string staging_key = "p/staging/mount1/aaa.tmp";
    const std::string payload(300, 'a');
    backend->putIfAbsent(staging_key, payload);

    auto build = precommittedBuildFor(store, ns, ref, hash, payload.size());
    const DB::Cas::PutBlobResult bref = build->putBlob(blob_id, serverSideCopySource(staging_key, payload.size()));

    /// EXACTLY one CONDITIONAL server-side copy staging->blobKey; zero unconditional copies.
    ASSERT_EQ(backend->copy_calls.size(), 1u);
    EXPECT_TRUE(backend->copy_calls[0].conditional);
    EXPECT_EQ(backend->copy_calls[0].from, staging_key);
    EXPECT_EQ(backend->copy_calls[0].to, blob_key);
    EXPECT_EQ(backend->unconditionalCopyCount(), 0u);

    /// A TOKENED Blob dep was recorded (created path); the created blob carries the copy's dest token.
    EXPECT_TRUE(build->depIsTokened(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)}));
    const DB::Cas::HeadResult hr = backend->head(blob_key);
    ASSERT_TRUE(hr.exists);
    EXPECT_FALSE(hr.token.empty());
    EXPECT_EQ(bref.size, payload.size());

    /// The promoted blob body IS the staging bytes (server-side copy moved them verbatim).
    const auto got = backend->get(blob_key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, payload);
}

/// (b) Blob key already exists and is CLEAN ⇒ the conditional copy 412s and the writer ADOPTS the
/// existing incarnation. No copy of any kind lands over the live blob. Also covers invariant (d): NO
/// unconditional copy is ever issued over a live (non-condemned) blob.
TEST(CASS3Staging, PromoteOverExistingCleanBlobAdoptsAndNeverOverwrites)
{
    auto backend = std::make_shared<RecordingStagingBackend>();
    auto store = openStagingPool(backend);
    const DB::Cas::RootNamespace ns{"srv1/nsB"};
    const std::string ref = "part_b";

    const DB::UInt128 hash = DB::Cas::tests::u128Of("payload-B");
    const DB::Cas::BlobRef blob_id{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)};
    const std::string blob_key = store->layout().blobKey(blob_id);
    const std::string staging_key = "p/staging/mount1/bbb.tmp";
    backend->putIfAbsent(staging_key, std::string(300, 'b'));

    /// A pre-existing, well-formed, CLEAN blob (envelope + payload) already at the content key.
    DB::Cas::tests::writeBlobBody(*backend, store->layout(), hash);
    DB::Cas::tests::writeMetaClean(*backend, store->layout(), hash, /*size=*/1);
    const DB::Cas::HeadResult before = backend->head(blob_key);
    ASSERT_TRUE(before.exists);

    auto build = precommittedBuildFor(store, ns, ref, hash, 300);
    build->putBlob(blob_id, serverSideCopySource(staging_key, 300));

    /// Exactly one CONDITIONAL promote (which 412s) — then ADOPT. Invariant (d): zero unconditional copies.
    ASSERT_EQ(backend->copy_calls.size(), 1u);
    EXPECT_TRUE(backend->copy_calls[0].conditional);
    EXPECT_EQ(backend->unconditionalCopyCount(), 0u);

    /// The existing incarnation is untouched: same token, same bytes.
    const DB::Cas::HeadResult after = backend->head(blob_key);
    EXPECT_EQ(after.token, before.token);

    /// The adopt recorded a TOKENED dep at the observed (existing) incarnation's token.
    EXPECT_TRUE(build->depIsTokened(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)}));
}

/// (c) Blob key exists but is CONDEMNED ⇒ the writer RESURRECTS by re-uploading its OWN staging PAYLOAD
/// under a FRESH-tagged envelope header — NEVER a read/copy of the condemned blob key
/// (`feedback_ca_resurrect_invariant`) — and the resurrected body DIFFERS from the condemned incarnation
/// (INV-NO-RETURN: a verbatim copy would reproduce identical bytes ⇒ identical ETag ⇒ the queued
/// exact-token delete of the condemned incarnation would kill the live resurrection = data loss).
TEST(CASS3Staging, PromoteOverCondemnedBlobResurrectsWithFreshTagNotVerbatim)
{
    auto backend = std::make_shared<RecordingStagingBackend>();
    auto store = openStagingPool(backend);
    const DB::Cas::RootNamespace ns{"srv1/nsC"};
    const std::string ref = "part_c";

    const DB::UInt128 hash = DB::Cas::tests::u128Of("payload-C");
    const DB::Cas::BlobRef blob_id{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)};
    const std::string blob_key = store->layout().blobKey(blob_id);
    const std::string staging_key = "p/staging/mount1/ccc.tmp";
    const std::string payload(300, 'c');

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
    /// verbatim resurrect WOULD reproduce these identical bytes ⇒ identical ETag ⇒ collision.
    backend->putIfAbsent(blob_key, staging_bytes);
    DB::Cas::tests::writeMetaClean(*backend, store->layout(), hash, /*size=*/payload.size());
    DB::Cas::tests::condemnMeta(*backend, store->layout(), hash, /*condemn_round=*/5);
    const DB::Cas::HeadResult before = backend->head(blob_key);
    ASSERT_TRUE(before.exists);

    auto build = precommittedBuildFor(store, ns, ref, hash, payload.size());
    build->putBlob(blob_id, serverSideCopySource(staging_key, payload.size()));

    /// A CONDITIONAL promote (412 — blob present) FOLLOWED BY exactly one UNCONDITIONAL resurrect
    /// whose SOURCE is the staging key, NEVER the condemned blob key.
    ASSERT_EQ(backend->copy_calls.size(), 2u);
    EXPECT_TRUE(backend->copy_calls[0].conditional);
    EXPECT_EQ(backend->copy_calls[0].to, blob_key);
    EXPECT_FALSE(backend->copy_calls[1].conditional);
    EXPECT_EQ(backend->copy_calls[1].to, blob_key);
    /// INV: the resurrect reads the STAGING object and never the condemned blob key. Asserted on the
    /// reads themselves rather than on a source argument, because the caller now opens the reader.
    EXPECT_GT(backend->reads_of[staging_key], 0u) << "the resurrect must read the writer's own staging object";
    EXPECT_EQ(backend->reads_of[blob_key], 0u) << "the condemned blob key must never be read";
    EXPECT_EQ(backend->unconditionalCopyCount(), 1u);

    /// The incarnation token is REFRESHED (a fresh incarnation displaced the condemned one).
    const DB::Cas::HeadResult after = backend->head(blob_key);
    EXPECT_NE(after.token, before.token);
    ASSERT_TRUE(after.exists);

    const auto got = backend->get(blob_key);
    ASSERT_TRUE(got.has_value());
    const uint64_t header_len = store->poolMeta().blob_header_len;

    /// INV-NO-RETURN — THE fresh-tag property: the resurrected body is NOT byte-identical to the
    /// condemned incarnation (a verbatim copy would have been). The PAYLOAD is preserved exactly (the
    /// resurrect read it from OUR staging object, skipping the staging header), but the envelope HEADER
    /// differs — the writer minted a FRESH incarnation_tag — so on a real content-addressed store the
    /// resurrected ETag differs and the queued exact-token delete of the condemned incarnation cannot
    /// match the live resurrection.
    EXPECT_NE(got->bytes, staging_bytes);
    ASSERT_GE(got->bytes.size(), header_len);
    EXPECT_EQ(got->bytes.substr(header_len), payload);              /// payload preserved
    EXPECT_NE(got->bytes.substr(0, header_len), staging_header);    /// header freshly re-tagged

    /// The resurrect recorded a tokened dep and flipped the meta back to Clean.
    EXPECT_TRUE(build->depIsTokened(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)}));
    const auto lm = DB::Cas::tests::loadMetaForTest(*backend, store->layout(), hash);
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(lm->meta.state, DB::Cas::MetaState::Clean);
}

/// ===========================================================================================
/// Task 6 of the S3-native staging plan: staging cleanup after commit, read-your-writes over an S3
/// pending blob, and the mount-lease-scoped sweeper (`CASStagingSweeper.h`).
///
/// The wiring-level tests below (cleanup-after-commit, read-your-writes) drive the REAL
/// `ContentAddressedMetadataStorage` + `ContentAddressedTransaction` over `FakeConditionalCopyObjectStorage`
/// in `Enforcing` mode. The fake's REAL `getType()` stays `Local`, so `Cas::ObjectStorageBackend` selects
/// `EmulatedSingleProcess` for the CAS core protocol — the same fully-supported combination every other
/// `gtest_ca_wiring.cpp` test uses — while the mount-time conditional-copy PROBE
/// (`Cas::probeConditionalCopy`) is decoupled from that (it exercises `copyObjectConditional` directly
/// against the object storage), so it reports S3 staging as usable independent of the backend mode. This
/// lets `writeFile` take the S3-staging code path (stream to a staging object while hashing) WITHOUT
/// needing a live/native conditional-copy backend for the promote: `PartWriteTxn::putBlob`'s
/// `promoteStaged`/`resurrect` seams (Native-mode only — see `CasObjectStorageBackend.cpp`) are
/// already covered directly against `Cas::PartWriteTxn`/`RecordingStagingBackend` above (Task 5) and against a
/// live backend in Task 7's `with_rustfs` integration test; these two tests only ever exercise an S3
/// pending blob that is either NEVER referenced (the B189 orphan shape — publishStaging skips its
/// `putBlob`) or read BEFORE commit, so `promoteStaged` is never reached here.

namespace
{

/// Construct a `ContentAddressedMetadataStorage` with `staging_backend=s3` over `object_storage`,
/// mirroring `DefaultConstructedStorageReportsLocalAndNoConditionalCopy`'s settings defaults for every
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

/// (a) A successful commit removes the S3 staging object of a pending blob it staged. Uses the B189
/// orphan shape (the pending blob's entry is unlinked before commit) so `publishStaging` never calls
/// `putBlob` for it — only `cleanupPendingTempFiles`'s Task 6 branch ever touches this staging object,
/// which is exactly the seam this test targets.
TEST(CASS3Staging, SuccessfulCommitRemovesOrphanedS3StagingObject)
{
    auto object_storage = makeFakeConditionalCopyStorage(FakeConditionalCopyObjectStorage::Mode::Enforcing);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountA");
    metadata_storage->startup();
    ASSERT_TRUE(metadata_storage->conditionalCopySupported());

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
    auto object_storage = makeFakeConditionalCopyStorage(FakeConditionalCopyObjectStorage::Mode::Enforcing);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountB");
    metadata_storage->startup();
    ASSERT_TRUE(metadata_storage->conditionalCopySupported());

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
