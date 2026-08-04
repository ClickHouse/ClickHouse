#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>
#include <Common/ProfileEvents.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Disks/WriteMode.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/S3Common.h>
#include <chrono>
#include <filesystem>
#include <map>
#include <mutex>
#endif

using namespace DB::Cas;

/// Minimal concrete implementation that overrides every pure virtual with trivial defaults.
/// Purpose: verify the interface compiles, is overridable, and result-type defaults are sane.
struct NullBackend final : Backend
{
    std::optional<GetResult> get(const String & /*key*/, Range /*range*/) override
    {
        return std::nullopt;
    }

    std::optional<GetStreamResult> getStream(const String & /*key*/, Range /*range*/) override
    {
        return std::nullopt;
    }

    HeadResult head(const String & /*key*/) override
    {
        return HeadResult{};
    }

    PutResult putIfAbsent(const String & /*key*/, const String & /*bytes*/, const ObjectMeta & /*meta*/) override
    {
        return {PutOutcome::Done, {}};
    }

    WriteSinkPtr putIfAbsentStream(const String & /*key*/, const ObjectMeta & /*meta*/) override
    {
        return nullptr;   /// trivial default — streaming behavior is pinned by the CASBackendContract suite
    }

    PutResult putOverwrite(const String & /*key*/, const String & /*bytes*/, const Token & /*expected*/, const ObjectMeta & /*meta*/) override
    {
        return {PutOutcome::PreconditionFailed, {}};
    }

    CasResult casPut(const String & /*key*/, const String & /*bytes*/, const std::optional<Token> & /*expected*/, const ObjectMeta & /*meta*/) override
    {
        return {CasOutcome::Conflict, {}};
    }

    DeleteOutcome deleteExact(const String & /*key*/, const Token & /*token*/) override
    {
        return DeleteOutcome{};
    }

    ListPage list(const String & /*prefix*/, const String & /*cursor*/, size_t /*limit*/) override
    {
        return ListPage{};
    }

    bool supportsListTokens() const override { return false; }
};

TEST(CASBackend, NullBackendShapeAndDefaults)
{
    NullBackend b;
    // Use the base-class reference so virtual dispatch uses base-class default args.
    Backend & ref = b;

    // get returns absent
    EXPECT_FALSE(ref.get("k").has_value());

    // head returns non-existent
    HeadResult h = b.head("k");
    EXPECT_FALSE(h.exists);
    EXPECT_EQ(h.size, 0u);
    EXPECT_TRUE(h.token.empty());

    // putIfAbsent returns Done
    EXPECT_EQ(ref.putIfAbsent("k", "v").outcome, PutOutcome::Done);

    // putOverwrite returns PreconditionFailed
    EXPECT_EQ(ref.putOverwrite("k", "v", Token{}).outcome, PutOutcome::PreconditionFailed);

    // casPut returns Conflict
    EXPECT_EQ(ref.casPut("k", "v", std::nullopt).outcome, CasOutcome::Conflict);

    // deleteExact default kind is NotFound
    DeleteOutcome d = b.deleteExact("k", Token{});
    EXPECT_EQ(d.kind, DeleteOutcome::Kind::NotFound);
    EXPECT_FALSE(d.created_delete_marker);

    // list returns empty page
    ListPage page = b.list("p/", "", 10);
    EXPECT_TRUE(page.keys.empty());
    EXPECT_TRUE(page.next_cursor.empty());

    // Range::whole() helper
    EXPECT_TRUE(Range{}.whole());
    Range r1; r1.offset = 1;
    EXPECT_FALSE(r1.whole());
    Range r2; r2.length = 5u;
    EXPECT_FALSE(r2.whole());
}

// =====================================================================
// Task 3: CasInMemoryBackend — enforcing token semantics
// =====================================================================

TEST(CASInMemory, PutIfAbsentAndGet)
{
    InMemoryBackend b;
    const auto put = b.putIfAbsent("k", "v1");
    const Token t1 = put.token;
    EXPECT_EQ(put.outcome, PutOutcome::Done);
    EXPECT_FALSE(t1.empty());
    EXPECT_EQ(b.putIfAbsent("k", "clobber").outcome, PutOutcome::PreconditionFailed);
    auto g = b.get("k");
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->bytes, "v1");
    EXPECT_EQ(g->token, t1);
    EXPECT_FALSE(b.get("absent").has_value());
}

TEST(CASInMemory, OverwriteIsTokenExactAndMintsFreshToken)
{
    InMemoryBackend b;
    const Token t1 = b.putIfAbsent("k", "v1").token;
    EXPECT_EQ(b.putOverwrite("k", "v2", Token{"wrong", TokenType::Emulated}).outcome, PutOutcome::PreconditionFailed);
    EXPECT_EQ(b.get("k")->bytes, "v1");                       // untouched on mismatch
    const auto overwrite = b.putOverwrite("k", "v2", t1);
    EXPECT_EQ(overwrite.outcome, PutOutcome::Done);
    EXPECT_NE(overwrite.token, t1);                           // tokens never repeat
    EXPECT_EQ(b.get("k")->bytes, "v2");
}

TEST(CASInMemory, CasPutCreateAndSwap)
{
    InMemoryBackend b;
    const auto create = b.casPut("m", "s1", std::nullopt);
    const Token t1 = create.token;
    EXPECT_EQ(create.outcome, CasOutcome::Committed);                             // create-if-absent
    EXPECT_EQ(b.casPut("m", "s1x", std::nullopt).outcome, CasOutcome::Conflict);  // exists now
    EXPECT_EQ(b.casPut("m", "s2", Token{"stale", TokenType::Emulated}).outcome, CasOutcome::Conflict);
    EXPECT_EQ(b.get("m")->bytes, "s1");
    EXPECT_EQ(b.casPut("m", "s2", t1).outcome, CasOutcome::Committed);
    EXPECT_EQ(b.get("m")->bytes, "s2");
}

TEST(CASInMemory, DeleteExactEnforced)
{
    InMemoryBackend b;
    const Token t1 = b.putIfAbsent("k", "v1").token;
    auto d1 = b.deleteExact("k", Token{"wrong", TokenType::Emulated});
    EXPECT_EQ(d1.kind, DeleteOutcome::Kind::TokenMismatch);
    EXPECT_TRUE(b.get("k").has_value());                      // SURVIVES wrong-token delete
    auto d2 = b.deleteExact("k", t1);
    EXPECT_EQ(d2.kind, DeleteOutcome::Kind::Deleted);
    EXPECT_FALSE(d2.created_delete_marker);
    EXPECT_FALSE(b.get("k").has_value());
    EXPECT_EQ(b.deleteExact("k", t1).kind, DeleteOutcome::Kind::NotFound);
}

TEST(CASInMemory, RangeGetAndHeadAndList)
{
    InMemoryBackend b;
    b.putIfAbsent("p/a", "0123456789");
    b.putIfAbsent("p/b", "xy");
    b.putIfAbsent("q/c", "z");
    EXPECT_EQ(b.get("p/a", Range{.offset = 2, .length = 3})->bytes, "234");
    auto h = b.head("p/a");
    EXPECT_TRUE(h.exists);
    EXPECT_EQ(h.size, 10u);
    auto page = b.list("p/", "", 10);
    ASSERT_EQ(page.keys.size(), 2u);                          // sorted, prefix-scoped
    EXPECT_EQ(page.keys[0].key, "p/a");
    EXPECT_EQ(page.keys[1].key, "p/b");
    EXPECT_TRUE(page.next_cursor.empty());
    auto page1 = b.list("p/", "", 1);                         // pagination
    EXPECT_EQ(page1.keys.size(), 1u);
    EXPECT_EQ(page1.keys[0].key, "p/a");
    EXPECT_EQ(page1.next_cursor, "p/a");
    EXPECT_FALSE(page1.next_cursor.empty());
    auto page2 = b.list("p/", page1.next_cursor, 1);
    EXPECT_EQ(page2.keys[0].key, "p/b");
}

// =====================================================================
// Task 4: CasInMemoryBackend — fault injection and probe-test modes
// =====================================================================

TEST(CASInMemoryFaults, HeldDeleteLandsLater)
{
    InMemoryBackend b;
    const Token t1 = b.putIfAbsent("k", "v1").token;
    b.setHoldDeletes(true);
    auto d = b.deleteExact("k", t1);                  // message "sent", not landed
    EXPECT_EQ(d.kind, DeleteOutcome::Kind::Deleted);  // caller sees the send accepted
    EXPECT_TRUE(b.get("k").has_value());              // ... but nothing landed yet
    ASSERT_EQ(b.pendingDeletes(), 1u);
    // the object is resurrected before the zombie lands:
    b.putOverwrite("k", "v1'", t1);
    auto landed = b.landPendingDelete(0);             // the zombie lands NOW
    EXPECT_EQ(landed.kind, DeleteOutcome::Kind::TokenMismatch);   // 412 — INV-NO-RETURN in miniature
    EXPECT_EQ(b.get("k")->bytes, "v1'");
}

TEST(CASInMemoryFaults, InjectedCasConflictFiresOnce)
{
    InMemoryBackend b;
    const Token t1 = b.casPut("m", "s1", std::nullopt).token;
    b.failNextCasPut("m");
    EXPECT_EQ(b.casPut("m", "s2", t1).outcome, CasOutcome::Conflict);     // injected
    EXPECT_EQ(b.get("m")->bytes, "s1");
    EXPECT_EQ(b.casPut("m", "s2", t1).outcome, CasOutcome::Committed);    // next attempt is real
}

TEST(CASInMemoryFaults, NonEnforcingModeMimicsBadBackend)
{
    InMemoryBackend b;
    b.setEnforceTokens(false);                        // MinIO-OSS-shaped backend
    b.putIfAbsent("k", "v1");
    auto d = b.deleteExact("k", Token{"totally-wrong", TokenType::Emulated});
    EXPECT_EQ(d.kind, DeleteOutcome::Kind::Deleted);  // silently deletes anyway — the dangerous behavior
    EXPECT_FALSE(b.get("k").has_value());
}

TEST(CASInMemoryFaults, VersioningMarkerMode)
{
    InMemoryBackend b;
    b.setSimulateDeleteMarkers(true);
    const Token t1 = b.putIfAbsent("k", "v1").token;
    EXPECT_TRUE(b.deleteExact("k", t1).created_delete_marker);    // probe must reject this pool
}

TEST(CASInMemoryBackend, RoundTripsUserMetadata)
{
    DB::Cas::InMemoryBackend backend;
    const DB::Cas::ObjectMeta meta{{"cas_owner", "ab:7:42"}};
    ASSERT_EQ(backend.putIfAbsent("k/key", "body", meta).outcome, DB::Cas::PutOutcome::Done);

    const auto hr = backend.head("k/key");
    ASSERT_TRUE(hr.exists);
    ASSERT_EQ(hr.attributes.at("cas_owner"), "ab:7:42");

    const auto gr = backend.get("k/key");
    ASSERT_TRUE(gr.has_value());
    ASSERT_EQ(gr->attributes.at("cas_owner"), "ab:7:42");
}

// =====================================================================
// getStream seam (forward-only reads of write-once objects)
// =====================================================================

TEST(CASBackendStream, StreamsBodyWindow)
{
    auto backend = std::make_shared<InMemoryBackend>();
    backend->putIfAbsent("k", "0123456789");
    auto got = backend->getStream("k", DB::Cas::Range{.offset = 2, .length = 5});
    ASSERT_TRUE(got.has_value());
    String out;
    DB::readStringUntilEOF(out, *got->stream);
    EXPECT_EQ(out, "23456");
    EXPECT_FALSE(got->token.empty());
    EXPECT_FALSE(backend->getStream("absent").has_value());
}

// =====================================================================
// B168 P0: InstrumentedBackend per-namespace/op ProfileEvents
// =====================================================================

namespace ProfileEvents
{
extern const Event CASBlobPut;
extern const Event CASBlobPutDeduplicated;
extern const Event CASBlobHead;
extern const Event CASBlobHeadMiss;
extern const Event CASGCCompareSwap;
}

TEST(CASInstrumentedBackend, ClassifierAndPerNamespaceOpEvents)
{
    /// Namespace classification by substring.
    EXPECT_EQ(classifyCasNs("pool/blobs/ab/abcdef"), CasNs::Blob);
    EXPECT_EQ(classifyCasNs("pool/gc/registry"), CasNs::Gc);   /// gc/ prefix covers GC state (state, retired sets, etc.)
    EXPECT_EQ(classifyCasNs("pool/roots/default/_files/x"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/gc/state"), CasNs::Gc);
    /// D3: the old per-server-control key shapes (`_watermark`, `_precommits/<n>`) have no producer
    /// anymore -- control state now lives under `/gc/server-roots/...` (classifies as Gc). A key of
    /// this legacy shape, if it ever showed up, would fall through to the generic /roots/ rule.
    EXPECT_EQ(classifyCasNs("pool/roots/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/_watermark"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/roots/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/_precommits/3"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/_pool_meta"), CasNs::Other);
    /// Final opaque-life layout: both immutable streams and point/path-addressed state remain Root
    /// instrumentation, while part manifests remain Manifest. None may fall into Other (the
    /// 2026-07-03 operator-stand CREATE storm misread as CASOtherHeadMiss=102 because of this).
    EXPECT_EQ(classifyCasNs("pool/cas/ns/stream/00000000000000000000000000000017/_log/1-1.zst"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/cas/ns/state/00000000000000000000000000000017/_ckpt.zst"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/cas/ns/state/00000000000000000000000000000017/_files/format_version.txt"), CasNs::Root);
    EXPECT_EQ(classifyCasNs("pool/cas/manifests/0/srv/store/d18/uuid@cas@/24/1/000001.proto"), CasNs::Manifest);

    auto inner = std::make_shared<InMemoryBackend>();
    InstrumentedBackend b(inner);

    using ProfileEvents::global_counters;
    const auto blob_put_before   = global_counters[ProfileEvents::CASBlobPut].load();
    const auto blob_dedup_before = global_counters[ProfileEvents::CASBlobPutDeduplicated].load();
    const auto blob_head_before  = global_counters[ProfileEvents::CASBlobHead].load();
    const auto blob_miss_before  = global_counters[ProfileEvents::CASBlobHeadMiss].load();
    const auto gc_cas_before     = global_counters[ProfileEvents::CASGCCompareSwap].load();

    const String blob_key = "pool/blobs/ab/abcdef0123456789";

    /// First put of a blob ⇒ Put.
    EXPECT_EQ(b.putIfAbsent(blob_key, "payload").outcome, PutOutcome::Done);
    /// Second put of the same key ⇒ PutDeduplicated (content already exists).
    EXPECT_EQ(b.putIfAbsent(blob_key, "payload").outcome, PutOutcome::PreconditionFailed);
    /// head of an absent blob key ⇒ HeadMiss (the 404 signal).
    EXPECT_FALSE(b.head("pool/blobs/zz/absent").exists);
    /// head of the present blob key ⇒ Head.
    EXPECT_TRUE(b.head(blob_key).exists);
    /// casPut create on a gc key ⇒ Gc Cas.
    EXPECT_EQ(b.casPut("pool/gc/state", "g1", std::nullopt).outcome, CasOutcome::Committed);
    /// Streaming put to a fresh blob key, then finalize ⇒ Put.
    {
        auto sink = b.putIfAbsentStream("pool/blobs/cd/cafebabe");
        ASSERT_TRUE(sink != nullptr);
        DB::writeString(String("streamed"), sink->buffer());
        EXPECT_EQ(sink->finalize().outcome, PutOutcome::Done);
    }

    /// Under coverage builds ProfileEvents propagate into a thread-local subtree that does not reach
    /// `global_counters`; deltas read 0 there only (see gtest_unique_key_index_cache).
#if !WITH_COVERAGE
    EXPECT_EQ(global_counters[ProfileEvents::CASBlobPut].load()      - blob_put_before,   2u);
    EXPECT_EQ(global_counters[ProfileEvents::CASBlobPutDeduplicated].load() - blob_dedup_before, 1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASBlobHead].load()     - blob_head_before,  1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASBlobHeadMiss].load() - blob_miss_before,  1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASGCCompareSwap].load()        - gc_cas_before,     1u);
#else
    (void)blob_put_before; (void)blob_dedup_before; (void)blob_head_before;
    (void)blob_miss_before; (void)gc_cas_before;
#endif
}

// =====================================================================
// M-C2 Task 2: typed S3 precondition signal
// =====================================================================

#if USE_AWS_S3

/// The Native conditional-PUT path discriminates a lost precondition by the canonical S3 error code
/// string ("PreconditionFailed", "NoSuchKey", ...) that `S3Exception` carries from the response XML
/// `<Code>` — a 412 is UNMODELED for the AWS SDK (the enum value is UNKNOWN), so the name is the only
/// machine-readable signal.
TEST(CASS3Signal, S3ExceptionCarriesCanonicalErrorName)
{
    DB::S3Exception e("412 from backend", Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed");
    EXPECT_EQ(e.getExceptionName(), "PreconditionFailed");
    DB::S3Exception bare("no name attached", Aws::S3::S3Errors::UNKNOWN);
    EXPECT_TRUE(bare.getExceptionName().empty());
}

namespace
{

/// WriteBuffer stub whose finalize throws a configured S3Exception — drives the classifier directly.
class ThrowOnFinalizeBuffer final : public DB::WriteBuffer
{
public:
    ThrowOnFinalizeBuffer() : DB::WriteBuffer(nullptr, 0) {}

    explicit ThrowOnFinalizeBuffer(DB::S3Exception e) : DB::WriteBuffer(nullptr, 0), to_throw(std::move(e)) {}

private:
    void nextImpl() override {}

    void finalizeImpl() override
    {
        if (to_throw)
            throw *to_throw; /// NOLINT(cert-err09-cpp,cert-err60-cpp,cert-err61-cpp,misc-throw-by-value-catch-by-reference) -- the mock stores the configured exception to throw later, so it cannot be an anonymous temporary
    }

    std::optional<DB::S3Exception> to_throw;
};

}

/// detail::finalizeConditionalWrite maps a lost precondition to an OUTCOME by exact-matching the
/// canonical S3 error name (plus the modeled NO_SUCH_KEY enum, which WriteBufferFromS3 surfaces
/// nameless on retry exhaustion) and rethrows anything else.
TEST(CASS3Signal, FinalizeClassifierMapsPreconditionLossExactly)
{
    using DB::Cas::detail::finalizeConditionalWrite;

    auto classify = [](DB::S3Exception e)
    {
        ThrowOnFinalizeBuffer buf(std::move(e));
        return finalizeConditionalWrite(buf);
    };

    EXPECT_EQ(classify(DB::S3Exception("412", Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed")),
              PutOutcome::PreconditionFailed);
    EXPECT_EQ(classify(DB::S3Exception("404 gone under If-Match", Aws::S3::S3Errors::UNKNOWN, "NoSuchKey")),
              PutOutcome::PreconditionFailed);
    EXPECT_EQ(classify(DB::S3Exception("retries exhausted, no name attached", Aws::S3::S3Errors::NO_SUCH_KEY)),
              PutOutcome::PreconditionFailed);

    ThrowOnFinalizeBuffer unrelated(DB::S3Exception("503", Aws::S3::S3Errors::UNKNOWN, "SlowDown"));
    EXPECT_THROW(finalizeConditionalWrite(unrelated), DB::S3Exception);

    ThrowOnFinalizeBuffer clean;
    EXPECT_EQ(finalizeConditionalWrite(clean), PutOutcome::Done);
}

namespace
{

/// A `LocalObjectStorage` that round-trips user metadata in-process. The production
/// `LocalObjectStorage` deliberately drops the `attributes` argument of `writeObject` and never
/// populates `ObjectMetadata::attributes` (local files carry no `x-amz-meta-*`), so it cannot stand
/// in for S3/RustFS when verifying the metadata threading. This test-only subclass records the
/// attributes passed on write, keyed by physical path, and injects them back on metadata reads —
/// exactly what a real object store does for `x-amz-meta-*`. It exercises the `EmulatedSingleProcess`
/// `ObjectStorageBackend` threading (`putIfAbsent` → `writeObject` attributes → `head` attributes)
/// without a live S3 backend; the real S3/RustFS round trip is verified empirically out-of-band.
class AttributePreservingLocalObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object,
        DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> attributes,
        size_t buf_size,
        const DB::WriteSettings & write_settings) override
    {
        if (attributes.has_value())
        {
            std::lock_guard lock(mutex);
            saved_attributes[object.remote_path] = *attributes;
        }
        return DB::LocalObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
        if (metadata)
            inject(path, *metadata);
        return metadata;
    }

    DB::ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = DB::LocalObjectStorage::getObjectMetadata(path, with_tags);
        inject(path, metadata);
        return metadata;
    }

private:
    void inject(const std::string & path, DB::ObjectMetadata & metadata) const
    {
        std::lock_guard lock(mutex);
        if (auto it = saved_attributes.find(path); it != saved_attributes.end())
            metadata.attributes = it->second;
    }

    mutable std::mutex mutex;
    mutable std::map<std::string, DB::ObjectAttributes> saved_attributes;
};

DB::ObjectStoragePtr makeAttributePreservingStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_meta_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<AttributePreservingLocalObjectStorage>(std::move(settings));
}

}

/// The `EmulatedSingleProcess` `ObjectStorageBackend` must thread user metadata through to the
/// underlying object storage's `writeObject` attributes on `putIfAbsent` and read it back into
/// `HeadResult::attributes` on `head`. Verified here over an attribute-preserving object storage
/// (the production `LocalObjectStorage` drops attributes); the live S3/RustFS round trip is verified
/// empirically out-of-band.
TEST(CASObjectStorageBackend, EmulatedRoundTripsUserMetadata)
{
    ObjectStorageBackend backend(makeAttributePreservingStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);

    const DB::Cas::ObjectMeta meta{{"cas_owner", "ab:7:42"}};
    ASSERT_EQ(backend.putIfAbsent("k/key", "body", meta).outcome, DB::Cas::PutOutcome::Done);

    const auto hr = backend.head("k/key");
    ASSERT_TRUE(hr.exists);
    ASSERT_EQ(hr.attributes.at("cas_owner"), "ab:7:42");
}

namespace
{

/// A `LocalObjectStorage` whose `readObject` throws `S3Exception(NO_SUCH_KEY)` for a configured
/// physical key, while `tryGetObjectMetadata` still reports that key as PRESENT.
/// This simulates the HEAD→GET race window: the HEAD succeeds, then the object is deleted before
/// the GET arrives.
class NativeReadThrowsNoSuchKeyObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    void setThrowOnRead(const std::string & path)
    {
        throw_on_read_path = path;
    }

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject & object,
        const DB::ReadSettings & read_settings,
        std::optional<size_t> read_hint,
        bool use_external_buffer,
        bool restrict_seek) const override
    {
        if (object.remote_path == throw_on_read_path)
            throw DB::S3Exception(
                "NoSuchKey: The specified key does not exist.",
                Aws::S3::S3Errors::NO_SUCH_KEY);

        return DB::LocalObjectStorage::readObject(object, read_settings, read_hint, use_external_buffer, restrict_seek);
    }

private:
    std::string throw_on_read_path;
};

struct ThrowOnReadFixture
{
    DB::ObjectStoragePtr storage;
    /// Anchored under `storage`'s own root, because `Mode::Native` hands the key to the object storage
    /// verbatim and this one is a real filesystem.
    std::string key;
};

ThrowOnReadFixture makeThrowOnReadStorageForTest(const std::string & key_suffix)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_midget_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    auto storage = std::make_shared<NativeReadThrowsNoSuchKeyObjectStorage>(std::move(settings));
    const std::string key = DB::Cas::tests::nativeKeyUnder(storage, key_suffix);

    /// Write the object so tryGetObjectMetadata reports it present (HEAD succeeds).
    {
        auto buf = storage->writeObject(DB::StoredObject(key), DB::WriteMode::Rewrite, std::nullopt);
        buf->write("content", 7);
        buf->finalize();
    }

    /// Now configure: future readObject calls for this key will throw NO_SUCH_KEY.
    storage->setThrowOnRead(key);
    return {std::move(storage), key};
}

}

/// `ObjectStorageBackend::get` in `Native` mode: when `tryGetObjectMetadata` (`nativeHead`) reports the
/// key PRESENT but `readObject` throws `S3Exception(NO_SUCH_KEY)` — simulating a deletion in the
/// HEAD→GET window — `get` MUST return `std::nullopt` rather than letting the raw exception escape.
TEST(CASObjectStorageBackend, NativeModeGetReturnsNulloptOnMidGetNoSuchKey)
{
    /// The Native mode backend uses the key verbatim as the physical path (no emu_root prefix), so the
    /// logical key IS the physical one the fixture wrote and armed.
    const auto fixture = makeThrowOnReadStorageForTest("pool/blobs/ab/abcdef0123456789abcdef0123456789");

    ObjectStorageBackend backend(fixture.storage, ObjectStorageBackend::Mode::Native);

    /// `get` HEADs before it reads and answers nullopt for an absent key, so without this the nullopt
    /// below would be satisfied by an object the fixture failed to place — the mid-GET race would go
    /// untested and the case would still pass.
    Backend & iface = backend;
    ASSERT_TRUE(iface.head(fixture.key).exists);

    /// HEAD reports the key present; readObject then throws NO_SUCH_KEY.
    /// Contract: get must return std::nullopt, not propagate the S3Exception.
    /// Call through the base-class interface so the default `Range{}` arg is available.
    const auto result = iface.get(fixture.key);
    EXPECT_FALSE(result.has_value());
}

/// A ranged `get` over a real `LocalObjectStorage` returns exactly the requested window, with the
/// same clamping the old read-whole-then-substr path had: a window whose offset is at or past EOF
/// yields an empty result. The only-the-window I/O property (no whole-object read) is enforced by
/// the `readObjectRanged` rewrite and cross-checked by the request-size gate in a later task.
TEST(CASObjectStorageBackend, RangedGetReadsOnlyTheWindow)
{
    auto backend = std::make_shared<ObjectStorageBackend>(
        tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);

    const String payload = String(300000, 'a') + String(300000, 'b') + String(300000, 'c');
    backend->putIfAbsent("p/obj", payload);

    const auto mid = backend->get("p/obj", DB::Cas::Range{.offset = 300000, .length = 300000});
    ASSERT_TRUE(mid.has_value());
    EXPECT_EQ(mid->bytes, String(300000, 'b'));

    const auto tail = backend->get("p/obj", DB::Cas::Range{.offset = 600000, .length = std::nullopt});
    ASSERT_TRUE(tail.has_value());
    EXPECT_EQ(tail->bytes, String(300000, 'c'));

    const auto past = backend->get("p/obj", DB::Cas::Range{.offset = 1000000, .length = 10});
    ASSERT_TRUE(past.has_value());
    EXPECT_TRUE(past->bytes.empty());
}

/// codex-review-triage §3.18, finding 19c: the `EmulatedSingleProcess` adapter used to mint tokens
/// from a plain in-process counter (`emu_seq`), NOT actually seeded from the underlying object's etag
/// despite the class comment's claim. After a process restart (modeled here as a fresh
/// `ObjectStorageBackend` instance over the SAME storage) the counter restarts at 0 and can re-mint a
/// value that TEXTUALLY collides with a token persisted before the restart (e.g. a GC condemned-delete
/// token queued for replay), even though the two values name completely different incarnations of the
/// key. `deleteExact` must never let a stale, pre-restart token match a freshly recreated object.
TEST(CASObjectStorageBackend, EmuTokenSurvivesProcessRestartAcrossRecreate)
{
    auto storage = tests::makeLocalObjectStorageForTest();

    auto backend1 = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);
    /// A throwaway prior mutation on a DIFFERENT key: with the old counter this advances backend1's
    /// process-wide op counter to 1, so "k/restart"'s own mint below lands on 2 — chosen so it collides
    /// with backend2's post-restart recreate mint further down (also its SECOND op; see there).
    ASSERT_EQ(backend1->putIfAbsent("k/other", "junk").outcome, PutOutcome::Done);
    ASSERT_EQ(backend1->putIfAbsent("k/restart", "v1").outcome, PutOutcome::Done);
    const Token stale_token = backend1->head("k/restart").token;

    /// Simulate a process restart: a brand-new `ObjectStorageBackend` instance (fresh emu state) over
    /// the SAME underlying storage — exactly what happens when the CAS process restarts.
    auto backend2 = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);

    /// Delete and recreate the key through the NEW instance — a fresh incarnation with a fresh mtime.
    /// This is backend2's first-ever op (op 1) then a delete (no mint) then the recreate (op 2) — the
    /// same op-index as `stale_token` above under the old counter, so the two textually collide there.
    const Token current = backend2->head("k/restart").token;
    ASSERT_EQ(backend2->deleteExact("k/restart", current).kind, DeleteOutcome::Kind::Deleted);
    ASSERT_EQ(backend2->putIfAbsent("k/restart", "v2-after-restart").outcome, PutOutcome::Done);

    /// The pre-restart token must NEVER match the post-restart incarnation, however coincidentally a
    /// process-local counter would have re-minted the identical textual value.
    const auto stale_delete = backend2->deleteExact("k/restart", stale_token);
    EXPECT_EQ(stale_delete.kind, DeleteOutcome::Kind::TokenMismatch);

    /// The live (post-restart) incarnation must be untouched by the rejected stale delete.
    EXPECT_TRUE(backend2->head("k/restart").exists);
}

/// codex-review-triage §3.18, finding №18: `list`'s `EmulatedSingleProcess` branch minted its per-key
/// token via `tokenForList`, which always stamps `native_token_type` (ETag) REGARDLESS of `mode` --
/// while `head`/`get` mint `TokenType::Emulated`. `Token::operator==` compares type AND value, so a
/// list-derived token could never satisfy an emulated `deleteExact`/`putOverwrite` expectation: a
/// fail-safe leak (never a wrong delete), but every consumer of listed tokens (GC namespace cleanup,
/// `deletePrefixWholesale`, orphan sweep, decommission drain) always saw `TokenMismatch` against a
/// LOCAL pool. `list` must surface the SAME (type, value) as `head` for the same key.
TEST(CASObjectStorageBackend, EmulatedListTokenMatchesHeadToken)
{
    auto backend = std::make_shared<ObjectStorageBackend>(
        tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);

    ASSERT_EQ(backend->putIfAbsent("k/listed", "body").outcome, PutOutcome::Done);

    const Token head_token = backend->head("k/listed").token;
    ASSERT_EQ(head_token.type, TokenType::Emulated);

    const ListPage page = backend->list("k/", "", /*limit=*/10);
    ASSERT_EQ(page.keys.size(), 1u);
    ASSERT_TRUE(page.keys.front().token.has_value());
    EXPECT_EQ(*page.keys.front().token, head_token);
}

namespace
{

/// A `LocalObjectStorage` whose reported etag never changes -- simulating a filesystem/clock whose
/// mtime resolution is too coarse to separate two writes issued back-to-back (the "same mtime
/// quantum" hazard flagged for the etag-seeded emu token: two DIFFERENT incarnations must still mint
/// DIFFERENT tokens even when the storage's own etag does not advance between them).
class FixedEtagLocalObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
        if (metadata)
            metadata->etag = "same-quantum";
        return metadata;
    }
};

DB::ObjectStoragePtr makeFixedEtagStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_fixed_etag_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<FixedEtagLocalObjectStorage>(std::move(settings));
}

}

/// The mtime-resolution guard (codex-review-triage §3.18, 19c step 4): two writes to the same key
/// whose underlying etag does not advance between them (stubbed here to model a coarse clock) must
/// still mint DISTINCT emulated tokens, and a stale token from the first incarnation must not match
/// the second.
TEST(CASObjectStorageBackend, EmuTokenDisambiguatesSameEtagRewrite)
{
    ObjectStorageBackend backend(makeFixedEtagStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);

    const auto put1 = backend.putIfAbsent("k/tick", "v1");
    ASSERT_EQ(put1.outcome, PutOutcome::Done);
    const auto put2 = backend.putOverwrite("k/tick", "v2", put1.token);
    ASSERT_EQ(put2.outcome, PutOutcome::Done);

    EXPECT_NE(put1.token.value, put2.token.value);
    EXPECT_EQ(put1.token.type, TokenType::Emulated);
    EXPECT_EQ(put2.token.type, TokenType::Emulated);

    /// A stale delete using the FIRST incarnation's token must not match the live (second) one.
    EXPECT_EQ(backend.deleteExact("k/tick", put1.token).kind, DeleteOutcome::Kind::TokenMismatch);
    EXPECT_TRUE(backend.head("k/tick").exists);
}

namespace
{

/// A `LocalObjectStorage` that always reports a caller-supplied, fixed NUMERIC etag string — lets a
/// test pin `emuMintToken`'s etag input to a precise, controlled nanosecond value (an old timestamp
/// vs. one close to "now") regardless of the real filesystem clock. Used to test the
/// `emu_token_state` erase-on-delete bound (codex-review-triage §3.18, Important #1): the entry
/// must be erased only when the deleted incarnation's own etag is comfortably in the past.
class FixedNumericEtagLocalObjectStorage final : public DB::LocalObjectStorage
{
public:
    FixedNumericEtagLocalObjectStorage(DB::LocalObjectStorageSettings settings, String etag_)
        : DB::LocalObjectStorage(std::move(settings)), etag(std::move(etag_))
    {
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
        if (metadata)
            metadata->etag = etag;
        return metadata;
    }

private:
    String etag;
};

DB::ObjectStoragePtr makeFixedNumericEtagStorageForTest(const String & etag)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_fixed_numeric_etag_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<FixedNumericEtagLocalObjectStorage>(std::move(settings), etag);
}

class ClockEtagLocalObjectStorage final : public DB::LocalObjectStorage
{
public:
    ClockEtagLocalObjectStorage(DB::LocalObjectStorageSettings settings, std::shared_ptr<std::atomic<uint64_t>> now_ns_)
        : DB::LocalObjectStorage(std::move(settings)), now_ns(std::move(now_ns_))
    {
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
        if (metadata)
            metadata->etag = std::to_string(now_ns->load());
        return metadata;
    }

private:
    std::shared_ptr<std::atomic<uint64_t>> now_ns;
};

DB::ObjectStoragePtr makeClockEtagStorageForTest(const std::shared_ptr<std::atomic<uint64_t>> & now_ns)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_clock_etag_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<ClockEtagLocalObjectStorage>(std::move(settings), now_ns);
}

}

/// codex-review-triage §3.18, Important #1: `emu_token_state` must be BOUNDED, not grow for the
/// lifetime of the backend instance. `deleteExact` erases a key's entry only when its last-minted
/// etag is comfortably (>= 2s) in the past — recent enough to still collide with an immediate
/// same-process recreate must be RETAINED (the mtime-quantum guard stays intact).
TEST(CASObjectStorageBackend, DeleteExactErasesEmuTokenStateOnlyWhenEtagIsComfortablyOld)
{
    /// An etag far in the past (nanoseconds since epoch, ~2001): delete must erase the entry, so an
    /// immediate recreate reporting the SAME fixed etag is treated as a brand-new incarnation (bare
    /// etag, no disambiguator) rather than a same-quantum tie with the just-consumed delete token.
    {
        const String old_etag = "1000000000000000000";
        ObjectStorageBackend backend(makeFixedNumericEtagStorageForTest(old_etag), ObjectStorageBackend::Mode::EmulatedSingleProcess);

        const auto put1 = backend.putIfAbsent("k/old", "v1");
        ASSERT_EQ(put1.outcome, PutOutcome::Done);
        ASSERT_EQ(put1.token.value, old_etag);
        ASSERT_EQ(backend.deleteExact("k/old", put1.token).kind, DeleteOutcome::Kind::Deleted);

        const auto put2 = backend.putIfAbsent("k/old", "v2");
        ASSERT_EQ(put2.outcome, PutOutcome::Done);
        EXPECT_EQ(put2.token.value, old_etag) << "entry should have been erased on delete (etag comfortably old), "
                                                  "so the recreate mints the bare etag, not a disambiguated one";
    }

    /// An etag within the safety margin of "now": delete must RETAIN the entry, so the same
    /// immediate-recreate scenario still gets disambiguated -- the guard this bound must not break.
    {
        const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        const String recent_etag = std::to_string(now_ns);
        ObjectStorageBackend backend(makeFixedNumericEtagStorageForTest(recent_etag), ObjectStorageBackend::Mode::EmulatedSingleProcess);

        const auto put1 = backend.putIfAbsent("k/fresh", "v1");
        ASSERT_EQ(put1.outcome, PutOutcome::Done);
        ASSERT_EQ(put1.token.value, recent_etag);
        ASSERT_EQ(backend.deleteExact("k/fresh", put1.token).kind, DeleteOutcome::Kind::Deleted);

        const auto put2 = backend.putIfAbsent("k/fresh", "v2");
        ASSERT_EQ(put2.outcome, PutOutcome::Done);
        EXPECT_EQ(put2.token.value, recent_etag + "#1") << "entry should have been RETAINED on delete (etag recent), "
                                                            "so the recreate is disambiguated against it";
    }
}

TEST(CASObjectStorageBackend, EmuTokenStateEventuallyPrunesDistinctShortLivedKeys)
{
    constexpr uint64_t start_ns = 1'700'000'000'000'000'000ULL;
    constexpr uint64_t step_ns = 100'000'000ULL;
    constexpr size_t key_count = 128;
    constexpr size_t expected_recent_key_bound = 24;

    auto now_ns = std::make_shared<std::atomic<uint64_t>>(start_ns);
    ObjectStorageBackend backend(makeClockEtagStorageForTest(now_ns), ObjectStorageBackend::Mode::EmulatedSingleProcess);

    for (size_t i = 0; i < key_count; ++i)
    {
        const uint64_t current_ns = start_ns + i * step_ns;
        now_ns->store(current_ns);
        backend.setEmuNowNsForTest(current_ns);

        const String key = "k/short-lived-" + std::to_string(i);
        const auto put = backend.putIfAbsent(key, "body");
        ASSERT_EQ(put.outcome, PutOutcome::Done);
        ASSERT_EQ(backend.deleteExact(key, put.token).kind, DeleteOutcome::Kind::Deleted);
    }

    const uint64_t sweep_ns = start_ns + key_count * step_ns + 2'000'000'000ULL;
    now_ns->store(sweep_ns);
    backend.setEmuNowNsForTest(sweep_ns);
    const auto trigger = backend.putIfAbsent("k/sweep-trigger", "body");
    ASSERT_EQ(trigger.outcome, PutOutcome::Done);
    ASSERT_EQ(backend.deleteExact("k/sweep-trigger", trigger.token).kind, DeleteOutcome::Kind::Deleted);

    EXPECT_LE(backend.emuTokenStateSizeForTest(), expected_recent_key_bound)
        << "token state should track only the bounded recent-key window, not all " << key_count << " deleted keys";
}

namespace
{

/// A `LocalObjectStorage` that counts `writeObject`/`removeObjectIfTokenMatches` calls -- used to
/// prove that a wrong-dialect expected token is rejected LOCALLY, before anything reaches the wire.
class CallCountingObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object,
        DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> attributes,
        size_t buf_size,
        const DB::WriteSettings & write_settings) override
    {
        ++write_calls;
        return DB::LocalObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
    }

    DB::ConditionalRemoveResult removeObjectIfTokenMatches(const DB::StoredObject & object, const std::string & etag) override
    {
        ++remove_if_matches_calls;
        return DB::LocalObjectStorage::removeObjectIfTokenMatches(object, etag);
    }

    std::atomic<int> write_calls{0};
    std::atomic<int> remove_if_matches_calls{0};
};

DB::ObjectStoragePtr makeCallCountingStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_call_counting_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<CallCountingObjectStorage>(std::move(settings));
}

}

/// codex-review-triage §3.18, finding №19: Native-mode conditional mutations forward only
/// `Token::value` to the wire (`object_storage_write_if_match` / `removeObjectIfTokenMatches`),
/// blind to `Token::type`. A wrong-dialect token whose VALUE happens to equal the live incarnation's
/// must be rejected LOCALLY -- before any wire call is made -- never merely rely on the remote
/// backend to reject a foreign-dialect value it was never designed to compare.
TEST(CASObjectStorageBackend, NativeRejectsWrongDialectTokenBeforeTouchingTheWire)
{
    auto storage = std::static_pointer_cast<CallCountingObjectStorage>(makeCallCountingStorageForTest());
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    ASSERT_EQ(backend.putIfAbsent("k/dialect", "v1").outcome, PutOutcome::Done);
    const Token live = backend.head("k/dialect").token;
    ASSERT_EQ(live.type, TokenType::ETag);

    storage->write_calls = 0;
    storage->remove_if_matches_calls = 0;

    /// Same wire VALUE, wrong dialect TYPE (Emulated instead of this backend's native ETag dialect).
    const Token wrong_type_token{live.value, TokenType::Emulated};

    EXPECT_EQ(backend.putOverwrite("k/dialect", "v2", wrong_type_token).outcome, PutOutcome::PreconditionFailed);
    EXPECT_EQ(backend.casPut("k/dialect", "v2", wrong_type_token).outcome, CasOutcome::Conflict);
    EXPECT_EQ(backend.deleteExact("k/dialect", wrong_type_token).kind, DeleteOutcome::Kind::TokenMismatch);

    EXPECT_EQ(storage->write_calls.load(), 0);
    EXPECT_EQ(storage->remove_if_matches_calls.load(), 0);

    /// The live incarnation must be untouched by all three rejected attempts.
    EXPECT_EQ(backend.head("k/dialect").token, live);
}

/// §1 (opt round-B): the fold/point GETs read tiny bodies but a default `ReadBufferFromS3` preallocates
/// ~1 MiB. `casSizedReadSettings` shrinks the buffer to the known body size + slack, capped at the
/// caller's default — never larger than before, regardless of the reported size.
TEST(CASSizedReadSettings, CapsToKnownSizePlusSlackButNeverAboveBase)
{
    DB::ReadSettings base;
    base.remote_fs_settings.buffer_size = 1ULL << 20;   /// 1 MiB default
    base.local_fs_settings.buffer_size = 1ULL << 20;

    /// A ~3.7 KB fold body: buffer shrinks to size + slack, far below the 1 MiB default.
    const auto small = DB::Cas::casSizedReadSettings(base, 3700);
    EXPECT_EQ(small.remote_fs_settings.buffer_size, 3700 + DB::Cas::CAS_FOLD_READ_SLACK_BYTES);
    EXPECT_EQ(small.local_fs_settings.buffer_size, 3700 + DB::Cas::CAS_FOLD_READ_SLACK_BYTES);

    /// A body larger than the default is capped AT the default (never grown).
    const auto big = DB::Cas::casSizedReadSettings(base, 8ULL << 20);
    EXPECT_EQ(big.remote_fs_settings.buffer_size, 1ULL << 20);

    /// Unknown size (0) = leave the base untouched (the metadata-fetch fallback path).
    const auto unknown = DB::Cas::casSizedReadSettings(base, 0);
    EXPECT_EQ(unknown.remote_fs_settings.buffer_size, 1ULL << 20);
}

/// The CountingBackend request-shape recorders that the streaming-memory gates (Task 3/4) consume:
/// per-key/total getStream counts, the max ranged-get window per key, and the whole-object get flag.
TEST(CountingBackendShape, RecordsGetStreamAndRangeShape)
{
    DB::Cas::tests::CountingBackend backend;
    backend.putIfAbsent("k", String(1000, 'x'));

    /// A whole-object get flags the resident-memory violation; a ranged get tracks the max window.
    backend.get("k");
    backend.get("k", DB::Cas::Range{.offset = 0, .length = 100});
    backend.get("k", DB::Cas::Range{.offset = 10, .length = 400});
    EXPECT_EQ(backend.wholeGetCount("k"), 1u);
    EXPECT_EQ(backend.maxRangedGetLen("k"), 400u);

    /// getStream counters (per-key and total).
    backend.getStream("k", DB::Cas::Range{.offset = 2, .length = 5});
    backend.getStream("k");
    backend.getStream("absent");
    EXPECT_EQ(backend.getStreamCount("k"), 2u);
    EXPECT_EQ(backend.getStreamTotal(), 3u);

    backend.resetCounts();
    EXPECT_EQ(backend.wholeGetCount("k"), 0u);
    EXPECT_EQ(backend.maxRangedGetLen("k"), 0u);
    EXPECT_EQ(backend.getStreamTotal(), 0u);
}

#endif
