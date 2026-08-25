#include <gtest/gtest.h>
#include <algorithm>
#include <cmath>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/tests/cas_test_helpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Poco/Exception.h>

namespace ProfileEvents
{
extern const Event CASMetaPut;
extern const Event CASMetaCompareSwap;
extern const Event CASMetaCreateClean;
extern const Event CASMetaAdoptBackfill;
extern const Event CASMetaResurrectClean;
extern const Event CASBlobAdoptTrusted;
}

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int FILE_DOESNT_EXIST;
extern const int INVALID_STATE;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int ABORTED;
extern const int CORRUPTED_DATA;
extern const int LIMIT_EXCEEDED;
extern const int NETWORK_ERROR;
extern const int UNKNOWN_EXCEPTION;
}

using namespace DB::Cas;
using DB::Cas::tests::condemnMeta;
using DB::Cas::tests::expectThrowsCode;
using DB::Cas::tests::idOf;
using DB::Cas::tests::injectRetire;
using DB::Cas::tests::loadMetaForTest;
using DB::Cas::tests::streamingHexOf;
using DB::Cas::tests::u128Of;
using DB::Cas::tests::writeMetaClean;
using DB::Cas::tests::writeRawBlobBody;

namespace
{

PoolPtr openPool(const std::shared_ptr<InMemoryBackend> & b)
{
    return Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// Start a build whose owning manifest namespace + final ref name are `ns`/`ref` (promote/stageManifest
/// derive the manifest namespace by splitting PartWriteInfo::intended_ref on the LAST '/').
PartWriteTxnPtr startBuildFor(const PoolPtr & s, const RootNamespace & ns, const String & ref)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    return s->beginPartWrite(info);
}

/// A one-entry Blob ManifestEntry for `payload` at `path` (the build's stageManifest entry).
ManifestEntry blobManifestEntry(const String & path, const String & payload)
{
    ManifestEntry e;
    e.path = path;
    e.placement = EntryPlacement::Blob;
    e.ref = BlobRef{BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();
    return e;
}

/// The streaming (production-convention) `BlobRef` of `payload` — CityHash128 at the write width.
BlobRef streamRefOf(const String & payload)
{
    return BlobRef{BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hexToU128(streamingHexOf(payload)))};
}

ManifestEntry blobManifestEntryStreaming(const String & path, const String & payload)
{
    ManifestEntry e;
    e.path = path;
    e.placement = EntryPlacement::Blob;
    e.ref = streamRefOf(payload);

    e.blob_size = payload.size();
    return e;
}

PartWriteTxnPtr precommittedBuildForPayload(
    const PoolPtr & store, const RootNamespace & ns, const String & ref, const String & payload)
{
    auto build = startBuildFor(store, ns, ref);
    const ManifestId manifest = build->stageManifest({blobManifestEntry("data.bin", payload)});
    build->precommitAdd(ns, ref, manifest);
    return build;
}

ManifestId durablyPrecommit(
    const PartWriteTxnPtr & build,
    const RootNamespace & ns,
    const String & ref,
    std::vector<ManifestEntry> entries)
{
    const ManifestId manifest = build->stageManifest(std::move(entries));
    build->precommitAdd(ns, ref, manifest);
    return manifest;
}

/// The full single-blob write flow (EDGE-BEFORE-OBSERVE wiring order):
/// stageManifest(one entry) -> precommitAdd -> putBlob -> promote. Returns the committed ManifestId.
ManifestId publishOneBlobPart(
    const PoolPtr & s, const RootNamespace & ns, const String & ref, const String & path, const String & payload)
{
    auto build = startBuildFor(s, ns, ref);
    const ManifestId id = build->stageManifest({blobManifestEntry(path, payload)});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

/// A one-shot backend hook (mirrors the WriteCountingBackend delegation pattern in gtest_cas_pool.cpp):
/// it delegates every op to a wrapped Backend, but the FIRST time head(target_key) is called it fires a
/// deleteExact(target_key, condemned_token) AFTER computing the (present) HEAD result and BEFORE returning
/// it — simulating GC's exact-token content delete landing in the writer's HEAD->GET window (B136).
class HeadThenDeleteOnceBackend final : public DB::Cas::Backend
{
public:
    HeadThenDeleteOnceBackend(BackendPtr inner_, String target_key_, DB::Cas::Token condemned_)
        : inner(std::move(inner_)), target_key(std::move(target_key_)), condemned(condemned_) {}

    DB::Cas::HeadResult head(const String & k) override
    {
        const DB::Cas::HeadResult hr = inner->head(k);
        if (k == target_key && !fired)
        {
            fired = true;
            /// GC's single content-delete site, landing in the HEAD->GET window.
            inner->deleteExact(target_key, condemned);
        }
        return hr;
    }

    std::optional<DB::Cas::GetResult> get(const String & k, DB::Cas::Range r) override { return inner->get(k, r); }
    std::optional<DB::Cas::GetStreamResult> getStream(const String & k, DB::Cas::Range r) override { return inner->getStream(k, r); }
    DB::Cas::ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
    DB::Cas::PutResult putIfAbsent(const String & k, const String & b, const DB::Cas::ObjectMeta & meta) override { return inner->putIfAbsent(k, b, meta); }
    void publishBlob(const DB::Cas::BlobPublishRequest & request) override
    {
        inner->publishBlob(request);
    }
    DB::Cas::PutResult putOverwrite(const String & k, const String & b, const DB::Cas::Token & e, const DB::Cas::ObjectMeta & meta) override { return inner->putOverwrite(k, b, e, meta); }
    DB::Cas::CasResult casPut(const String & k, const String & b, const std::optional<DB::Cas::Token> & e, const DB::Cas::ObjectMeta & meta) override { return inner->casPut(k, b, e, meta); }
    DB::Cas::DeleteOutcome deleteExact(const String & k, const DB::Cas::Token & t) override { return inner->deleteExact(k, t); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }

private:
    BackendPtr inner;
    String target_key;
    DB::Cas::Token condemned;
    bool fired = false;
};

/// A delegating backend that counts head()/get() calls per key. Lets a test assert the promote gate
/// performs ZERO per-file probes on a TRUSTED adopted leaf (§4 manifest-trust): no presence HEAD on
/// the blob key, no loadMeta GET on the blob-meta key.
class KeyCountingBackend final : public DB::Cas::Backend
{
public:
    explicit KeyCountingBackend(BackendPtr inner_) : inner(std::move(inner_)) {}

    size_t headCountFor(const String & k) const { auto it = head_counts.find(k); return it == head_counts.end() ? 0 : it->second; }
    size_t getCountFor(const String & k) const { auto it = get_counts.find(k); return it == get_counts.end() ? 0 : it->second; }

    DB::Cas::HeadResult head(const String & k) override { ++head_counts[k]; return inner->head(k); }
    std::optional<DB::Cas::GetResult> get(const String & k, DB::Cas::Range r) override { ++get_counts[k]; return inner->get(k, r); }
    std::optional<DB::Cas::GetStreamResult> getStream(const String & k, DB::Cas::Range r) override { return inner->getStream(k, r); }
    DB::Cas::ListPage list(const String & pfx, const String & c, size_t l) override { return inner->list(pfx, c, l); }
    DB::Cas::PutResult putIfAbsent(const String & k, const String & b, const DB::Cas::ObjectMeta & meta) override { return inner->putIfAbsent(k, b, meta); }
    void publishBlob(const DB::Cas::BlobPublishRequest & request) override
    {
        inner->publishBlob(request);
    }
    DB::Cas::PutResult putOverwrite(const String & k, const String & b, const DB::Cas::Token & e, const DB::Cas::ObjectMeta & meta) override { return inner->putOverwrite(k, b, e, meta); }
    DB::Cas::CasResult casPut(const String & k, const String & b, const std::optional<DB::Cas::Token> & e, const DB::Cas::ObjectMeta & meta) override { return inner->casPut(k, b, e, meta); }
    DB::Cas::DeleteOutcome deleteExact(const String & k, const DB::Cas::Token & t) override { return inner->deleteExact(k, t); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }

private:
    BackendPtr inner;
    std::map<String, size_t> head_counts;
    std::map<String, size_t> get_counts;
};

/// Forces two writers to complete their absent `HEAD` observations before either can publish.
class RacingBlobPublicationBackend final : public InMemoryBackend
{
public:
    void watch(String key_)
    {
        std::lock_guard lock(mutex);
        key = std::move(key_);
        head_calls = 0;
        publish_calls = 0;
    }

    HeadResult head(const String & requested_key) override
    {
        if (requested_key != key)
            return InMemoryBackend::head(requested_key);

        const HeadResult observed = InMemoryBackend::head(requested_key);
        std::unique_lock lock(mutex);
        ++head_calls;
        cv.notify_all();
        cv.wait_for(lock, std::chrono::seconds(5), [&] { return head_calls >= 2; });
        return observed;
    }

    void publishBlob(const BlobPublishRequest & request) override
    {
        if (request.destination_key == key)
        {
            std::lock_guard lock(mutex);
            ++publish_calls;
        }
        InMemoryBackend::publishBlob(request);
    }

    String key;
    std::mutex mutex;
    std::condition_variable cv;
    size_t head_calls = 0;
    size_t publish_calls = 0;
};

}

TEST(CASPartWrite, RacingWritersBothHeadMissAndPublishEquivalentBodies)
{
    auto backend = std::make_shared<RacingBlobPublicationBackend>();
    auto store = openPool(backend);
    const String payload = "two-racing-mandatory-head-writers";
    const BlobRef ref = idOf(payload);
    auto first = precommittedBuildForPayload(store, RootNamespace{"srv1/racing-a"}, "part", payload);
    auto second = precommittedBuildForPayload(store, RootNamespace{"srv1/racing-b"}, "part", payload);
    backend->watch(store->layout().blobKey(ref));

    std::exception_ptr first_error;
    std::exception_ptr second_error;
    std::thread first_thread([&]
    {
        try
        {
            first->putBlob(ref, BlobSource::fromString(payload));
        }
        catch (...)
        {
            first_error = std::current_exception();
        }
    });
    std::thread second_thread([&]
    {
        try
        {
            second->putBlob(ref, BlobSource::fromString(payload));
        }
        catch (...)
        {
            second_error = std::current_exception();
        }
    });
    first_thread.join();
    second_thread.join();

    EXPECT_EQ(first_error, nullptr);
    EXPECT_EQ(second_error, nullptr);
    EXPECT_EQ(backend->head_calls, 2u);
    EXPECT_EQ(backend->publish_calls, 2u)
        << "both equivalent writers may publish after racing absent observations";
    EXPECT_EQ(first->dependencyProof(ref), BlobDependencyProof::Materialized);
    EXPECT_EQ(second->dependencyProof(ref), BlobDependencyProof::Materialized);
    const auto stored = backend->get(store->layout().blobKey(ref));
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->bytes.substr(store->poolMeta().blob_header_len), payload);
}

TEST(CASPartWrite, WrongSizeSourcePublishesNothing)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    const String expected_payload = "declared-eleven-bytes";
    const BlobRef ref = idOf(expected_payload);
    auto build = precommittedBuildForPayload(
        store, RootNamespace{"srv1/wrong-size-publication"}, "part", expected_payload);

    BlobSource source;
    source.size = 11;
    source.open = []() -> std::unique_ptr<DB::ReadBuffer>
    {
        return std::make_unique<DB::ReadBufferFromOwnString>(String("short"));
    };

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        build->putBlob(ref, std::move(source));
    });
    EXPECT_FALSE(backend->head(store->layout().blobKey(ref)).exists);
}

TEST(CASPartWriteTxn, PutBlobWritesEnvelopeWithFixedHeader)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/envelope"};
    auto build = precommittedBuildForPayload(s, ns, "part", "hello world");
    auto ref = build->putBlob(idOf("hello world"), BlobSource::fromString("hello world"));
    EXPECT_EQ(ref.size, 11u);

    auto raw = b->get(s->layout().blobKey(ref.ref));
    ASSERT_TRUE(raw.has_value());
    auto h = decodeEnvelopeHeader(raw->bytes, raw->bytes.size(), ObjectKind::Blob);
    EXPECT_EQ(h.header_len, s->poolMeta().blob_header_len);   /// 256
    /// `logical_size`/`logical_hash` were dropped 2026-07-11, and `domain_id` in codecs-v3 phase 7
    /// (the pool id no longer travels in the envelope) — identity is the content key and the payload
    /// starts at the fixed offset `header_len`.
    EXPECT_EQ(h.build_id, build->buildId());
    EXPECT_NE(h.incarnation_tag, UInt128{});
    EXPECT_EQ(raw->bytes.substr(h.header_len), "hello world");
}

TEST(CASPartWriteTxn, StageManifestUsesPerBuildOrdinals)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"test/tbl@cas@"};

    auto build = startBuildFor(s, ns, "all_1_1_0");
    const ManifestId first = build->stageManifest({blobManifestEntry("a.bin", "a")});
    const ManifestId second = build->stageManifest({blobManifestEntry("b.bin", "b")});

    EXPECT_EQ(first.ref.writer_epoch, s->writerEpoch());
    EXPECT_EQ(first.ref.build_sequence, build->buildSeq());
    EXPECT_EQ(first.ref.manifest_ordinal, 1u);
    EXPECT_EQ(second.ref.writer_epoch, first.ref.writer_epoch);
    EXPECT_EQ(second.ref.build_sequence, first.ref.build_sequence);
    EXPECT_EQ(second.ref.manifest_ordinal, 2u);
    /// Canonical hex build directory (spec §Manifest Identifier): `<epoch-hex>-<build-seq-hex>/`.
    const String build_segment = renderRefTxnId(RefTxnId{s->writerEpoch(), build->buildSeq()});
    EXPECT_EQ(s->layout().manifestKey(first), "p/cas/manifests/test/tbl@cas@/" + build_segment + "/000001.zst");
    EXPECT_EQ(s->layout().manifestKey(second), "p/cas/manifests/test/tbl@cas@/" + build_segment + "/000002.zst");

    auto next_build = startBuildFor(s, ns, "all_2_2_0");
    const ManifestId next = next_build->stageManifest({blobManifestEntry("c.bin", "c")});
    EXPECT_EQ(next.ref.writer_epoch, first.ref.writer_epoch);
    EXPECT_NE(next.ref.build_sequence, first.ref.build_sequence);
    EXPECT_EQ(next.ref.manifest_ordinal, 1u);
}

/// B171: the `cas_owner` owner-triple stamping (`PartWriteTxn::ownerMeta`) was DELETED — protection is now
/// the build-root precommit edge (reachability), not revocable object metadata GC reads per-candidate.
/// The old `CASPartWriteTxn.BlobCarriesOwnerTripleInMetadata` asserted that stamping; its coverage is replaced
/// by the build-root precommit/reclaim tests (`CASPartWriteTxnRoot*`, `CASPartWriteTxnRootDangle*`), which prove a
/// written-but-unreferenced object is protected by a live precommit and collectable once it is abandoned.

TEST(CASPartWriteTxn, PutBlobDedupSecondWriterAdopts)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    /// First writer publishes under its durable precommit edge.
    auto build_a = precommittedBuildForPayload(s, RootNamespace{"srv/tbl-a"}, "ref_a", "dup");
    auto ref_a = build_a->putBlob(idOf("dup"), BlobSource::fromString("dup"));
    const Token token_a = b->head(s->layout().blobKey(ref_a.ref)).token;

    /// Second writer ADOPTS — the adopt must happen under a durable precommit edge (EDGE-BEFORE-OBSERVE:
    /// stageManifest -> precommitAdd -> putBlob), so give build_b the wiring order.
    const RootNamespace ns_b{"srv/tbl"};
    auto build_b = startBuildFor(s, ns_b, "ref_b");
    const ManifestId id_b = build_b->stageManifest({blobManifestEntry("data.bin", "dup")});
    build_b->precommitAdd(ns_b, "ref_b", id_b);
    auto ref_b = build_b->putBlob(idOf("dup"), BlobSource::fromString("dup"));

    EXPECT_EQ(ref_b.ref, ref_a.ref);
    /// A's incarnation survives — the second writer adopts, nothing was overwritten.
    EXPECT_EQ(b->head(s->layout().blobKey(ref_a.ref)).token, token_a);
}

/// Task 3 (spec §meta-protocols v3): the writer's dedup gate no longer consults the RetireView for the
/// condemned decision — it point-reads the per-hash freshness meta instead. A fresh (absent -> present)
/// upload must WRITE that meta as Clean so future point-readers (other writers, GC) can see it.
TEST(CASPartWriteTxn, PutBlobFreshUploadWritesCleanMeta)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    const String payload = "fresh-meta-payload";
    auto build = precommittedBuildForPayload(s, RootNamespace{"srv1/fresh-meta"}, "part", payload);
    auto ref = build->putBlob(idOf(payload), BlobSource::fromString(payload));
    EXPECT_EQ(ref.size, payload.size());

    const auto lm = loadMetaForTest(*b, s->layout(), u128Of(payload));
    ASSERT_TRUE(lm.has_value()) << "a fresh upload must write a Clean meta descriptor (writer point-read protocol)";
    EXPECT_EQ(lm->meta.state, MetaState::Clean);
    EXPECT_EQ(lm->meta.size, payload.size());
}

/// §0 introspection: a fresh body upload writes the Clean meta exactly once through the
/// `putMetaIfAbsent` choke point (`CASMetaPut`), tagged with its reason (`CASMetaCreateClean`).
TEST(CASPartWriteTxnMetaCounters, CreateCleanAndChokePointCountOnFreshBody)
{
    /// Fresh body upload writes the Clean meta exactly once: CASMetaPut +1 (choke point)
    /// and CASMetaCreateClean +1 (reason). Reuse the fixture of the nearest putBlob test.
    const auto put_before = ProfileEvents::global_counters[ProfileEvents::CASMetaPut].load();
    const auto reason_before = ProfileEvents::global_counters[ProfileEvents::CASMetaCreateClean].load();

    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    const String payload = "fresh-meta-payload-counters";
    auto build = precommittedBuildForPayload(s, RootNamespace{"srv1/fresh-meta-counters"}, "part", payload);
    auto ref = build->putBlob(idOf(payload), BlobSource::fromString(payload));
    EXPECT_EQ(ref.size, payload.size());

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMetaPut].load() - put_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMetaCreateClean].load() - reason_before, 1);
}

/// §0 introspection: an adopt of a pre-existing body that has NO meta at all (a pre-protocol blob, or a
/// lost race with a concurrent fresh-uploader's own meta write) backfills a Clean meta through the
/// `putMetaIfAbsent` choke point (`CASMetaPut`), tagged with its reason (`CASMetaAdoptBackfill`). No
/// existing test elsewhere in the suite drives this branch: every other pre-seeded raw body in this file
/// pairs `writeRawBlobBody` with `writeMetaClean`, which skips the `!lm` backfill branch entirely.
TEST(CASPartWriteTxnMetaCounters, AdoptBackfillCountsChokePointAndReason)
{
    const auto put_before = ProfileEvents::global_counters[ProfileEvents::CASMetaPut].load();
    const auto reason_before = ProfileEvents::global_counters[ProfileEvents::CASMetaAdoptBackfill].load();

    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    const String payload = "adopt-backfill-payload-counters";
    const UInt128 hash = u128Of(payload);
    const BlobRef id = idOf(payload);

    /// Pre-seed a present body big enough that `ensureBlobPresent`'s logical-size guard does not underflow —
    /// deliberately WITHOUT any meta (unlike PutBlobAdoptsWhenMetaCleanNoRetireView), so the adopt reaches
    /// the `!lm` backfill branch.
    const uint64_t header_len = s->poolMeta().blob_header_len;
    String raw_body(header_len, '\0');
    raw_body += payload;
    writeRawBlobBody(*b, s->layout(), hash, raw_body);

    /// Adopt must happen under a durable precommit edge (EDGE-BEFORE-OBSERVE).
    const RootNamespace ns{"srv/tbl"};
    auto build = startBuildFor(s, ns, "ref_adopt_backfill");
    const ManifestId manifest_id = build->stageManifest({blobManifestEntry("data.bin", payload)});
    build->precommitAdd(ns, "ref_adopt_backfill", manifest_id);
    auto ref = build->putBlob(id, BlobSource::fromString(payload));
    EXPECT_EQ(ref.ref, id);

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMetaPut].load() - put_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMetaAdoptBackfill].load() - reason_before, 1);

    const auto lm = loadMetaForTest(*b, s->layout(), hash);
    ASSERT_TRUE(lm.has_value()) << "the adopt-backfill must leave a Clean meta for future point-readers";
    EXPECT_EQ(lm->meta.state, MetaState::Clean);
}

/// The adopt decision is driven PURELY by the meta point-read — no RetireView is ever seeded in this
/// test. A pre-existing body plus an independent Clean meta must be adopted (no putOverwrite/re-upload:
/// the pre-seeded incarnation's token survives untouched), and the meta stays Clean.
TEST(CASPartWriteTxn, PutBlobAdoptsWhenMetaCleanNoRetireView)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    const String payload = "adopt-meta-payload";
    const UInt128 hash = u128Of(payload);
    const BlobRef id = idOf(payload);
    const String blob_key = s->layout().blobKey(id);

    /// Pre-seed a body big enough that `ensureBlobPresent`'s logical-size guard (hr.size - header_len)
    /// does not underflow, plus an INDEPENDENT Clean meta — deliberately NOT via a real putBlob (so the
    /// adopt decision below cannot be riding on THIS task's own fresh-upload meta write).
    const uint64_t header_len = s->poolMeta().blob_header_len;
    String raw_body(header_len, '\0');
    raw_body += payload;
    writeRawBlobBody(*b, s->layout(), hash, raw_body);
    writeMetaClean(*b, s->layout(), hash, payload.size());
    const Token t0 = b->head(blob_key).token;

    /// Adopt must happen under a durable precommit edge (EDGE-BEFORE-OBSERVE), mirroring
    /// PutBlobDedupSecondWriterAdopts above.
    const RootNamespace ns{"srv/tbl"};
    auto build = startBuildFor(s, ns, "ref_adopt");
    const ManifestId manifest_id = build->stageManifest({blobManifestEntry("data.bin", payload)});
    build->precommitAdd(ns, "ref_adopt", manifest_id);
    auto ref = build->putBlob(id, BlobSource::fromString(payload));

    EXPECT_EQ(ref.ref, id);
    /// Adopted: the pre-seeded incarnation survives untouched — no putOverwrite/re-upload happened.
    EXPECT_EQ(b->head(blob_key).token, t0);

    const auto lm = loadMetaForTest(*b, s->layout(), hash);
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(lm->meta.state, MetaState::Clean) << "an adopt must leave the meta Clean";
}

/// A putBlob call without the mandatory durable precommit must fail closed before either observing or
/// publishing a body. This is the intentional negative fixture for the writer-readiness contract.
TEST(CASPartWriteTxn, AdoptBeforePrecommitFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    const String payload = "adopt-before-precommit-payload";
    const UInt128 hash = u128Of(payload);
    const BlobRef id = idOf(payload);

    /// Pre-seed a present body (padded past the pool header so the logical-size guard does not
    /// underflow) + an independent Clean meta, so putBlob's upload conflicts on the present object and
    /// takes the observation/adoption branch of `ensureBlobPresent` — mirroring PutBlobAdoptsWhenMetaCleanNoRetireView.
    const uint64_t header_len = s->poolMeta().blob_header_len;
    String raw_body(header_len, '\0');
    raw_body += payload;
    writeRawBlobBody(*b, s->layout(), hash, raw_body);
    writeMetaClean(*b, s->layout(), hash, payload.size());

    /// Start a build but DO NOT call precommitAdd.
    const RootNamespace ns{"srv/tbl"};
    auto build = startBuildFor(s, ns, "ref_adopt");

    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->putBlob(id, BlobSource::fromString(payload));
        },
        "durable precommit required");
}

/// The condemned-body replacement decision is likewise driven purely by the metadata point-read
/// — again, no RetireView is seeded. A condemned meta must cause putBlob to displace the body (a fresh
/// token, the old one never returns — INV-NO-RETURN, unchanged body mechanics) AND flip the meta back
/// to Clean.
TEST(CASPartWriteTxn, PutBlobRepublishesWhenMetaCondemned)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    const String payload = "republish-meta-payload";
    const UInt128 hash = u128Of(payload);
    const BlobRef id = idOf(payload);
    const String blob_key = s->layout().blobKey(id);

    const uint64_t header_len = s->poolMeta().blob_header_len;
    String raw_body(header_len, '\0');
    raw_body += payload;
    writeRawBlobBody(*b, s->layout(), hash, raw_body);
    writeMetaClean(*b, s->layout(), hash, payload.size());
    condemnMeta(*b, s->layout(), hash, /*condemn_round*/ 1);
    const Token t0 = b->head(blob_key).token;

    /// No retire-view seeding: the replacement is decided from the metadata point-read.
    auto build = precommittedBuildForPayload(
        s, RootNamespace{"srv1/republish-meta"}, "part", payload);
    auto ref = build->putBlob(id, BlobSource::fromString(payload));
    EXPECT_EQ(ref.ref, id);

    /// Resurrected: the condemned incarnation was displaced by a fresh one.
    const HeadResult hr = b->head(blob_key);
    ASSERT_TRUE(hr.exists);
    EXPECT_NE(hr.token, t0) << "a condemned incarnation must be displaced by a fresh publication";
    EXPECT_EQ(b->deleteExact(blob_key, t0).kind, DeleteOutcome::Kind::TokenMismatch)
        << "the condemned token must never return (INV-NO-RETURN)";

    const auto lm = loadMetaForTest(*b, s->layout(), hash);
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(lm->meta.state, MetaState::Clean) << "republishing must flip the metadata back to Clean";
}

/// §0 introspection: the condemned-displacement metadata flip goes through the `casMeta`
/// choke point (`CASMetaCompareSwap`), tagged with its reason (`CASMetaResurrectClean`).
TEST(CASPartWriteTxnMetaCounters, CondemnedRepublicationCountsCasAndReason)
{
    const auto cas_before = ProfileEvents::global_counters[ProfileEvents::CASMetaCompareSwap].load();
    const auto reason_before = ProfileEvents::global_counters[ProfileEvents::CASMetaResurrectClean].load();

    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    const String payload = "republish-meta-payload-counters";
    const UInt128 hash = u128Of(payload);
    const BlobRef id = idOf(payload);

    const uint64_t header_len = s->poolMeta().blob_header_len;
    String raw_body(header_len, '\0');
    raw_body += payload;
    writeRawBlobBody(*b, s->layout(), hash, raw_body);
    writeMetaClean(*b, s->layout(), hash, payload.size());
    condemnMeta(*b, s->layout(), hash, /*condemn_round*/ 1);

    auto build = precommittedBuildForPayload(
        s, RootNamespace{"srv1/republish-meta-counters"}, "part", payload);
    auto ref = build->putBlob(id, BlobSource::fromString(payload));
    EXPECT_EQ(ref.ref, id);

    EXPECT_GE(ProfileEvents::global_counters[ProfileEvents::CASMetaCompareSwap].load() - cas_before, 1);
    EXPECT_GE(ProfileEvents::global_counters[ProfileEvents::CASMetaResurrectClean].load() - reason_before, 1);
}

TEST(CASPartWriteTxn, PutBlobWrongSizeFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    auto build = precommittedBuildForPayload(
        s, RootNamespace{"srv1/wrong-size"}, "part", "hello world");

    BlobSource lying;
    lying.size = 11;   /// declares 11 but writes 5
    lying.open = []() -> std::unique_ptr<DB::ReadBuffer>
    { return std::make_unique<DB::ReadBufferFromOwnString>(String("short")); };

    const BlobRef id = idOf("hello world");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        build->putBlob(id, std::move(lying));
    });
    /// The cancelled stream created nothing.
    EXPECT_FALSE(b->head(s->layout().blobKey(id)).exists);
}

/// The happy-path upload STREAMS the source directly into the put sink — it does NOT pre-materialize the
/// whole blob into an in-memory String before the I/O. We assert this by counting `open`
/// invocations: a single fresh upload must invoke it EXACTLY ONCE (streamed straight into the sink). The
/// previous implementation buffered the whole blob into a `String source_bytes` first (a full in-memory
/// copy whose peak grew ~linearly with the blob size — the OOM); that pass would invoke `open`
/// before the sink write. One invocation here is the streaming-not-materializing guarantee.
TEST(CASPartWriteTxn, PutBlobStreamsSourceOnceNoFullMaterialization)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    const String payload = "streamed-not-materialized";
    auto build = precommittedBuildForPayload(s, RootNamespace{"srv1/streaming"}, "part", payload);
    int invocations = 0;
    BlobSource source;
    source.size = payload.size();
    source.open = [&invocations, &payload]() -> std::unique_ptr<DB::ReadBuffer>
    {
        ++invocations;
        return std::make_unique<DB::ReadBufferFromOwnString>(payload);
    };

    auto ref = build->putBlob(idOf(payload), std::move(source));
    EXPECT_EQ(ref.size, payload.size());
    EXPECT_EQ(invocations, 1) << "happy-path upload must stream the source exactly once (no pre-materialization pass)";

    /// And the object really landed with the streamed payload (at the fixed header offset).
    auto raw = b->get(s->layout().blobKey(ref.ref));
    ASSERT_TRUE(raw.has_value());
    auto h = decodeEnvelopeHeader(raw->bytes, raw->bytes.size(), ObjectKind::Blob);
    EXPECT_EQ(raw->bytes.substr(h.header_len), payload);
}

/// B190: reuseBlob is removed (it had no production callers post-B188). Its behaviors are now covered by:
///   - trusted adopted leaf at gate: PromoteTrustsAdoptedLeafNoProbeManifestTrust (CasPartWriteTxn) — §4
///     manifest-trust: a committed-source adopted leaf publishes with NO per-file probe; a materialized leaf
///     is edge-protected (Phase A) and never re-observed at the gate.
///   - absent adopted leaf trusted:  PromoteTrustsAdoptedLeafEvenIfBackendRaced (CasPartWriteTxn) — the D4
///     trade-off (a genuinely-absent adopted blob is caught by fsck, not the promote gate).
///   - explicit evidence: DependencyProofDistinguishesMaterializedAndTrustedManifest.
///   - no-dependency staging bug: MissingDependencyProofFailsClosed.

TEST(CASPartWriteTxnReuseBlob, DependencyProofDistinguishesMaterializedAndTrustedManifest)
{
    /// A successful publication records physical evidence without retaining the backend token in
    /// writer readiness state.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);

    auto build = precommittedBuildForPayload(
        s, RootNamespace{"srv1/dependency-proof/materialized"}, "written", "written");

    /// A fresh upload is materialized.
    build->putBlob(idOf("written"), BlobSource::fromString("written"));
    EXPECT_EQ(build->dependencyProof(idOf("written")), BlobDependencyProof::Materialized);

    /// Observing that same live blob under a durable precommit is also materialized.
    const RootNamespace ns{"srv1/dependency-proof"};
    auto observer = startBuildFor(s, ns, "observed");
    const ManifestEntry observed_entry = blobManifestEntry("data.bin", "written");
    const ManifestId observed_manifest = observer->stageManifest({observed_entry});
    observer->precommitAdd(ns, "observed", observed_manifest);
    observer->putBlob(idOf("written"), BlobSource::fromString("written"));
    EXPECT_EQ(observer->dependencyProof(idOf("written")), BlobDependencyProof::Materialized);

    /// A committed-source manifest supplies trusted-manifest evidence without physical I/O.
    build->adoptEvidence(blobManifestEntry("f", "adopted"));
    EXPECT_EQ(build->dependencyProof(idOf("adopted")), BlobDependencyProof::TrustedManifest);

    /// An unknown hash has no proof; absence is distinct from both accepted states.
    EXPECT_EQ(build->dependencyProof(idOf("unknown")), std::nullopt);
}

/// B190: ReuseBlobCondemnedThrowsAbortedRetryable is removed (reuseBlob is gone). §4 manifest-trust: a
/// committed-source adopted leaf is TRUSTED at the promote gate (no HEAD/loadMeta probe), so a condemned
/// pool blob no longer surfaces at promote — covered by PromoteTrustsAdoptedLeafNoProbeManifestTrust.

TEST(CASPartWriteTxn, PutBlobRepublishesVanishedBodyFromHeldSource)
{
    auto b = std::make_shared<InMemoryBackend>();

    /// 1. Write payload-X via a throwaway build to create the blob; capture its token t0.
    BlobRef id;
    Token t0;
    {
        auto s0 = openPool(b);
        auto build0 = precommittedBuildForPayload(
            s0, RootNamespace{"srv1/republish-vanished-seed"}, "part", "payload-X");
        id = build0->putBlob(idOf("payload-X"), BlobSource::fromString("payload-X")).ref;
        t0 = b->head(s0->layout().blobKey(id)).token;
        build0->abandon();
    }

    /// 2. Condemn (Blob, hash(X), t0) in the retire view.
    DB::Cas::Layout layout("p");
    const String blob_key = layout.blobKey(id);
    /// v3: the writer's condemned decision is a per-hash meta point-read (not the retire-view). Condemn the
    /// meta; t0 stays as the body token the delete-hook below fires with.
    condemnMeta(*b, layout, u128Of("payload-X"), /*condemn_round*/ 1);

    /// 3. Wrap the backend so the NEXT head(blob_key) returns the (present) result and THEN fires
    ///    deleteExact(blob_key, t0) exactly once — GC's delete in the HEAD->GET window. Open a FRESH
    ///    Pool over the hook so its retire view (refreshed at open) sees the condemnation.
    auto hook = std::make_shared<HeadThenDeleteOnceBackend>(b, blob_key, t0);
    auto s = Pool::open(hook, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    auto build = precommittedBuildForPayload(
        s, RootNamespace{"srv1/republish-vanished"}, "part", "payload-X");

    /// 4. putBlob with a re-invokable body.
    /// The mandatory `HEAD` observes the condemned body and the hook deletes it before returning.
    /// Publication then recreates the body under a fresh token without reading the condemned object.
    auto ref = build->putBlob(idOf("payload-X"), BlobSource::fromString("payload-X"));
    EXPECT_EQ(ref.ref, id);

    /// 5. The blob is present again under a FRESH token, with the same payload; and the condemned token
    ///    never returns (INV-NO-RETURN).
    const HeadResult hr = b->head(blob_key);
    ASSERT_TRUE(hr.exists);
    EXPECT_NE(hr.token, t0);

    auto raw = b->get(blob_key);
    ASSERT_TRUE(raw.has_value());
    auto h = decodeEnvelopeHeader(raw->bytes, raw->bytes.size(), ObjectKind::Blob);
    EXPECT_EQ(h.header_len, s->poolMeta().blob_header_len);
    EXPECT_EQ(raw->bytes.substr(h.header_len), "payload-X");

    EXPECT_EQ(b->deleteExact(blob_key, t0).kind, DeleteOutcome::Kind::TokenMismatch);

    /// The freshness meta must be reconciled to Clean too, not left stale at Condemned: the fresh
    /// re-upload's meta write (writeFreshMetaClean) must find and fix the pre-existing Condemned
    /// marker via the same reload-and-reconcile path used after condemned replacement, not discard the
    /// conflict (a stale Condemned marker would otherwise mislead every future point-reader).
    const auto lm = loadMetaForTest(*b, s->layout(), u128Of("payload-X"));
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(lm->meta.state, MetaState::Clean)
        << "a fresh re-upload over a stale Condemned marker must reconcile it back to Clean";
}

/// A persistently-failing freshness-meta write (every attempt of every outer reload-retry) must
/// surface as a controlled retry-later signal, not silently succeed with the marker left stale
/// (S22 RCA). The blob body PUT
/// itself is unaffected (MetaWriteFaultBackend only faults `.meta` keys) -- only the meta write
/// exhausts, and that exhaustion must reach putBlob's caller as NETWORK_ERROR.
TEST(CASPartWriteTxn, PutBlobFreshMetaExhaustionThrowsRetryLater)
{
    /// Short budget + zero backoff: keep the test fast. Each of the metadata reconciliation loop's 8 outer
    /// attempts calls putMetaIfAbsent, which itself retries up to max_attempts times internally —
    /// with max_attempts=1 the controller gives up on the first faulted attempt each time.
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.retry_initial_backoff_ms = 0;
    auto b = std::make_shared<DB::Cas::tests::MetaWriteFaultBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .cas_request_budget = budget});

    const String payload = "fresh-meta-exhaustion-payload";
    auto build = precommittedBuildForPayload(
        s, RootNamespace{"srv1/fresh-meta-exhaustion"}, "part", payload);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        build->putBlob(idOf(payload), BlobSource::fromString(payload));
    });

    /// The body itself landed (only .meta writes are faulted) -- confirming the failure is
    /// specifically the freshness marker, not the blob body.
    const HeadResult hr = b->head(s->layout().blobKey(idOf(payload)));
    EXPECT_TRUE(hr.exists) << "the body PUT is unaffected by the meta-only fault";
}

/// INV-1 (revival-from-source): a condemned blob is NEVER read via GET to revive it.
/// putBlob on a condemned-dedup hit must re-upload from its OWN source bytes — never calling
/// backend().get(blob_key). This test counts backend GETs on the blob key and asserts zero.
TEST(CASPartWriteTxn, PutBlobCondemnedDedupNeverGetsTheDyingObject)
{
    /// A delegating backend that counts get() calls on a specific key to assert INV-1.
    struct GetCountingBackend final : public DB::Cas::Backend
    {
        explicit GetCountingBackend(BackendPtr inner_, String watched_key_)
            : inner(std::move(inner_)), watched_key(std::move(watched_key_)) {}
        size_t get_count = 0;

        DB::Cas::HeadResult head(const String & k) override { return inner->head(k); }
        std::optional<DB::Cas::GetResult> get(const String & k, DB::Cas::Range r) override
        {
            if (k == watched_key)
                ++get_count;
            return inner->get(k, r);
        }
        std::optional<DB::Cas::GetStreamResult> getStream(const String & k, DB::Cas::Range r) override { return inner->getStream(k, r); }
        DB::Cas::ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
        DB::Cas::PutResult putIfAbsent(const String & k, const String & bts, const DB::Cas::ObjectMeta & m) override { return inner->putIfAbsent(k, bts, m); }
        void publishBlob(const DB::Cas::BlobPublishRequest & request) override
        {
            inner->publishBlob(request);
        }
        DB::Cas::PutResult putOverwrite(const String & k, const String & bts, const DB::Cas::Token & e, const DB::Cas::ObjectMeta & m) override { return inner->putOverwrite(k, bts, e, m); }
        DB::Cas::CasResult casPut(const String & k, const String & bts, const std::optional<DB::Cas::Token> & e, const DB::Cas::ObjectMeta & m) override { return inner->casPut(k, bts, e, m); }
        DB::Cas::DeleteOutcome deleteExact(const String & k, const DB::Cas::Token & tok) override { return inner->deleteExact(k, tok); }
        bool supportsListTokens() const override { return inner->supportsListTokens(); }
    private:
        BackendPtr inner;
        String watched_key;
    };

    auto b = std::make_shared<InMemoryBackend>();

    /// 1. Upload blob Y via a throwaway build; capture the token t0.
    BlobRef id;
    Token t0;
    {
        auto s0 = openPool(b);
        auto build0 = precommittedBuildForPayload(
            s0, RootNamespace{"srv1/condemned-absent-seed"}, "part", "payload-Y");
        id = build0->putBlob(idOf("payload-Y"), BlobSource::fromString("payload-Y")).ref;
        t0 = b->head(s0->layout().blobKey(id)).token;
        build0->abandon();
    }

    /// 2. Condemn (Blob, hash(Y), t0) in the retire view, then GC-delete the object so it is absent
    ///    (simulates GC completing the delete before the writer's dedup hit).
    DB::Cas::Layout layout("p");
    const String blob_key = layout.blobKey(id);
    injectRetire(*b, layout, /*round*/ 1, /*shard*/ 0,
        {RetiredEntry{.kind = ObjectKind::Blob, .ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of("payload-Y"))}, .token = t0, .size = 9}});
    b->deleteExact(blob_key, t0);
    ASSERT_FALSE(b->head(blob_key).exists);

    /// 3. Open a fresh Pool over a GET-counting wrapper; the retire view sees the condemnation at open.
    auto counting = std::make_shared<GetCountingBackend>(b, blob_key);
    auto s = Pool::open(counting, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    auto build = precommittedBuildForPayload(
        s, RootNamespace{"srv1/condemned-absent"}, "part", "payload-Y");

    /// 4. The object is absent, so `putBlob` observes absence and publishes from the held source.
    /// Even if a racing re-creation happens between observation and publication, the condemned branch
    /// must never call `Backend::get` on the blob key.
    auto ref = build->putBlob(idOf("payload-Y"), BlobSource::fromString("payload-Y"));
    EXPECT_EQ(ref.ref, id);
    EXPECT_EQ(counting->get_count, 0u) << "INV-1: putBlob must not GET the dying object to revive it";

    const HeadResult hr = b->head(blob_key);
    ASSERT_TRUE(hr.exists);
    EXPECT_NE(hr.token, t0) << "a fresh incarnation must have a new token";
    const auto raw = b->get(blob_key);
    ASSERT_TRUE(raw.has_value());
    const auto hdr = decodeEnvelopeHeader(raw->bytes, raw->bytes.size(), ObjectKind::Blob);
    EXPECT_EQ(raw->bytes.substr(hdr.header_len), "payload-Y");
}

/// INV-1 variant: blob is PRESENT and condemned (GC hasn't fired the delete yet). putBlob dedup-hits
/// it via PreconditionFailed, sees condemned token, and must re-upload from source — NEVER GET.
TEST(CASPartWriteTxn, PutBlobCondemnedDedupPresentNeverGetsTheDyingObject)
{
    struct GetCountingBackend final : public DB::Cas::Backend
    {
        explicit GetCountingBackend(BackendPtr inner_, String watched_key_)
            : inner(std::move(inner_)), watched_key(std::move(watched_key_)) {}
        size_t get_count = 0;

        DB::Cas::HeadResult head(const String & k) override { return inner->head(k); }
        std::optional<DB::Cas::GetResult> get(const String & k, DB::Cas::Range r) override
        {
            if (k == watched_key)
                ++get_count;
            return inner->get(k, r);
        }
        std::optional<DB::Cas::GetStreamResult> getStream(const String & k, DB::Cas::Range r) override { return inner->getStream(k, r); }
        DB::Cas::ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
        DB::Cas::PutResult putIfAbsent(const String & k, const String & bts, const DB::Cas::ObjectMeta & m) override { return inner->putIfAbsent(k, bts, m); }
        void publishBlob(const DB::Cas::BlobPublishRequest & request) override
        {
            inner->publishBlob(request);
        }
        DB::Cas::PutResult putOverwrite(const String & k, const String & bts, const DB::Cas::Token & e, const DB::Cas::ObjectMeta & m) override { return inner->putOverwrite(k, bts, e, m); }
        DB::Cas::CasResult casPut(const String & k, const String & bts, const std::optional<DB::Cas::Token> & e, const DB::Cas::ObjectMeta & m) override { return inner->casPut(k, bts, e, m); }
        DB::Cas::DeleteOutcome deleteExact(const String & k, const DB::Cas::Token & tok) override { return inner->deleteExact(k, tok); }
        bool supportsListTokens() const override { return inner->supportsListTokens(); }
    private:
        BackendPtr inner;
        String watched_key;
    };

    auto b = std::make_shared<InMemoryBackend>();

    /// 1. Upload blob Z via a throwaway build; capture the token t0.
    BlobRef id;
    Token t0;
    {
        auto s0 = openPool(b);
        auto build0 = precommittedBuildForPayload(
            s0, RootNamespace{"srv1/condemned-present-seed"}, "part", "payload-Z");
        id = build0->putBlob(idOf("payload-Z"), BlobSource::fromString("payload-Z")).ref;
        t0 = b->head(s0->layout().blobKey(id)).token;
        build0->abandon();
    }

    /// 2. Condemn (Blob, hash(Z), t0) — object still PRESENT (GC condemned but not yet deleted).
    DB::Cas::Layout layout("p");
    const String blob_key = layout.blobKey(id);
    /// v3: condemn via the per-hash meta (the writer's freshness point-read), object still PRESENT.
    condemnMeta(*b, layout, u128Of("payload-Z"), /*condemn_round*/ 1);
    ASSERT_TRUE(b->head(blob_key).exists) << "blob must be PRESENT for the condemned-present path";

    /// 3. Open a fresh Pool over a GET-counting wrapper.
    auto counting = std::make_shared<GetCountingBackend>(b, blob_key);
    auto s = Pool::open(counting, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    auto build = precommittedBuildForPayload(
        s, RootNamespace{"srv1/condemned-present"}, "part", "payload-Z");

    /// 4. `putBlob` observes the condemned metadata and republishes from the held source without a GET.
    auto ref = build->putBlob(idOf("payload-Z"), BlobSource::fromString("payload-Z"));
    EXPECT_EQ(ref.ref, id);
    EXPECT_EQ(counting->get_count, 0u) << "INV-1: putBlob must not GET the condemned object";

    const HeadResult hr = b->head(blob_key);
    ASSERT_TRUE(hr.exists);
    EXPECT_NE(hr.token, t0) << "condemned incarnation must be displaced by a fresh token";
    const auto raw = b->get(blob_key);
    ASSERT_TRUE(raw.has_value());
    const auto hdr = decodeEnvelopeHeader(raw->bytes, raw->bytes.size(), ObjectKind::Blob);
    EXPECT_EQ(raw->bytes.substr(hdr.header_len), "payload-Z");
}

TEST(CASPartWriteTxn, PromoteTrustsAdoptedLeafNoProbeManifestTrust)
{
    /// §4 manifest-trust: a committed-source adoptEvidence leaf is TRUSTED at the promote gate — the live
    /// source pins the blob (in-degree >= 1, not condemnable) and this build's precommit edge is durable,
    /// so promote publishes with NO per-file HEAD (presence) and NO loadMeta GET (the condemned point-read)
    /// and NO copy-forward. The durable manifest edge is the liveness evidence. `CASBlobAdoptTrusted` counts
    /// the trusted leaf. A KeyCountingBackend proves zero probes on the blob key and the blob-meta key.
    auto raw = std::make_shared<InMemoryBackend>();
    auto counting = std::make_shared<KeyCountingBackend>(raw);
    auto s = Pool::open(counting, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"srv1/tbl"};

    /// A committed-source blob lives in the shared pool (seeded via a throwaway build on the same store).
    {
        const RootNamespace seed_ns{"srv1/trusted-seed"};
        auto seed = startBuildFor(s, seed_ns, "part");
        const ManifestId seed_manifest = durablyPrecommit(
            seed,
            seed_ns,
            "part",
            {blobManifestEntryStreaming("data.bin", "payload-TR")});
        seed->putBlob(streamRefOf("payload-TR"), BlobSource::fromString("payload-TR"));
        seed->promote(seed_ns, "part", seed->buildId(), seed_manifest);
    }
    const String blob_key = s->layout().blobKey(streamRefOf("payload-TR"));
    const String meta_key = s->layout().blobMetaKey(streamRefOf("payload-TR"));

    auto build = startBuildFor(s, ns, "part_1");
    const ManifestEntry entry = blobManifestEntryStreaming("data.bin", "payload-TR");
    build->adoptEvidence(entry);
    EXPECT_EQ(build->dependencyProof(entry.ref), BlobDependencyProof::TrustedManifest);
    const ManifestId id = build->stageManifest({entry});
    build->precommitAdd(ns, "part_1", id);

    const auto trusted_before = ProfileEvents::global_counters[ProfileEvents::CASBlobAdoptTrusted].load();
    const size_t head_before = counting->headCountFor(blob_key);
    const size_t meta_get_before = counting->getCountFor(meta_key);

    build->promote(ns, "part_1", build->buildId(), id);

    EXPECT_TRUE(s->resolveRef(ns, "part_1").has_value());
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASBlobAdoptTrusted].load() - trusted_before, 1);
    EXPECT_EQ(counting->headCountFor(blob_key) - head_before, 0u) << "trust must not HEAD the adopted blob";
    EXPECT_EQ(counting->getCountFor(meta_key) - meta_get_before, 0u) << "trust must not loadMeta the adopted blob";
}

TEST(CASPartWriteTxn, PromotionAcceptsBothDependencyProofs)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/proof-promotion"};

    /// Seed the committed-source body used by the trusted-manifest branch.
    {
        publishOneBlobPart(
            s, RootNamespace{"srv1/proof-promotion-seed"}, "part", "data.bin", "trusted-body");
    }

    auto build = startBuildFor(s, ns, "part_1");
    const ManifestEntry materialized = blobManifestEntry("data.bin", "materialized-body");
    const ManifestEntry trusted = blobManifestEntry("data.cmrk3", "trusted-body");
    const ManifestId id = build->stageManifest({materialized, trusted});
    build->precommitAdd(ns, "part_1", id);

    build->putBlob(materialized.ref, BlobSource::fromString("materialized-body"));
    build->adoptEvidence(trusted);
    ASSERT_EQ(build->dependencyProof(materialized.ref), BlobDependencyProof::Materialized);
    ASSERT_EQ(build->dependencyProof(trusted.ref), BlobDependencyProof::TrustedManifest);

    EXPECT_NO_THROW(build->promote(ns, "part_1", build->buildId(), id));
    EXPECT_TRUE(s->resolveRef(ns, "part_1").has_value());
}

TEST(CASPartWriteTxn, InvalidDependencyProofFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/invalid-proof"};
    auto build = startBuildFor(s, ns, "part_1");

    const ManifestEntry entry = blobManifestEntry("data.bin", "invalid-proof-body");
    const ManifestId id = build->stageManifest({entry});
    build->precommitAdd(ns, "part_1", id);

    BlobUploadResult invalid{
        entry.ref,
        BlobDepRecord{ObjectKind::Blob, static_cast<BlobDependencyProof>(2), entry.blob_size},
        BlobUploadDiagnostics{
            BlobMaterializationAction::Published,
            BlobPublicationReason::Absent,
            BlobPublicationTransport::Streaming}};
    build->mergeBlobUploadResults(std::span<const BlobUploadResult>(&invalid, 1));

    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->promote(ns, "part_1", build->buildId(), id);
        },
        "unnamed dependency proof");
}

TEST(CASPartWriteTxn, PromoteTrustsAdoptedLeafEvenIfBackendRaced)
{
    /// §4 manifest-trust trade-off (D4 relink interserver-trust model): a committed-source adopted leaf is
    /// published WITHOUT a presence probe. Even if the pool object raced to absent between adopt and
    /// promote, promote does NOT re-observe it — the ref publishes. A genuinely-absent adopted blob is an
    /// invariant violation detected by fsck (or an actual body GET on read), not caught at the promote gate.
    /// This is the deliberate reduction from the pre-§4 "absent adopted leaf => ABORTED at gate".
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    /// Seed X, adopt it, then delete it out from under the build (a landed GC delete in the adopt->promote
    /// window). The pre-§4 gate HEADed X, found it absent, and threw ABORTED; §4 trusts the durable edge.
    {
        const RootNamespace seed_ns{"srv1/race-seed"};
        auto seed = startBuildFor(s, seed_ns, "part");
        const ManifestId seed_manifest = durablyPrecommit(
            seed,
            seed_ns,
            "part",
            {blobManifestEntryStreaming("data.bin", "payload-RACE")});
        seed->putBlob(streamRefOf("payload-RACE"), BlobSource::fromString("payload-RACE"));
        seed->promote(seed_ns, "part", seed->buildId(), seed_manifest);
    }
    const String blob_key = s->layout().blobKey(streamRefOf("payload-RACE"));
    const Token t0 = b->head(blob_key).token;

    auto build = startBuildFor(s, ns, "part_1");
    const ManifestEntry entry = blobManifestEntryStreaming("data.bin", "payload-RACE");
    build->adoptEvidence(entry);
    const ManifestId id = build->stageManifest({entry});
    build->precommitAdd(ns, "part_1", id);

    ASSERT_EQ(b->deleteExact(blob_key, t0).kind, DeleteOutcome::Kind::Deleted);
    ASSERT_FALSE(b->head(blob_key).exists);

    EXPECT_NO_THROW(build->promote(ns, "part_1", build->buildId(), id));
    EXPECT_TRUE(s->resolveRef(ns, "part_1").has_value());
}

TEST(CASPartWriteTxn, PromoteSwallowsPostDurableEventSinkFailure)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    auto build = startBuildFor(s, ns, "part_1");

    ManifestEntry entry;
    entry.path = "data.bin";
    entry.placement = EntryPlacement::Inline;
    entry.ref = idOf("payload");
    entry.blob_size = 7;
    entry.inline_bytes = "payload";
    const ManifestId id = build->stageManifest({entry});
    build->precommitAdd(ns, "part_1", id);

    /// UNKNOWN_EXCEPTION (not LOGICAL_ERROR): this simulates an arbitrary observer/sink callback
    /// failing, not a CAS invariant violation -- LOGICAL_ERROR would abort the whole process under
    /// debug/sanitizer builds instead of behaving like a catchable exception.
    s->setEventSink([](const CasEvent & e)
    {
        if (e.type == CasEventType::BuildPublish)
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_EXCEPTION, "injected post-durable event sink failure");
    });

    EXPECT_NO_THROW(build->promote(ns, "part_1", build->buildId(), id));
    const auto resolved = s->resolveRef(ns, "part_1");
    ASSERT_TRUE(resolved);
    EXPECT_EQ(resolved->manifest_id, id);
    s->setEventSink(nullptr);
}

TEST(CASPartWriteTxn, MissingDependencyProofFailsClosed)
{
    /// A manifest blob leaf with NO recorded dep (a staging-bug shape: neither putBlob nor adoptEvidence
    /// recorded it) must fail closed at the promote gate because neither accepted proof exists, so §4
    /// never silently publishes it. Under manifest-trust there is NO per-file
    /// probe, so the fail-closed is a LOGICAL_ERROR decided from the dep set alone — it fires regardless of
    /// the pool blob's presence/condemnation (here the blob is even condemned, but that is never observed).
    /// The no-dep shape is reachable through the public build API (stageManifest names the leaf without any
    /// dep having been recorded), so no test accessor for the private predicate is needed.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    /// X exists (streaming-keyed) but THIS build records NO dep for it (no putBlob, no adoptEvidence).
    {
        const RootNamespace seed_ns{"srv1/missing-proof-seed"};
        auto seed = startBuildFor(s, seed_ns, "part");
        const ManifestId seed_manifest = durablyPrecommit(
            seed,
            seed_ns,
            "part",
            {blobManifestEntryStreaming("data.bin", "payload-NODEP")});
        seed->putBlob(streamRefOf("payload-NODEP"), BlobSource::fromString("payload-NODEP"));
        seed->promote(seed_ns, "part", seed->buildId(), seed_manifest);
    }
    const String blob_key = s->layout().blobKey(streamRefOf("payload-NODEP"));
    const Token t0 = b->head(blob_key).token;

    auto build = startBuildFor(s, ns, "part_1");
    const ManifestEntry entry = blobManifestEntryStreaming("data.bin", "payload-NODEP");
    /// NB: no `adoptEvidence` call — dependencies stay empty for this hash.
    const ManifestId id = build->stageManifest({entry});
    build->precommitAdd(ns, "part_1", id);

    /// Condemn X (present) via the meta — under §4 the gate never point-reads it (no probe on a non-trusted
    /// leaf), so this only confirms the fail-closed does not depend on the leaf being clean.
    condemnMeta(*b, s->layout(), hexToU128(streamingHexOf("payload-NODEP")), /*condemn_round*/ 1);

    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->promote(ns, "part_1", build->buildId(), id);
        },
        "no dependency proof");
    EXPECT_FALSE(s->resolveRef(ns, "part_1").has_value());
    /// The pool blob was never touched (no probe, no displacement).
    EXPECT_EQ(b->head(blob_key).token, t0);
}

TEST(CASPartWriteTxn, PromoteRevalidatesBlobPresenceFailClosed)
{
    /// Port of the old W-TREE-BUILD bottom-up enforcement (PutTreeEnforcesBottomUp): the surviving
    /// "a committed ref never names a missing dependency" invariant. In the part-manifest model
    /// stageManifest does not validate its entries' bodies. §4 manifest-trust: the fail-closed authority at
    /// the promote gate is now the DEP SET, not a backend HEAD — a leaf named by the manifest with NO
    /// dependency proof (neither `putBlob` nor `adoptEvidence` recorded one) is a staging bug and fails
    /// closed with LOGICAL_ERROR (a real write always records a dep for every leaf). No per-file probe.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    /// Stage + precommit a manifest naming a blob hash that was NEVER uploaded (no dep recorded).
    auto build = startBuildFor(s, ns, "part_1");
    const ManifestId mid = build->stageManifest({blobManifestEntry("data.bin", "never-uploaded")});
    build->precommitAdd(ns, "part_1", mid);

    /// Promotion must fail closed because the leaf has no dependency proof. No ref is committed.
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->promote(ns, "part_1", build->buildId(), mid);
        },
        "no dependency proof");
    EXPECT_FALSE(s->resolveRef(ns, "part_1").has_value());

    /// After uploading the blob, a fresh build's promote succeeds — the same manifest content is now
    /// fully present.
    auto build2 = startBuildFor(s, ns, "part_1");
    const ManifestId mid2 = build2->stageManifest({blobManifestEntry("data.bin", "never-uploaded")});
    build2->precommitAdd(ns, "part_1", mid2);
    build2->putBlob(idOf("never-uploaded"), BlobSource::fromString("never-uploaded"));
    EXPECT_NO_THROW(build2->promote(ns, "part_1", build2->buildId(), mid2));
    EXPECT_TRUE(s->resolveRef(ns, "part_1").has_value());
}

TEST(CASPartWriteTxn, AdoptEvidenceRecordsTrustedManifestProof)
{
    /// Port of AdoptFromTreeRecordsEvidence. `adoptEvidence` records `TrustedManifest` directly
    /// from a resolved `ManifestEntry`; a Blob entry has a proof, while an Inline entry
    /// records nothing. §4: whether the dep is a committed-source adopt vs absent (adopted vs no-dep) is
    /// asserted end-to-end at the promote gate by PromoteTrustsAdoptedLeafNoProbeManifestTrust (positive:
    /// adopted leaf ⇒ trusted, no probe) and PromoteCondemnedLeafWithoutDepAbortsFailClosed (negative
    /// control: no dep ⇒ fail closed, LOGICAL_ERROR).
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    auto build = s->beginPartWrite({});

    const ManifestEntry adopted = blobManifestEntry("data.bin", "source-blob");
    build->adoptEvidence(adopted);
    EXPECT_EQ(build->dependencyProof(adopted.ref), BlobDependencyProof::TrustedManifest);

    /// An Inline entry references no standalone object and records no proof.
    ManifestEntry inline_entry;
    inline_entry.path = "small";
    inline_entry.placement = EntryPlacement::Inline;
    inline_entry.inline_bytes = "abc";
    build->adoptEvidence(inline_entry);
    EXPECT_EQ(build->dependencyProof(idOf("abc")), std::nullopt);
}

TEST(CASPartWriteTxn, AbandonRemovesStagedDebrisAndDisables)
{
    /// Port of AbandonLeavesDebrisAndDisables to the new abandon semantics (CasPartWriteTxn.cpp abandon):
    /// abandon best-effort exact-token-DELETEs this build's STAGED manifest debris, leaves blob bodies
    /// (full GC's job via min_active), and disables the build (further ops throw via requireAlive).
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    auto build = startBuildFor(s, ns, "ref");

    const ManifestId owner = build->stageManifest({blobManifestEntry("owner", "kept")});
    build->precommitAdd(ns, "ref", owner);
    auto blob_ref = build->putBlob(idOf("kept"), BlobSource::fromString("kept"));
    const ManifestId mid = build->stageManifest({blobManifestEntry("f", "kept")});

    /// The staged manifest body and the blob are present before abandon.
    EXPECT_TRUE(b->head(s->layout().blobKey(blob_ref.ref)).exists);
    EXPECT_TRUE(b->head(s->layout().manifestKey(mid)).exists);

    build->abandon();

    /// Blob stays (debris — full GC reclaims it). The staged manifest debris is best-effort cleaned now.
    EXPECT_TRUE(b->head(s->layout().blobKey(blob_ref.ref)).exists);
    EXPECT_FALSE(b->head(s->layout().manifestKey(mid)).exists)
        << "abandon must best-effort delete this build's staged manifest debris";

    /// Further operations throw via requireAlive.
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->putBlob(idOf("after"), BlobSource::fromString("after"));
        },
        "has been abandoned");
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->stageManifest({blobManifestEntry("g", "kept")});
        },
        "has been abandoned");
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->precommitAdd(ns, "ref", mid);
        },
        "has been abandoned");
}

TEST(CASPartWriteTxn, PublishHappyPathRoundTrip)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    auto build = startBuildFor(s, ns, "part_1");

    const ManifestId id = build->stageManifest({blobManifestEntry("data.bin", "hello world")});
    build->precommitAdd(ns, "part_1", id);
    auto blob = build->putBlob(idOf("hello world"), BlobSource::fromString("hello world"));
    EXPECT_EQ(blob.size, 11u);

    build->promote(ns, "part_1", build->buildId(), id);

    auto r = s->resolveRef(ns, "part_1");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->manifest_id, id);

    /// Read the manifest back and locate its single blob leaf.
    const PartManifest manifest = s->readManifest(id);
    ASSERT_EQ(manifest.entries.size(), 1u);
    const auto * entry = findEntry(manifest.entries, "data.bin");
    ASSERT_TRUE(entry != nullptr);
    const auto loc = s->locate(*entry);
    auto got = b->get(loc.key, Range{loc.offset, loc.length});
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "hello world");
}

TEST(CASPartWriteTxn, PromoteCrossNamespaceManifestFailsClosed)
{
    /// Port of PublishRequiresTreeInDepSet. The W-DEP-SET "root must be a built/adopted dep" authority
    /// is gone (the tree object model it guarded is gone); the surviving fail-closed authority that
    /// refuses an inconsistent commit target is the namespace consistency check in precommitAdd/promote
    /// (CasPartWriteTxn.cpp): a manifest whose root_namespace != the target namespace is a bug ⇒ LOGICAL_ERROR.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    const RootNamespace other_ns{"srv1/other"};

    auto build = startBuildFor(s, ns, "part_1");
    /// The manifest is minted in `ns` (derived from intended_ref). Promoting/precommitting it into a
    /// DIFFERENT namespace must fail closed.
    const ManifestId id = build->stageManifest({blobManifestEntry("data.bin", "hello world")});

    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->precommitAdd(other_ns, "part_1", id);
        },
        "precommitAdd: manifest namespace");
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->promote(other_ns, "part_1", build->buildId(), id);
        },
        "promote: manifest namespace");
}

/// (CASPartWriteTxn.PublishOwnThreadConflictRetries was removed with the legacy mutable ref-shard lane: it
/// injected a Conflict on the promote's shard `casPut` and asserted the shard re-read/retry. The ref model
/// has no shard CAS -- promote appends a write-once ref-log object via `putIfAbsentControlled`, and an
/// uncertain create is resolved by exact-key observation, covered by the ref-writer uncertain-result tests
/// (`gtest_cas_ref_writer.cpp`), not a CAS retry.)

TEST(CASPartWriteTxn, PublishIntoSecondNamespaceSameBlob)
{
    /// Port of PublishIntoSecondNamespaceSameTree. A part manifest is single-owner and namespace-qualified
    /// (precommitAdd/promote enforce id.root_namespace == target_ns), so the SAME ManifestId cannot be
    /// published into two namespaces — each namespace gets its OWN manifest. The invariant the original
    /// test protected is preserved at the BLOB plane: the shared blob is uploaded ONCE and adopted by the
    /// second build (its token is unchanged after the second publish).
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns1{"srv1/tbl"};
    const RootNamespace ns2{"srv1/tbl/detached"};

    /// First build publishes part_1 in ns1, uploading the blob.
    auto build1 = startBuildFor(s, ns1, "part_1");
    const ManifestId id1 = build1->stageManifest({blobManifestEntry("data.bin", "hello world")});
    build1->precommitAdd(ns1, "part_1", id1);
    auto blob = build1->putBlob(idOf("hello world"), BlobSource::fromString("hello world"));
    const String blob_key = s->layout().blobKey(blob.ref);
    const Token blob_token = b->head(blob_key).token;
    build1->promote(ns1, "part_1", build1->buildId(), id1);

    /// Second build publishes part_1 in ns2 referencing the SAME blob: putBlob dedup-hits and ADOPTS the
    /// present incarnation (no re-upload), so the blob token is unchanged. Wiring order
    /// (EDGE-BEFORE-OBSERVE): stageManifest -> precommitAdd -> putBlob -> promote.
    auto build2 = startBuildFor(s, ns2, "part_1");
    const ManifestId id2 = build2->stageManifest({blobManifestEntry("data.bin", "hello world")});
    build2->precommitAdd(ns2, "part_1", id2);
    build2->putBlob(idOf("hello world"), BlobSource::fromString("hello world"));
    build2->promote(ns2, "part_1", build2->buildId(), id2);

    auto r1 = s->resolveRef(ns1, "part_1");
    auto r2 = s->resolveRef(ns2, "part_1");
    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r1->manifest_id, id1);
    EXPECT_EQ(r2->manifest_id, id2);

    /// The blob object was uploaded once: its token is unchanged after both publishes.
    EXPECT_EQ(b->head(blob_key).token, blob_token);
}

/// Task 10: refs are no longer sharded (one whole-table cache per namespace, spec §Table State), so
/// there is no more "same shard" CAS-conflict-retry to force — two builds publishing into the SAME
/// TABLE now serialize through the append lane's per-namespace batching queue instead (exercised by
/// gtest_cas_ref_writer.cpp's co-batch/queue tests). What remains a real regression to guard is the
/// end-to-end outcome: two builds publishing distinct refs into one namespace both land correctly.
TEST(CASPartWriteTxn, TwoBuildsPublishToSameNamespaceBothLand)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    const String ref1 = "a";
    const String ref2 = "b";

    auto build_a = startBuildFor(s, ns, ref1);
    const ManifestId id_a = build_a->stageManifest({blobManifestEntry("data.bin", "content-a")});
    build_a->precommitAdd(ns, ref1, id_a);
    build_a->putBlob(idOf("content-a"), BlobSource::fromString("content-a"));
    build_a->promote(ns, ref1, build_a->buildId(), id_a);

    auto build_b = startBuildFor(s, ns, ref2);
    const ManifestId id_b = build_b->stageManifest({blobManifestEntry("data.bin", "content-b")});
    build_b->precommitAdd(ns, ref2, id_b);
    build_b->putBlob(idOf("content-b"), BlobSource::fromString("content-b"));
    build_b->promote(ns, ref2, build_b->buildId(), id_b);

    auto r1 = s->resolveRef(ns, ref1);
    auto r2 = s->resolveRef(ns, ref2);
    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r1->manifest_id, id_a);
    EXPECT_EQ(r2->manifest_id, id_b);
    EXPECT_EQ(s->listRefs(ns).size(), 2u);
}

TEST(CASPartWriteTxn, FirstPublishMakesNamespaceDiscoverable)
{
    /// After Task 4 the registry is deleted; the first publication admits the namespace to the
    /// authoritative catalog before appending its stream record.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"srv9/fresh"};

    EXPECT_TRUE(s->listNamespaces("").namespaces.empty());
    publishOneBlobPart(s, ns, "part_1", "f", "reg-payload");

    /// The namespace is now discoverable from the catalog -- no registry write needed.
    const auto all = s->listNamespaces("").namespaces;
    ASSERT_EQ(all.size(), 1u);
    EXPECT_EQ(all[0], "srv9/fresh");
}

TEST(CASPartWriteTxn, AdoptEvidenceRecordsTrustedManifestDependencyProofWithoutIO)
{
    /// B188: `adoptEvidence` records `TrustedManifest` from an already resolved `ManifestEntry`
    /// WITHOUT any backend call (no HEAD, no GET, no PUT).
    ///
    /// Two behavioural assertions:
    ///   1. No backend op fires during adoptEvidence (counted via a delegating wrapper).
    ///   2. The recorded dependency proof is `TrustedManifest`.

    /// A delegating wrapper that counts every backend access path.
    struct LocalCountingBackend final : public Backend
    {
        explicit LocalCountingBackend(BackendPtr inner_) : inner(std::move(inner_)) {}
        size_t heads = 0;
        size_t puts = 0;
        size_t gets = 0;

        HeadResult head(const String & k) override { ++heads; return inner->head(k); }
        void publishBlob(const BlobPublishRequest & request) override
        {
            ++puts;
            inner->publishBlob(request);
        }
        std::optional<GetResult> get(const String & k, Range r) override { ++gets; return inner->get(k, r); }
        std::optional<GetStreamResult> getStream(const String & k, Range r) override { return inner->getStream(k, r); }
        ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
        PutResult putIfAbsent(const String & k, const String & bts, const ObjectMeta & m) override
        {
            ++puts;
            return inner->putIfAbsent(k, bts, m);
        }
        PutResult putOverwrite(const String & k, const String & bts, const Token & e, const ObjectMeta & m) override { return inner->putOverwrite(k, bts, e, m); }
        CasResult casPut(const String & k, const String & bts, const std::optional<Token> & e, const ObjectMeta & m) override { return inner->casPut(k, bts, e, m); }
        DeleteOutcome deleteExact(const String & k, const Token & t) override { return inner->deleteExact(k, t); }
        bool supportsListTokens() const override { return inner->supportsListTokens(); }
    private:
        BackendPtr inner;
    };

    auto raw = std::make_shared<InMemoryBackend>();
    auto counting = std::make_shared<LocalCountingBackend>(raw);
    auto s = Pool::open(counting, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    auto build = s->beginPartWrite({});

    /// A Blob ManifestEntry. adoptEvidence is called on a hand-crafted entry — that IS the B188 interface.
    const ManifestEntry entry = blobManifestEntry("b188.bin", "b188-content");

    /// Reset the counters after Pool::open (which may HEAD/GET gc/server-roots etc. during startup).
    counting->heads = 0;
    counting->puts = 0;
    counting->gets = 0;

    /// adoptEvidence — must record the dep WITHOUT touching the backend.
    EXPECT_NO_THROW(build->adoptEvidence(entry));
    EXPECT_EQ(counting->heads, 0u) << "adoptEvidence must not HEAD the backend";
    EXPECT_EQ(counting->puts, 0u) << "adoptEvidence must not PUT to the backend";
    EXPECT_EQ(counting->gets, 0u) << "adoptEvidence must not GET from the backend";

    /// The dep is recorded as trusted manifest evidence; the no-backend-op counts above are the B188
    /// contract's primary guard.
    EXPECT_EQ(build->dependencyProof(entry.ref), BlobDependencyProof::TrustedManifest);

    /// Inline entry: adoptEvidence records nothing (Inline has no standalone object) and no backend op.
    ManifestEntry inline_entry;
    inline_entry.path = "small";
    inline_entry.placement = EntryPlacement::Inline;
    inline_entry.inline_bytes = "xy";
    EXPECT_NO_THROW(build->adoptEvidence(inline_entry));
    EXPECT_EQ(counting->heads, 0u);
    EXPECT_EQ(counting->puts, 0u);
    EXPECT_EQ(counting->gets, 0u);
    EXPECT_EQ(build->dependencyProof(idOf("xy")), std::nullopt);
}

TEST(CASPartWriteTxn, ConvergesUnderProductiveGc)
{
    /// B167/B171 LIVENESS — the re-upload/condemn livelock, now closed by the build-root precommit edge.
    ///
    /// THE BUG (before the fix): a blob H was referenced, dropped, and GC-condemned (everEdged ∧ InDeg=0,
    /// condemned in the retire view). A NEW build dedup-HITS H by content and must re-upload it from
    /// source — it re-streams a FRESH incarnation of H. But the productive GC, re-deriving H as a
    /// zero-in-degree candidate every round, kept RE-CONDEMNING and exact-token-DELETING that fresh
    /// incarnation in the build's upload→commit window. The build never converged → livelock.
    ///
    /// THE FIX (B171): protection is the build-root PRECOMMIT EDGE. PartWriteTxn B precommits its manifest (naming
    /// H) BEFORE the adversarial loop, so the GC fold lifts H to in-degree ≥ 1 — H is never even a
    /// zero-in-degree candidate and is SPARED every round until B promotes (the committed ref then pins H).
    ///
    /// FORM: full adversarial loop. A real Gc drives complete runRegularRound rounds against the same
    /// pool while build B holds an active watermark covering H's incarnation. We assert H is SPARED
    /// every round and that B promotes within a BOUNDED number of GC rounds, after which H reads back.
    auto b = std::make_shared<InMemoryBackend>();
    const RootNamespace ns{"srv1/tbl"};

    PoolConfig cfg;
    cfg.pool_prefix = "p";
    cfg.server_root_id = "test";
    cfg.server_id = UInt128(0xAB);
    cfg.background_watermark = false;
    const String content = "shared-content";

    /// 1. PartWriteTxn A creates H ("shared-content"), publishes a part referencing it, then drops the ref.
    ///    Capture H's first incarnation token so we can condemn exactly it.
    BlobRef h;
    Token h_token0;
    {
        auto s0 = Pool::open(b, cfg);
        publishOneBlobPart(s0, ns, "part_1", "f", content);
        h = idOf(content);
        h_token0 = b->head(s0->layout().blobKey(h)).token;
        s0->dropRef(ns, "part_1");
    }

    /// 2. Condemn (Blob, H, h_token0): `injectRetire` seeds the LEDGER (a real round's later settle/spare
    ///    of h_token0 rides this entry — the adversarial loop below still exercises that), and v3's
    ///    `condemnMeta` seeds the per-hash META (the writer's condemned decision is now a point-read of
    ///    it, not the retire-view — Task 3). `publishOneBlobPart` already created H's meta as Clean, so
    ///    condemnMeta's read-modify-CAS finds it. Together these reproduce exactly what a real GC condemn
    ///    now writes (Task 5), without driving a full round just to observe H at in-degree 0.
    DB::Cas::Layout layout("p");
    injectRetire(*b, layout, /*round*/ 1, /*shard*/ 0,
        {RetiredEntry{.kind = ObjectKind::Blob, .ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(content))}, .token = h_token0,
                      .size = content.size()}});
    condemnMeta(*b, layout, u128Of(content), /*condemn_round*/ 1);

    /// 3. Open the live Pool and start build B. B observes condemned H and republishes from
    ///    its own source through `putBlob`: a fresh incarnation, a NEW token. B stays ACTIVE for
    ///    the whole adversarial loop — its build_seq is never retired below.
    auto s = Pool::open(b, cfg);
    const String blob_key = s->layout().blobKey(h);
    auto build_b = startBuildFor(s, ns, "part_2");

    /// Establish the reachability edge before observing/replacing H.
    const ManifestId mid_b = build_b->stageManifest({blobManifestEntry("f", content)});
    build_b->precommitAdd(ns, "part_2", mid_b);

    /// B190: use `putBlob` (holds source bytes). It detects the condemned observation and publishes
    /// unconditionally — no GET of the dying object.
    const auto ref_b = build_b->putBlob(h, BlobSource::fromString(content));
    ASSERT_EQ(ref_b.ref, h);

    const HeadResult after_reupload = b->head(blob_key);
    ASSERT_TRUE(after_reupload.exists);
    EXPECT_NE(after_reupload.token, h_token0);   /// a genuinely fresh incarnation

    /// 4. THE ADVERSARIAL LOOP. A real, productive GC keeps trying to reclaim. It reclaims the now-
    ///    unreferenced part_1 manifest (build A's, UNprotected) but H stays pinned by B's PRECOMMIT edge
    ///    (in-degree ≥ 1), so H is never even a zero-in-degree candidate. We drive far more rounds than B
    ///    needs to promote; H must survive ALL of them. Each round renews B's watermark so the crash
    ///    detector keeps judging B live.
    Gc gc(s, hexToU128("00000000000000000000000000000001"));
    constexpr int MAX_GC_ROUNDS = 8;

    const auto driveRoundAndAssertHSpared = [&](int round_no)
    {
        /// A LIVE server renews its watermark continuously. Renew once per GC round so B's watermark seq
        /// ADVANCES between rounds — that is precisely what distinguishes a live server from a crashed one
        /// (a frozen B would have its precommit reclaimed; an advancing seq keeps it).
        s->renewWatermarkOnce();
        gc.runRegularRound();
        const HeadResult hr = b->head(blob_key);
        ASSERT_TRUE(hr.exists) << "H was deleted by GC at round " << round_no
                               << " despite being pinned by the live build B's precommit (B167 livelock would do this)";
        const auto raw = b->get(blob_key);
        ASSERT_TRUE(raw.has_value());
        const auto hdr = decodeEnvelopeHeader(raw->bytes, raw->bytes.size(), ObjectKind::Blob);
        EXPECT_EQ(raw->bytes.substr(hdr.header_len), content)
            << "H's content was lost/corrupted at round " << round_no;
    };

    /// Phase 1 — the livelock window. H is referenced by NO committed TABLE ref (B has not promoted yet)
    /// but IS named by B's precommit, so the build-root fold lifts it to in-degree ≥ 1. Drive several
    /// full rounds; the precommit edge must SPARE H's fresh incarnation every round.
    int rounds_run = 0;
    constexpr int PRE_PUBLISH_ROUNDS = 4;
    for (int i = 0; i < PRE_PUBLISH_ROUNDS; ++i)
    {
        driveRoundAndAssertHSpared(++rounds_run);
        if (::testing::Test::HasFatalFailure())
            return;
    }

    /// Phase 2 — converge. With H still alive (spared through the whole window), build B promotes a part
    /// referencing it. The promote gate sees H present + live (fresh incarnation uploaded above), so it
    /// commits. This MUST succeed — the build converges in bounded steps.
    build_b->promote(ns, "part_2", build_b->buildId(), mid_b);
    const bool published = true;

    /// Phase 3 — keep the GC hammering after promote. H is now pinned by the committed ref's manifest
    /// edge; the GC must keep sparing it as a genuinely-reachable node.
    while (rounds_run < MAX_GC_ROUNDS)
    {
        driveRoundAndAssertHSpared(++rounds_run);
        if (::testing::Test::HasFatalFailure())
            return;
    }

    /// 6. ASSERT convergence: promote SUCCEEDED within the bounded budget, and H reads back intact.
    ASSERT_TRUE(published) << "build B never published — the B167 livelock is back";
    EXPECT_LE(rounds_run, MAX_GC_ROUNDS);

    const auto resolved = s->resolveRef(ns, "part_2");
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(resolved->manifest_id, mid_b);

    const PartManifest manifest = s->readManifest(mid_b);
    ASSERT_EQ(manifest.entries.size(), 1u);
    const auto * entry = findEntry(manifest.entries, "f");
    ASSERT_TRUE(entry != nullptr);
    const auto loc = s->locate(*entry);
    const auto got = b->get(loc.key, Range{loc.offset, loc.length});
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, content);
}

/// BUG 1 (WPromote owner==bld): promote is a PURE owner MOVE (Δ=0 — it restores no blob in-degree). The
/// TLA+ `WPromote` requires the precommit to STILL be the live owner of the ref before the move (`owner[m]
/// = bld`). If the precommit was removed/reclaimed (an abandon or GC reclaim appended a removal event), a
/// Δ=0 move would re-publish a committed ref over blobs whose in-degree was already decremented to 0 — GC
/// then deletes them ⇒ a reachable committed manifest with dangling blobs (INV_NO_DANGLE violation).
/// promote MUST fail closed (ABORTED) unless the precommit is the current live owner binding of the ref.
TEST(CASPartWriteTxn, PromoteFailsClosedWhenPrecommitNoLongerLiveOwner)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    auto build = startBuildFor(s, ns, "part_1");

    const ManifestId id = build->stageManifest({blobManifestEntry("data.bin", "hello world")});
    build->precommitAdd(ns, "part_1", id);
    build->putBlob(idOf("hello world"), BlobSource::fromString("hello world"));

    /// Make the precommit NO LONGER the live owner: append an exact precommit-removal ref-log
    /// transaction exactly as an abandon / GC reclaim would (spec §Remove Precommit) -- via the SAME
    /// public append lane a real abandon/reclaim would use, simulating an external actor this build
    /// object does not know about (not this build's own `abandon()`, which would also retire it and
    /// mask the "precommit no longer live" guard behind requireAlive()'s own rejection).
    s->appendRefOps(ns, MutationScope::ref("part_1"),
        [&](const RefTableState &) -> std::vector<RefOp>
        {
            RefOp op;
            op.kind = RefOpKind::OwnerTransition;
            op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "part_1", id.ref};
            return {op};
        },
        RootMutationOrigin::Writer, RootMutationKind::Abandon);

    /// promote must fail closed: the precommit is no longer the live owner, so a Δ=0 move would dangle.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR,
        [&] { build->promote(ns, "part_1", build->buildId(), id); });
    /// No ref committed.
    EXPECT_FALSE(s->resolveRef(ns, "part_1").has_value());
}

/// BUG 1 happy path: a promote whose precommit is STILL the live owner succeeds (the guard must not
/// reject the normal commit). Distinct from PublishHappyPathRoundTrip in that it pins the WPromote guard.
TEST(CASPartWriteTxn, PromoteSucceedsWhenPrecommitIsLiveOwner)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    auto build = startBuildFor(s, ns, "part_1");

    const ManifestId id = build->stageManifest({blobManifestEntry("data.bin", "hello world")});
    build->precommitAdd(ns, "part_1", id);
    build->putBlob(idOf("hello world"), BlobSource::fromString("hello world"));

    EXPECT_NO_THROW(build->promote(ns, "part_1", build->buildId(), id));
    ASSERT_TRUE(s->resolveRef(ns, "part_1").has_value());
    EXPECT_EQ(s->resolveRef(ns, "part_1")->manifest_id, id);
}

/// all-tree-part-files Task 2 (TLA+ `WRepoint`):
/// `promote`'s existing unique-ref guard (BUG 1a) refuses to overwrite a committed ref naming a
/// DIFFERENT manifest -- correct for an ACCIDENTAL double-publish, but there is no way to perform an
/// INTENDED repoint (a standalone write/remove on an already-committed part) without it. `allow_repoint`
/// opts into exactly that: the guard's throw is skipped, and the committed-transition RefOp (old =
/// the currently-committed manifest, new = the incoming one) is appended in the SAME ref-log record as
/// the ordinary precommit->committed promotion -- the C++ realization of `WRepoint`'s one-event
/// old-binding/new-binding shape (Phase 0, task-1 gate). Without the flag, behavior is BYTE-IDENTICAL
/// to today (BUG 1a still fires).
TEST(CASPartWriteTxnRepoint, PromoteRepointsCommittedRef)
{
    auto b = std::make_shared<InMemoryBackend>();
    /// The sink target must outlive the Pool: `~Pool` emits terminate events into the sink.
    std::vector<CasEvent> events;
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    /// Publish ref "part_1" -> M1 through the normal build path.
    auto build1 = startBuildFor(s, ns, "part_1");
    const ManifestId m1_id = build1->stageManifest({blobManifestEntry("data.bin", "m1")});
    build1->precommitAdd(ns, "part_1", m1_id);
    build1->putBlob(idOf("m1"), BlobSource::fromString("m1"));
    build1->promote(ns, "part_1", build1->buildId(), m1_id);
    ASSERT_TRUE(s->resolveRef(ns, "part_1").has_value());
    EXPECT_EQ(s->resolveRef(ns, "part_1")->manifest_id, m1_id);

    /// A second build stages M2 (one extra entry) onto the SAME ref.
    auto build2 = startBuildFor(s, ns, "part_1");
    const ManifestId m2_id = build2->stageManifest({blobManifestEntry("data.bin", "m2"), blobManifestEntry("extra.bin", "m2x")});
    build2->precommitAdd(ns, "part_1", m2_id);
    build2->putBlob(idOf("m2"), BlobSource::fromString("m2"));
    build2->putBlob(idOf("m2x"), BlobSource::fromString("m2x"));

    /// allow_repoint = false (the default) -> NETWORK_ERROR (CAS write-retry-later), existing invariant
    /// untouched; M1 still resolves.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR,
        [&] { build2->promote(ns, "part_1", build2->buildId(), m2_id); });
    EXPECT_EQ(s->resolveRef(ns, "part_1")->manifest_id, m1_id);

    /// The failed no-flag attempt threw BEFORE appendRefOps returned, so build2's precommit is still the
    /// live owner (no removal was appended) -- the SAME build/manifest can be retried with the flag.
    s->setEventSink([&](const CasEvent & e) { events.push_back(e); });
    EXPECT_NO_THROW(build2->promote(ns, "part_1", build2->buildId(), m2_id, /*allow_repoint=*/true));
    auto resolved = s->resolveRef(ns, "part_1");
    ASSERT_TRUE(resolved);
    EXPECT_EQ(resolved->manifest_id.ref, m2_id.ref);

    /// Every effective repoint is loud (spec §4): exactly one RefRepoint event, naming the ref and the
    /// old manifest it replaced.
    size_t repoint_events = 0;
    for (const CasEvent & e : events)
        if (e.type == CasEventType::RefRepoint)
        {
            ++repoint_events;
            EXPECT_EQ(e.ref_name, "part_1");
            EXPECT_EQ(e.detail.at("old_manifest"), manifestRefDebugString(m1_id.ref));
        }
    EXPECT_EQ(repoint_events, 1u);
}

/// BUG 2 (WAbandonPrecommit; delete-after-sealed-decrements): once `precommitAdd` has made a manifest a
/// LIVE precommit owner input, `abandon` must NOT writer-delete its body. The TLA+ `WAbandonPrecommit`
/// appends a REMOVAL event (`old = precommit(build_id, final_ref, T)`, `new = none`) and NEVER deletes
/// the body — GC decrements the precommit's blob edges and deletes the body only after the decrement is
/// sealed. Writer-deleting a live precommit body strands GC's fold barrier (live precommit, missing body
/// → clamp forever) or loses the activating +1.
TEST(CASPartWriteTxn, AbandonAppendsPrecommitRemovalAndKeepsLivePrecommitBody)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    auto build = startBuildFor(s, ns, "part_1");

    const ManifestId mid = build->stageManifest({blobManifestEntry("data.bin", "kept")});
    const String manifest_key = s->layout().manifestKey(mid);
    const UInt128 abandoned_build_id = build->buildId();
    build->precommitAdd(ns, "part_1", mid);
    build->putBlob(idOf("kept"), BlobSource::fromString("kept"));

    /// The precommit manifest body is present before abandon.
    ASSERT_TRUE(b->head(manifest_key).exists);

    build->abandon();

    /// (a) the LIVE precommit body must SURVIVE abandon (left for GC after the sealed decrement).
    EXPECT_TRUE(b->head(manifest_key).exists)
        << "abandon must NOT writer-delete a live precommit body (delete-after-sealed-decrements)";

    /// (b) the exact precommit binding is gone (spec §Remove Precommit: an exact owner_transition
    /// removal, old=Precommit new=none). Proven black-box: a FRESH precommitAdd for the ref must
    /// succeed -- if abandon had failed to remove the exact binding, this would instead throw
    /// CORRUPTED_DATA ("add precommit ... already exists"). The probe manifest must be freshly staged
    /// BY `rebuild` itself rather than re-precommitting `mid` (which `build`, a different transaction,
    /// staged): A3 mint-tightening now refuses an unowned `ManifestId` from any transaction other than
    /// the one that minted it, regardless of whether abandon's removal landed, so re-using `mid` here
    /// would no longer distinguish the property under test. Content identity is irrelevant to the
    /// ref-level owner-slot check this proves, so a fresh manifest is just as conclusive a probe.
    (void)abandoned_build_id;
    auto rebuild = startBuildFor(s, ns, "part_1");
    const ManifestId rebuild_mid = rebuild->stageManifest({blobManifestEntry("data.bin", "kept")});
    EXPECT_NO_THROW(rebuild->precommitAdd(ns, "part_1", rebuild_mid));
}

/// BUG 2 regression for the never-precommitted path: a manifest that was STAGED but never precommitted is
/// still best-effort writer-deleted by abandon (pre-precommit debris) — only a LIVE precommit body is
/// spared. Confirms the fix narrows the skip to the precommitted manifest exactly.
TEST(CASPartWriteTxn, AbandonStillDeletesNeverPrecommittedStagedDebris)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    auto build = startBuildFor(s, ns, "part_1");

    /// Two staged manifests: one becomes the precommit, the other is pure pre-precommit debris.
    const ManifestId debris = build->stageManifest({blobManifestEntry("debris.bin", "kept")});
    const ManifestId precommitted = build->stageManifest({blobManifestEntry("data.bin", "kept")});
    build->precommitAdd(ns, "part_1", precommitted);
    build->putBlob(idOf("kept"), BlobSource::fromString("kept"));

    build->abandon();

    /// The never-precommitted debris is best-effort deleted; the live precommit body survives.
    EXPECT_FALSE(b->head(s->layout().manifestKey(debris)).exists)
        << "never-precommitted staged debris must still be best-effort deleted by abandon";
    EXPECT_TRUE(b->head(s->layout().manifestKey(precommitted)).exists)
        << "the live precommit body must be spared";
}

/// Task 6 (review finding 2): `abandon()`'s three audit `EventEmitter{*store}.emit(...)` calls are each
/// wrapped `try { ... } catch (...) { tryLogCurrentException(...); }`, mirroring `promote`'s own
/// post-durable emit guard -- a throwing sink (e.g. a bad_alloc growing the system-log queue, or a
/// Context/log-shutdown edge) must never turn an otherwise-successful abandon into a reported failure.
TEST(CASPartWriteTxn, AbandonSwallowsThrowingEventSink)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl_abandon_sink"};
    auto build = startBuildFor(s, ns, "part_1");

    const ManifestId mid = build->stageManifest({blobManifestEntry("data.bin", "kept")});
    build->precommitAdd(ns, "part_1", mid);
    build->putBlob(idOf("kept"), BlobSource::fromString("kept"));

    /// UNKNOWN_EXCEPTION (not LOGICAL_ERROR): mirrors `PromoteSwallowsPostDurableEventSinkFailure`
    /// above -- this simulates an arbitrary observer/sink callback failing, not a CAS invariant
    /// violation. LOGICAL_ERROR would abort the whole process under debug/sanitizer builds instead of
    /// behaving like a catchable exception.
    s->setEventSink([](const CasEvent &)
    {
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_EXCEPTION, "injected event sink failure");
    });

    EXPECT_NO_THROW(build->abandon());
    s->setEventSink(nullptr);

    /// The precommit binding is gone despite the sink failure -- proven black-box exactly like
    /// `AbandonAppendsPrecommitRemovalAndKeepsLivePrecommitBody` above: a FRESH precommitAdd for the
    /// ref must succeed (it would instead throw CORRUPTED_DATA "add precommit ... already exists" had
    /// the throwing sink aborted the removal). The probe manifest is freshly staged BY `rebuild`
    /// itself, not `mid` (staged by `build`, a different transaction) -- A3 mint-tightening now refuses
    /// a foreign id unconditionally, so re-using `mid` would no longer isolate the property under test.
    auto rebuild = startBuildFor(s, ns, "part_1");
    const ManifestId rebuild_mid = rebuild->stageManifest({blobManifestEntry("data.bin", "kept")});
    EXPECT_NO_THROW(rebuild->precommitAdd(ns, "part_1", rebuild_mid));
}

namespace
{

/// Forces the SINGLE ref-log ('_log/' key) PUT that `abandon()`'s precommit-removal `appendRefOps`
/// issues to observe a PROVEN conflict instead of a genuine ambiguity. Mirrors
/// `RefWriterTestBackend::corrupt_key_substr` (gtest_cas_ref_writer.cpp, reproduced locally because
/// that class lives in a different translation unit): landing a DIFFERENT object at the intended key
/// makes `putIfAbsentControlled`'s resolve-before-reissue observe a real conflict and throw
/// CORRUPTED_DATA -- a CONCLUSIVE rejection ("do NOT wedge: the cache is unchanged and nothing of ours
/// is durable", CasRefLedger.cpp's `commitRefChunk`), unlike a genuinely-ambiguous timeout, which would
/// instead WEDGE the whole table's append lane (`rt->append_attempt`) until the SAME key resolves durable -- a
/// state a one-shot fault can never itself clear, since wedge resolution only re-GETs the intended key
/// and never re-PUTs it (proven by
/// `CASRefWriterAppendLane.WedgedLaneBlocksSameTableWhileOtherTableProceeds`). A conflict leaves the cached
/// ref-table state untouched, so the SAME logical retry reaches its append again -- and under INV-1 that
/// retry re-derives the SAME id from that unchanged state, so it meets the same occupant rather than
/// carving a fresh id around it.
class RefLogConflictOnceBackend final : public InMemoryBackend
{
public:
    String corrupt_key_substr;
    int corrupt_count = 0;

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        if (corrupt_count > 0 && !corrupt_key_substr.empty() && key.find(corrupt_key_substr) != String::npos)
        {
            --corrupt_count;
            /// The 3-arg qualified call bypasses virtual dispatch entirely (unlike the 2-arg
            /// convenience overload, which would re-enter this very override through the vtable).
            InMemoryBackend::putIfAbsent(key, bytes + String("\x01_FOREIGN_DIFFERENT"), meta);
            throw Poco::TimeoutException("RefLogConflictOnceBackend: a foreign different object landed; response lost");
        }
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }
};

}

/// Task 6 (review finding 2): `alive` now flips to false only AFTER the correctness-bearing precommit
/// removal's `appendRefOps` succeeds, so a caller that catches an append failure can retry `abandon()`
/// on the SAME object. Before the fix, `alive = false` ran unconditionally before that append, so a
/// retry would hit `requireAlive`'s "has been abandoned" LOGICAL_ERROR instead.
TEST(CASPartWriteTxn, AbandonRetryableAfterAppendFailure)
{
    auto b = std::make_shared<RefLogConflictOnceBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const RootNamespace ns{"srv1/tbl_abandon_retry"};
    /// Stage B (Task 4-C): pin `ns`'s real incarnation to the Stage-A sentinel BEFORE the first real
    /// append, so the corruption injected below (at a key computed from that sentinel) actually lands
    /// on the path production writes to -- otherwise `precommitAdd` mints an unrelated random
    /// incarnation and the corruption below misses it entirely.
    DB::Cas::tests::casAdmitRecoverableEntry(*b, s->layout(), ns, s->liveWriterEpoch());
    auto build = startBuildFor(s, ns, "part_1");

    const ManifestId mid = build->stageManifest({blobManifestEntry("data.bin", "kept")});
    build->precommitAdd(ns, "part_1", mid);
    build->putBlob(idOf("kept"), BlobSource::fromString("kept"));

    b->corrupt_key_substr = s->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    b->corrupt_count = 1;

    /// First abandon(): the precommit-removal appendRefOps' single PUT observes a foreign object at its
    /// exact key (a proven conflict) -> CORRUPTED_DATA propagates.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { build->abandon(); });

    /// The proven conflict fences the mount closed and schedules a remount (the append site routes
    /// through the anomaly policy exactly as the wedge-resolve site does). Re-arming only the test
    /// fence does not replace this runtime, so its immutable admitted generation remains stale and its
    /// terminal `Faulted` lane remains blocked behind that outer refusal.
    DB::Cas::tests::rearmMountFenceAfterAnomalyForTest(s);

    /// The retryability under test: the SAME object accepts a second abandon() -- `alive` was not
    /// flipped by the failed append, so this is not the "has been abandoned" condition the unfixed code
    /// produced. It reaches immutable-runtime admission and is refused by the stale generation; only a
    /// real remount may replace that runtime and reach a fresh lane.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { build->abandon(); });

    /// The removal never landed, and nothing was written around the occupant: the precommit binding this
    /// build owns is still live, exactly where the failed abandon left it. That is the honest end state
    /// under the fail-closed contract -- the old proof (a fresh `precommitAdd` for the same ref
    /// succeeding, which showed the binding gone) needed one more successful append on a table that can
    /// no longer take one.
    String greatest_key;
    size_t foreign_objects = 0;
    for (String cursor;;)
    {
        const ListPage page = b->list(b->corrupt_key_substr, cursor, 1000);
        for (const auto & listed : page.keys)
        {
            if (listed.key > greatest_key)
                greatest_key = listed.key;
            const auto body = b->get(listed.key);
            if (body && body->bytes.find("_FOREIGN_DIFFERENT") != String::npos)
                ++foreign_objects;
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    EXPECT_EQ(foreign_objects, 1u) << "the foreign object must still own the key it took";
    ASSERT_FALSE(greatest_key.empty());
    const auto greatest_body = b->get(greatest_key);
    ASSERT_TRUE(greatest_body.has_value());
    EXPECT_NE(greatest_body->bytes.find("_FOREIGN_DIFFERENT"), String::npos)
        << "the foreign occupant must still be the highest id in this table's stream: a log object above "
           "it would mean an append carved a fresh id around the damage instead of failing closed";
}

/// ------------------------------------------------------------------------------------------------
/// OQ7 manifest-cap fail-close (S07): the scenario suite tried to reach `stageManifest`'s encoded-bytes
/// cap through a wide-column SQL `INSERT`, but the cap sits 3+ orders of magnitude above what dev SQL
/// can reach in reasonable time (confirmed: even a 20000-column full-scale insert cannot get there). So
/// this P0 safety path is not scenario-testable and is exercised directly here instead.
/// ------------------------------------------------------------------------------------------------

namespace
{

/// Mirrors `CasPartWriteTxn.cpp`'s private `kMaxManifestEncodedBytes` (256 MiB). There is no way to read a
/// file-local `constexpr` from a different translation unit, so this is kept in sync by hand — if that
/// cap ever changes, update this one to match.
constexpr uint64_t kExpectedManifestEncodedCap = 256ULL << 20;

/// The exact encoded size `PartWriteTxn::stageManifest` would compute for a single Blob-placement entry whose
/// path is `path_len` bytes long, staged under `ns` — measured through the SAME `encodePartManifest`
/// codec `stageManifest` calls, so this is an exact reproduction rather than a hand-derived estimate.
/// `ref` and `payload_digest` are fixed-width fields (20 and 16 bytes respectively): their VALUES don't
/// affect the encoded size, only their presence does, so the zero-valued placeholders here reproduce
/// the exact same byte count `stageManifest` would produce with its real (non-zero) values.
size_t manifestEncodedSizeForPathLen(const RootNamespace & ns, size_t path_len)
{
    PartManifest probe;
    probe.root_namespace_id = ns;
    ManifestEntry e;
    e.path = String(path_len, 'a');
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(UInt128{})};

    e.blob_size = 12345;
    probe.entries = {std::move(e)};
    return encodePartManifest(probe).size();
}

/// Finds the exact boundary: the SMALLEST `path_len` whose single-entry manifest encodes to MORE than
/// `kExpectedManifestEncodedCap` bytes under `ns`. `path_len - 1` therefore encodes to AT MOST the cap
/// (the encoding is monotonic in `path_len` — a longer path can only grow the encoded size). Starts
/// from a linear estimate (the encoding is affine in `path_len`: fixed framing overhead plus a constant
/// number of bytes per path byte) and walks to the exact crossing, so this stays correct even if the
/// framing overhead changes, without needing a full binary search over a ~256 MiB range.
size_t findManifestEncodedCapBoundaryPathLen(const RootNamespace & ns)
{
    constexpr size_t probe_lo = 1000;
    constexpr size_t probe_hi = 2'000'000;
    const size_t size_lo = manifestEncodedSizeForPathLen(ns, probe_lo);
    const size_t size_hi = manifestEncodedSizeForPathLen(ns, probe_hi);
    const double slope = static_cast<double>(size_hi - size_lo) / static_cast<double>(probe_hi - probe_lo);
    const double intercept = static_cast<double>(size_lo) - slope * static_cast<double>(probe_lo);

    size_t path_len = static_cast<size_t>(std::ceil(
        (static_cast<double>(kExpectedManifestEncodedCap) - intercept) / slope)) + 1;

    while (manifestEncodedSizeForPathLen(ns, path_len) <= kExpectedManifestEncodedCap)
        ++path_len;
    while (path_len > 1 && manifestEncodedSizeForPathLen(ns, path_len - 1) > kExpectedManifestEncodedCap)
        --path_len;
    return path_len;
}

/// A one-entry Blob ManifestEntry with a synthetic `path_len`-byte path (used only to inflate the
/// encoded manifest size towards the OQ7 cap).
ManifestEntry wideBlobManifestEntry(size_t path_len)
{
    ManifestEntry e;
    e.path = String(path_len, 'a');
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(UInt128{0x42})};

    e.blob_size = 12345;
    return e;
}

}

/// Boundary case 1/2: a manifest whose encoded size is the LARGEST that still fits under the cap stages
/// successfully. Proves the cap enforcement isn't overly conservative — a real just-under-the-limit
/// manifest is not mistakenly rejected.
TEST(CASPartWriteTxn, ManifestCapEncodedBytesJustUnderStagesSuccessfully)
{
    auto b = std::make_shared<InMemoryBackend>();
    /// A frozen boot_ms_fn (not the shared openPool helper): this test's manifest sits just under the
    /// 256 MiB cap, so encodePartManifest/sealObject do real, sizeable CPU work before the single
    /// InMemoryBackend put (which always succeeds deterministically, no faults). Under heavy
    /// instrumentation (TSan) that encode+seal step alone can take long enough in real wall-clock time
    /// to cross the mount lease's fence margin (CasMountRuntime::refAppendFenceOk) and the CAS request
    /// controller's own deadline (both consult the SAME injected clock, CasRefLedger.cpp) before the
    /// attempt even resolves -- a sanitizer-speed artifact unrelated to what this test verifies. Freezing
    /// the clock decouples the outcome from real execution speed: the single attempt now succeeds or
    /// fails purely on the backend's own (deterministic) behavior, on any build.
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .boot_ms_fn = [] { return uint64_t{0}; }});
    const RootNamespace ns{"srv1/tbl"};

    const size_t path_len_over = findManifestEncodedCapBoundaryPathLen(ns);
    ASSERT_GT(path_len_over, 1u);
    const size_t path_len_under = path_len_over - 1;
    ASSERT_LE(manifestEncodedSizeForPathLen(ns, path_len_under), kExpectedManifestEncodedCap);

    auto build = startBuildFor(s, ns, "wide_part");
    const ManifestId id = build->stageManifest({wideBlobManifestEntry(path_len_under)});
    EXPECT_EQ(id.root_namespace, ns);
    EXPECT_TRUE(b->head(s->layout().manifestKey(id)).exists)
        << "a just-under-cap manifest must actually be written";
}

/// Boundary case 2/2: a manifest whose encoded size exceeds the cap by the smallest possible margin
/// (one more path byte than the passing case above) throws `LIMIT_EXCEEDED` fail-closed, BEFORE the body
/// write — no manifest object lands in the backend for the rejected attempt.
TEST(CASPartWriteTxn, ManifestCapEncodedBytesOverThrowsBeforeBodyWrite)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    const size_t path_len_over = findManifestEncodedCapBoundaryPathLen(ns);
    ASSERT_GT(manifestEncodedSizeForPathLen(ns, path_len_over), kExpectedManifestEncodedCap);

    auto build = startBuildFor(s, ns, "wide_part");

    const size_t keys_before = b->list("", "", 100).keys.size();
    bool threw = false;
    try
    {
        build->stageManifest({wideBlobManifestEntry(path_len_over)});
    }
    catch (const DB::Exception & e)
    {
        threw = true;
        EXPECT_EQ(e.code(), DB::ErrorCodes::LIMIT_EXCEEDED);
        EXPECT_NE(e.message().find("exceeds cap"), String::npos) << e.message();
    }
    EXPECT_TRUE(threw) << "an over-cap manifest must throw, not silently truncate or accept";

    /// Fail-closed BEFORE the body write: the over-cap attempt must not have created ANY new object
    /// (no partial state, no orphaned blob/manifest debris for a manifest that was never accepted).
    const size_t keys_after = b->list("", "", 100).keys.size();
    EXPECT_EQ(keys_before, keys_after)
        << "stageManifest must fail closed before writing the manifest body, leaving no new objects";
}

/// spec §9.9 (mixed-algo pools, Phase 3 T2) — the W-DEP-SET cross-satisfaction crux: a manifest with
/// two entries carrying the SAME digest VALUE under TWO DIFFERENT algos (`ch128:X` / `xxh3:X`). Only
/// `ch128:X`'s body is ever putBlob'd; `xxh3:X`'s body never lands anywhere. Promote MUST fail closed —
/// the materialized `ch128:X` proof must never satisfy the missing `xxh3:X` proof.
/// This test is RED (wrongly passes / silently promotes) if `PartWriteTxn::deps` (the W-DEP-SET) were keyed on
/// a bare digest instead of the full `BlobRef` pair: both entries would collapse to the SAME map key
/// (the digest alone), so the proof query would report the xxh3 leaf as edge-protected via the ch128
/// entry's putBlob and promote would skip its revalidation (and hence its absence) entirely.
TEST(CASPartWriteTxn, WDepSetCrossAlgoSatisfactionFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv/tbl"};

    const BlobDigest shared_digest = BlobDigest::fromU128(u128Of("shared-digest-value"));

    ManifestEntry e_ch128;
    e_ch128.path = "a.bin";
    e_ch128.placement = EntryPlacement::Blob;
    e_ch128.ref = BlobRef{BlobHashAlgo::CityHash128, shared_digest};
    e_ch128.blob_size = 3;

    ManifestEntry e_xxh3;
    e_xxh3.path = "b.bin";
    e_xxh3.placement = EntryPlacement::Blob;
    e_xxh3.ref = BlobRef{BlobHashAlgo::XXH3_128, shared_digest};   /// SAME digest bytes, DIFFERENT algo
    e_xxh3.blob_size = 3;

    auto build = startBuildFor(s, ns, "part_mixed");
    const ManifestId id = build->stageManifest({e_ch128, e_xxh3});
    build->precommitAdd(ns, "part_mixed", id);

    /// Only the ch128 leaf's body is ever uploaded — its BlobId hex is the digest at the ch128 width,
    /// which addresses EXACTLY `e_ch128`'s object key (`blobs/ch128/...`), a DISTINCT key from
    /// `e_xxh3`'s (`blobs/xxh3/...`), even though the raw digest bytes are identical.
    build->putBlob(BlobRef{BlobHashAlgo::CityHash128, shared_digest}, BlobSource::fromString("abc"));

    /// Promotion must fail closed: the xxh3:X leaf has no dependency proof — never silently
    /// satisfied by the ch128:X entry's materialized proof (same digest bytes, distinct object key). §4
    /// manifest-trust catches an unsatisfied leaf by the dependency set and
    /// fails closed with LOGICAL_ERROR — a staging bug — without any backend probe on the xxh3 key.
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->promote(ns, "part_mixed", build->buildId(), id);
        },
        "no dependency proof");

    /// No committed ref appears — the promote aborted before installing one.
    EXPECT_FALSE(s->resolveRef(ns, "part_mixed").has_value());
}

/// =====================================================================================
/// Task B (chaos-tolerance-report §Task B): stageManifest's part-manifest conditional PUT rides the
/// shared CasRequestController — budgeted attempts + resolve-before-reissue — instead of the old
/// single bare attempt (which a 19s object-store pause killed while every read path survived).
/// =====================================================================================

namespace
{

/// Faults the part-manifest body PUT (`/cas/manifests/` keys) with an ambiguous
/// (Unresolved-classified) timeout a bounded number of times, mirroring RefWriterTestBackend's fault
/// seam (gtest_cas_ref_writer.cpp). Part manifests use the small-object `putIfAbsent` primitive.
class ManifestPutFaultBackend final : public InMemoryBackend
{
public:
    int fault_count = 0;                 /// remaining ambiguous faults on matching body PUTs
    bool land_despite_fault = false;     /// the faulted attempt's own write actually lands (response lost)
    String plant_different_on_fault;     /// a FOREIGN different body lands at the key before the fault
    int put_attempts = 0;                /// matching body-PUT attempts observed

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        if (!isManifestBodyKey(key))
            return InMemoryBackend::putIfAbsent(key, bytes, meta);
        ++put_attempts;
        maybeFault(key, bytes);
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }

private:
    static bool isManifestBodyKey(const String & key) { return key.find("/cas/manifests/") != String::npos; }

    /// One fault: apply the configured server-side effect, then lose the response.
    void maybeFault(const String & key, const String & bytes)
    {
        if (fault_count <= 0)
            return;
        --fault_count;
        if (!plant_different_on_fault.empty())
            InMemoryBackend::putIfAbsent(key, plant_different_on_fault, {});
        else if (land_despite_fault)
            InMemoryBackend::putIfAbsent(key, bytes, {});
        throw Poco::TimeoutException("ManifestPutFaultBackend: simulated ambiguous result (response lost)");
    }
};

}

/// The Task B core: two consecutive ambiguous timeouts on the part-manifest body PUT (each resolved
/// to "absent" by the controller's exact-GET), then a clean third attempt. The old single-attempt
/// path fails the whole stage on the FIRST timeout (the observed 19s-pause INSERT kill); the
/// controller path must ride its attempt budget and succeed.
TEST(CASPartWriteTxnStageManifestRetry, AmbiguousTimeoutsThenCommitSucceedsWithinBudget)
{
    /// Zero backoff: the retry semantics are under test here, not the (controller-level-tested)
    /// inter-attempt sleep schedule — keep the suite free of real sleeps.
    CasRequestBudget budget;
    budget.retry_initial_backoff_ms = 0;
    auto b = std::make_shared<ManifestPutFaultBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .cas_request_budget = budget});
    const RootNamespace ns{"srv/tbl"};

    auto build = startBuildFor(s, ns, "part_retry");
    b->fault_count = 2;
    const ManifestId id = build->stageManifest({blobManifestEntry("a.bin", "a")});

    EXPECT_EQ(b->put_attempts, 3) << "two faulted attempts + the committing third";
    const auto got = b->get(s->layout().manifestKey(id));
    ASSERT_TRUE(got.has_value()) << "the staged manifest body must be durable";
    EXPECT_EQ(decodePartManifest(openObject(FormatId::PartManifest, got->bytes)).ref, id.ref);
}

/// Ambiguous-but-landed: the FIRST attempt's response is lost AFTER the write actually landed
/// server-side. Resolve-before-reissue's exact-GET observes the identical bytes and reports
/// Committed — the stage succeeds WITHOUT a reissue (no duplicate PUT of the object), and the
/// `ManifestPut` audit event carries the landed incarnation's token (from the resolve GET).
TEST(CASPartWriteTxnStageManifestRetry, AmbiguousLandedWriteResolvesToCommittedWithoutReissue)
{
    auto b = std::make_shared<ManifestPutFaultBackend>();
    /// The sink target must outlive the Pool: `~Pool` emits terminate events into the sink.
    std::vector<CasEvent> events;
    auto s = openPool(b);
    const RootNamespace ns{"srv/tbl"};

    s->setEventSink([&](const CasEvent & e) { events.push_back(e); });

    auto build = startBuildFor(s, ns, "part_landed");
    b->fault_count = 1;
    b->land_despite_fault = true;
    const ManifestId id = build->stageManifest({blobManifestEntry("a.bin", "a")});

    EXPECT_EQ(b->put_attempts, 1) << "a landed ambiguous attempt must be resolved, never reissued";
    const String key = s->layout().manifestKey(id);
    ASSERT_TRUE(b->get(key).has_value());

    const auto ev = std::find_if(events.begin(), events.end(),
                                 [](const CasEvent & e) { return e.type == CasEventType::ManifestPut; });
    ASSERT_NE(ev, events.end()) << "the stage must still emit its ManifestPut audit event";
    EXPECT_EQ(ev->token, b->head(key).token.value)
        << "the audit token must be the landed incarnation's token";
}

/// A DIFFERENT object at the exact staged key (a foreign body ahead of our ambiguous attempt) is a
/// proven conflict — the NoManifestIdReuse invariant broke — and must stay the loud CORRUPTED_DATA
/// class: never a retry signal, never silently adopted.
TEST(CASPartWriteTxnStageManifestRetry, DifferentObjectAtKeyStaysLoudConflict)
{
    auto b = std::make_shared<ManifestPutFaultBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv/tbl"};

    auto build = startBuildFor(s, ns, "part_conflict");
    b->fault_count = 1;
    b->plant_different_on_fault = "a-foreign-different-manifest-body";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        build->stageManifest({blobManifestEntry("a.bin", "a")});
    });
    EXPECT_EQ(b->put_attempts, 1) << "a proven conflict is never retried";
}

/// Budget exhaustion: EVERY attempt is ambiguous and nothing ever lands. The controller reports
/// Unresolved after `max_attempts` and stageManifest maps it to NETWORK_ERROR (fix #37 phase 2) —
/// the same retryable abort class the ref-log lane's exhausted budget maps to. Nothing was durably
/// named: the caller re-stages with a fresh ManifestId.
TEST(CASPartWriteTxnStageManifestRetry, BudgetExhaustionMapsToNetworkError)
{
    CasRequestBudget budget;
    budget.max_attempts = 3;
    budget.retry_initial_backoff_ms = 0;   /// no real sleeps; the backoff schedule has its own tests
    auto b = std::make_shared<ManifestPutFaultBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .cas_request_budget = budget});
    const RootNamespace ns{"srv/tbl"};

    auto build = startBuildFor(s, ns, "part_exhausted");
    b->fault_count = 1000;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        build->stageManifest({blobManifestEntry("a.bin", "a")});
    });
    EXPECT_EQ(b->put_attempts, 3) << "attempts must be bounded by the configured budget";
}

/// =====================================================================================
/// Blob publication retries re-stream from the writer's replayable source. A retry after a failed
/// server-side copy retags and streams from the intact source instead of repeating the copy.
/// =====================================================================================

namespace
{

/// Faults unconditional blob publications with an ambiguous timeout a bounded number of times.
/// Blob metadata writes (`.meta` keys, plain `putIfAbsent`) are never faulted.
class BlobPutFaultBackend final : public InMemoryBackend
{
public:
    int fault_count = 0;                 /// remaining ambiguous faults on matching create attempts
    bool land_despite_fault = false;     /// the faulted attempt's own write actually lands (response lost)
    int publish_stream_attempts = 0;     /// unconditional streaming publications observed
    int publish_copy_attempts = 0;       /// unconditional native-copy publications observed
    int blob_head_attempts = 0;          /// transaction-level blob observations

    HeadResult head(const String & key) override
    {
        if (isBlobBodyKey(key))
            ++blob_head_attempts;
        return InMemoryBackend::head(key);
    }

    void publishBlob(const BlobPublishRequest & request) override
    {
        if (std::holds_alternative<StreamingBlobPublication>(request.publication))
            ++publish_stream_attempts;
        else
            ++publish_copy_attempts;

        if (fault_count > 0)
        {
            --fault_count;
            if (land_despite_fault)
                InMemoryBackend::publishBlob(request);
            else if (const auto * streaming = std::get_if<StreamingBlobPublication>(&request.publication))
                (void)streaming->open_payload();
            throw Poco::TimeoutException("BlobPutFaultBackend: simulated ambiguous publication (response lost)");
        }
        InMemoryBackend::publishBlob(request);
    }

private:
    static bool isBlobBodyKey(const String & key)
    {
        return key.find("/blobs/") != String::npos && !key.ends_with(".meta");
    }

};

/// Zero-backoff store over a BlobPutFaultBackend: the sleep schedule has its own controller-level
/// tests; these Pool-level tests pin the retry/resolve/abort semantics without real sleeps.
PoolPtr openBlobFaultPool(const std::shared_ptr<BlobPutFaultBackend> & b, uint32_t max_attempts = CasRequestBudget{}.max_attempts)
{
    CasRequestBudget budget;
    budget.max_attempts = max_attempts;
    budget.retry_initial_backoff_ms = 0;
    return Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .cas_request_budget = budget});
}

/// A replayable BlobSource that COUNTS its own re-streams — pins INV-1's "retry = fresh re-stream
/// from the writer's own source" (never a GET of the dying/failed object).
BlobSource countingSource(const String & payload, int & payload_streams)
{
    BlobSource source;
    source.size = payload.size();
    source.open = [payload, &payload_streams]() -> std::unique_ptr<DB::ReadBuffer>
    {
        ++payload_streams;
        return std::make_unique<DB::ReadBufferFromOwnString>(payload);
    };
    return source;
}

}

/// The core ride: two consecutive ambiguous timeouts on the blob-body streaming PUT (each resolved
/// "absent" by the controller's occupancy HEAD), then a clean third attempt. The old single-attempt
/// path failed the whole INSERT on the FIRST timeout (the raw Poco::TimeoutException escaped
/// putBlob); the controller path rides its budget, RE-STREAMING the payload from the writer's own
/// replayable source on every attempt.
TEST(CASPartWrite, AmbiguousTimeoutsThenCommitRestreamsFromSource)
{
    auto b = std::make_shared<BlobPutFaultBackend>();
    auto s = openBlobFaultPool(b);
    const RootNamespace ns{"srv/tbl"};
    const String payload = "blob-payload-A";

    auto build = startBuildFor(s, ns, "part_blob_retry");
    const ManifestId id = build->stageManifest({blobManifestEntry("a.bin", payload)});
    build->precommitAdd(ns, "part_blob_retry", id);

    int payload_streams = 0;
    b->fault_count = 2;
    const PutBlobResult res = build->putBlob(idOf(payload), countingSource(payload, payload_streams));
    EXPECT_EQ(res.size, payload.size());

    EXPECT_EQ(b->publish_stream_attempts, 3) << "two ambiguous publications + the committing third";
    EXPECT_EQ(b->blob_head_attempts, 3) << "every outer retry restarts from a fresh blob HEAD";
    EXPECT_EQ(payload_streams, 3) << "every reissue must RE-STREAM from the writer's own source (INV-1)";
    EXPECT_TRUE(b->head(s->layout().blobKey(idOf(payload))).exists) << "the blob body must be durable";
}

/// Ambiguous-but-landed: the FIRST attempt's response is lost AFTER the write actually landed
/// server-side. The occupancy resolve observes the key present and the existing 412 machinery takes
/// over — the occupant is ADOPTED (content-addressed identity: any occupant of this key IS the
/// content), with NO reissue and NO second body upload.
TEST(CASPartWrite, AmbiguousLandedWriteAdoptsOccupantWithoutReupload)
{
    auto b = std::make_shared<BlobPutFaultBackend>();
    /// The sink target must outlive the Pool: `~Pool` emits terminate events into the sink.
    std::vector<CasEvent> events;
    auto s = openBlobFaultPool(b);
    const RootNamespace ns{"srv/tbl"};
    const String payload = "blob-payload-B";

    s->setEventSink([&](const CasEvent & e) { events.push_back(e); });

    auto build = startBuildFor(s, ns, "part_blob_landed");
    const ManifestId id = build->stageManifest({blobManifestEntry("a.bin", payload)});
    build->precommitAdd(ns, "part_blob_landed", id);

    int payload_streams = 0;
    b->fault_count = 1;
    b->land_despite_fault = true;
    const PutBlobResult res = build->putBlob(idOf(payload), countingSource(payload, payload_streams));
    EXPECT_EQ(res.size, payload.size());

    EXPECT_EQ(b->publish_stream_attempts, 1) << "a landed ambiguous attempt must be observed, never reissued";
    EXPECT_EQ(b->blob_head_attempts, 2) << "the ambiguity is resolved by restarting from HEAD";
    EXPECT_EQ(payload_streams, 1);

    const String key = s->layout().blobKey(idOf(payload));
    const auto adopt = std::find_if(events.begin(), events.end(),
                                    [](const CasEvent & e) { return e.type == CasEventType::BlobReuseAdopt; });
    ASSERT_NE(adopt, events.end()) << "the landed occupant must be ADOPTED (the standard dedup leg)";
    EXPECT_EQ(adopt->token, b->head(key).token.value) << "the adopted token must be the landed incarnation's";
    EXPECT_EQ(std::count_if(events.begin(), events.end(),
                            [](const CasEvent & e) { return e.type == CasEventType::BlobPut; }), 0)
        << "no fresh-upload event: the body was never re-uploaded";
}

/// Budget exhaustion: EVERY attempt is ambiguous and nothing ever lands. The controller reports the
/// uncertainty and `ensureBlobPresent` maps it to `NETWORK_ERROR` -- the same retryable
/// abort class stageManifest and the ref-log lane map their exhausted budgets to. Unlike the OLD
/// ABORTED mapping, putBlob's bounded condemned-churn loop (8 rounds) does NOT re-drive this: it only
/// catches ABORTED, so a NETWORK_ERROR escapes on the FIRST attempt -- desirable (no point hammering a
/// lost fence locally 8 times; the caller's own backoff, e.g. the merge queue's, is what should retry).
TEST(CASPartWrite, AmbiguousNonLandingPublicationStopsAtOuterBound)
{
    auto b = std::make_shared<BlobPutFaultBackend>();
    auto s = openBlobFaultPool(b, /*max_attempts=*/3);
    const RootNamespace ns{"srv/tbl"};
    const String payload = "blob-payload-C";

    auto build = startBuildFor(s, ns, "part_blob_exhausted");
    const ManifestId id = build->stageManifest({blobManifestEntry("a.bin", payload)});
    build->precommitAdd(ns, "part_blob_exhausted", id);

    int payload_streams = 0;
    b->fault_count = 1000000;
    bool threw = false;
    try
    {
        build->putBlob(idOf(payload), countingSource(payload, payload_streams));
    }
    catch (const DB::Exception & e)
    {
        threw = true;
        EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR);
        EXPECT_NE(e.message().find("ambiguous"), String::npos) << e.message();
    }
    EXPECT_TRUE(threw);
    EXPECT_EQ(b->publish_stream_attempts, 8) << "the writer's correctness retry loop is bounded";
    EXPECT_EQ(b->blob_head_attempts, 8) << "every ambiguous retry is preceded by a new observation";
    EXPECT_EQ(payload_streams, 8);
}

/// A server-side copy publication is ambiguous-but-landed: its response is lost after the
/// destination was created. The occupancy resolve observes the destination present and the occupant
/// is adopted — Committed-in-effect WITHOUT a re-copy.
TEST(CASPartWrite, AmbiguousCopyLandedAdoptsDestinationWithoutRecopy)
{
    auto b = std::make_shared<BlobPutFaultBackend>();
    /// The sink target must outlive the Pool: `~Pool` emits terminate events into the sink.
    std::vector<CasEvent> events;
    auto s = openBlobFaultPool(b);
    const RootNamespace ns{"srv/tbl"};
    const String payload = "staged-payload-A";
    /// The staging object: [pool-fixed-length envelope header][payload], promoted VERBATIM by the copy.
    const String staging_key = "p/staging/test/blob-a";
    const String staging_bytes = String(s->poolMeta().blob_header_len, 'h') + payload;
    ASSERT_EQ(b->putIfAbsent(staging_key, staging_bytes).outcome, PutOutcome::Done);

    s->setEventSink([&](const CasEvent & e) { events.push_back(e); });

    auto build = startBuildFor(s, ns, "part_copy_landed");
    const ManifestId id = build->stageManifest({blobManifestEntry("a.bin", payload)});
    build->precommitAdd(ns, "part_copy_landed", id);

    BlobSource source;
    source.size = payload.size();
    source.server_side_copy_from = staging_key;
    b->fault_count = 1;
    b->land_despite_fault = true;
    const PutBlobResult res = build->putBlob(idOf(payload), std::move(source));
    EXPECT_EQ(res.size, payload.size());

    EXPECT_EQ(b->publish_copy_attempts, 1) << "a landed ambiguous copy must be resolved, never re-copied";
    EXPECT_EQ(b->publish_stream_attempts, 0);
    EXPECT_EQ(b->blob_head_attempts, 2);
    const String key = s->layout().blobKey(idOf(payload));
    const auto got = b->get(key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, staging_bytes) << "the destination is the staging object's verbatim copy";
    EXPECT_NE(std::find_if(events.begin(), events.end(),
                           [](const CasEvent & e) { return e.type == CasEventType::BlobReuseAdopt; }),
              events.end()) << "the landed destination must be ADOPTED";
}

/// A server-side copy publication is ambiguous-and-absent: the first copy attempt times out with
/// nothing landing; the resolve observes the destination absent and the copy is REISSUED from the
/// (intact, still-staged) source object — the second attempt commits.
TEST(CASPartWrite, AmbiguousCopyAbsentReattemptsAndCommits)
{
    auto b = std::make_shared<BlobPutFaultBackend>();
    auto s = openBlobFaultPool(b);
    const RootNamespace ns{"srv/tbl"};
    const String payload = "staged-payload-B";
    const String staging_key = "p/staging/test/blob-b";
    const String staging_bytes = String(s->poolMeta().blob_header_len, 'h') + payload;
    ASSERT_EQ(b->putIfAbsent(staging_key, staging_bytes).outcome, PutOutcome::Done);

    auto build = startBuildFor(s, ns, "part_copy_retry");
    const ManifestId id = build->stageManifest({blobManifestEntry("a.bin", payload)});
    build->precommitAdd(ns, "part_copy_retry", id);

    BlobSource source;
    source.size = payload.size();
    source.server_side_copy_from = staging_key;
    source.open = [payload]() -> std::unique_ptr<DB::ReadBuffer>
    {
        return std::make_unique<DB::ReadBufferFromOwnString>(payload);
    };
    b->fault_count = 1;
    const PutBlobResult res = build->putBlob(idOf(payload), std::move(source));
    EXPECT_EQ(res.size, payload.size());

    EXPECT_EQ(b->publish_copy_attempts, 1) << "only the first absent observation may select verbatim copy";
    EXPECT_EQ(b->publish_stream_attempts, 1) << "the absent retry must retag and stream";
    EXPECT_EQ(b->blob_head_attempts, 2);
    const String key = s->layout().blobKey(idOf(payload));
    const auto got = b->get(key);
    ASSERT_TRUE(got.has_value());
    EXPECT_NE(got->bytes, staging_bytes);
    EXPECT_EQ(got->bytes.substr(s->poolMeta().blob_header_len), payload);
}
