#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Interpreters/ContentAddressedLog.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/typeid_cast.h>
#include <mutex>
#include <vector>
using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

/// Round-B opt §6: `reason` is templated rationale (a handful of distinct strings repeated across
/// every row), unlike `object_hash`/`token` which are genuinely per-row varied -- it belongs alongside
/// the log's other LowCardinality columns (event_type/object_kind/outcome), not as a full String.
TEST(CASContentAddressedLog, ReasonColumnIsLowCardinality)
{
    const auto columns = DB::ContentAddressedLogElement::getColumnsDescription();
    const auto & reason_col = columns.get("reason");
    EXPECT_TRUE(typeid_cast<const DB::DataTypeLowCardinality *>(reason_col.type.get()))
        << "reason column must be LowCardinality(String) (Round-B opt §6)";
}
TEST(CASEvent, ConstructAndCopyAndName)
{
    CasEvent e;
    e.type = CasEventType::BlobDelete;
    e.object_kind = CasEventObjectKind::Blob;
    e.object_hash = "abcd";
    e.token = "tok";
    e.round = 7; e.gen = 3;
    e.reason = "in-degree 0 after strip";
    e.detail["freed"] = "10";
    CasEvent c = e;
    EXPECT_EQ(c.type, CasEventType::BlobDelete);
    EXPECT_EQ(c.object_hash, "abcd");
    EXPECT_EQ(c.detail.at("freed"), "10");
    EXPECT_EQ(toString(CasEventType::BlobDelete), "blob_delete");
    EXPECT_EQ(toString(CasEventType::IndegZero), "indegree_zero");
    EXPECT_EQ(toString(CasEventType::GcRecheckVerdict), "gc_recheck_verdict");
    EXPECT_EQ(toString(CasEventObjectKind::Manifest), "manifest");
}

TEST(CASEvent, PoolEmitsToSink)
{
    auto b = std::make_shared<InMemoryBackend>();
    std::vector<CasEvent> seen;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    s->setEventSink([&](const CasEvent & e){ seen.push_back(e); });
    CasEvent e;
    e.type = CasEventType::BlobPut;
    e.object_hash = "h";
    s->emitEvent(std::move(e));
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].type, CasEventType::BlobPut);
    /// null sink => no-op (no crash, no row); a fresh event, not the one already moved above.
    s->setEventSink(nullptr);
    CasEvent e2;
    e2.type = CasEventType::BlobPut;
    s->emitEvent(std::move(e2));
    EXPECT_EQ(seen.size(), 1u);
}

/// Round-B opt §6: `emitEvent` takes the event BY VALUE (moved-through, not `const &`), so a
/// caller's local is genuinely moved-from -- not merely copied via a const reference -- by the time
/// the sink runs. Mirrors `makeCasEventSink`'s own move-out-of-the-by-value-event idiom (a small test
/// double stands in for the `ContentAddressedLogElement` it would normally build).
TEST(CASEvent, EmitEventMovesSourceIntoSink)
{
    auto b = std::make_shared<InMemoryBackend>();
    String captured_reason;
    std::map<String, String> captured_detail;
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    s->setEventSink([&](CasEvent ev)
    {
        captured_reason = std::move(ev.reason);
        captured_detail = std::move(ev.detail);
    });
    CasEvent e;
    e.type = CasEventType::BlobPut;
    e.reason = "sentinel-reason";
    e.detail["k"] = "v";
    s->emitEvent(std::move(e));
    EXPECT_EQ(captured_reason, "sentinel-reason");
    EXPECT_EQ(captured_detail.at("k"), "v");
    /// the source event must be MOVED-FROM after emit, not merely aliased/copied through -- reading
    /// `e` here is the whole point of the test, not an oversight.
    EXPECT_TRUE(e.reason.empty()); // NOLINT(bugprone-use-after-move, hicpp-invalid-access-moved)
    EXPECT_TRUE(e.detail.empty()); // NOLINT(bugprone-use-after-move, hicpp-invalid-access-moved)
}

namespace
{

/// A single-blob part: upload one blob, stage a one-entry manifest naming it, precommit + promote the
/// ref. Returns the blob's object_hash (lowercase hex) so the test can filter the captured rows by it.
String publishOneBlobPart(const PoolPtr & s, const String & ns, const String & ref, const String & payload)
{
    const RootNamespace nsr{ns};
    PartWriteInfo info;
    info.intended_ref = ns + "/" + ref;
    auto build = s->beginPartWrite(info);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();
    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(nsr, ref, id);
    build->promote(nsr, ref, build->buildId(), id);
    /// Phase 3 (mixed-algo pools): every blob-content-hash event render is `blobIdOf(ref)`
    /// ("<algoName>:<hex>"), never a bare hex -- the prime directive that a digest never appears
    /// without its algo.
    return DB::Cas::blobIdOf(e.ref);
}

/// Whether the CURRENT retired list (any gc-shard) still holds an entry (ack-floor pipeline in flight).
bool anyRetiredPending(const PoolPtr & s)
{
    /// Retired-in-snapshot (T4): condemned state rides the adopted fold seal's kCondemned rows, not a
    /// separate retired list — reconstruct the in-flight set from the seal.
    return DB::Cas::tests::anyCondemnedInSeal(s->backend(), s->layout());
}

/// Drive regular GC to a fixpoint over the ACK-FLOOR round (renew the store's mount ack after each round;
/// stay alive while any work counter is nonzero OR an in-flight retired entry remains).
void runGcToFixpoint(const PoolPtr & s, Gc & gc, size_t max_rounds = 64)
{
    for (size_t r = 0; r < max_rounds; ++r)
    {
        const RoundReport rep = DB::Cas::tests::runRegularRoundReclaiming(gc);
        if (!rep.acquired_lease)
            continue;
        s->renewWatermarkOnce();
        const bool no_work = rep.candidates == 0 && rep.deleted == 0 && rep.absent == 0
            && rep.replaced == 0 && rep.spared == 0;
        if (no_work && !anyRetiredPending(s))
            break;
    }
}

bool hasType(const std::vector<CasEvent> & events, CasEventType t)
{
    for (const auto & e : events)
        if (e.type == t)
            return true;
    return false;
}

}

/// B170 Task 4 acceptance: drive a full publish -> drop -> GC-to-delete lifecycle through a capturing
/// sink and assert (a) the taxonomy of events is emitted, (b) EVERY event carries a non-empty reason,
/// (c) filtering by a deleted blob's object_hash reconstructs its edge/retire/delete chain in order.
TEST(CASEvent, LifecycleReconstructionFromRows)
{
    auto b = std::make_shared<InMemoryBackend>();
    /// Declared BEFORE the Pool so they OUTLIVE it: the Pool's background retired-view syncer can emit
    /// (e.g. a view-advance event) right up to the Pool's destructor, and a sink capturing locals that
    /// die first is a use-after-scope (found by ASan 2026-07-09; the production sink captures the Context
    /// shared_ptr by value and is immune).
    std::vector<CasEvent> events;
    std::mutex events_mutex;
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    s->setEventSink([&](const CasEvent & e)
    {
        std::lock_guard lock(events_mutex);
        events.push_back(e);
    });

    const RootNamespace ns{"srv1/tbl"};
    const String ref = "all_0_0_0";
    const String payload = "the-doomed-blob-payload";

    /// publish -> the blob's whole closure is born and a ref names it.
    const String blob_hash = publishOneBlobPart(s, ns.string(), ref, payload);

    /// drop the ref and advance the watermark so the now-unreferenced closure is collectable.
    s->dropRef(ns, ref);
    s->renewWatermarkOnce();

    /// GC reclaims the tree and the blob to a fixpoint.
    Gc gc(s, u128Of("gc-event-log"));
    runGcToFixpoint(s, gc);

    /// The blob must actually be gone (the delete fired).
    ASSERT_FALSE(b->head(s->layout().blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of(payload))})).exists)
        << "GC must have deleted the now-unreferenced blob";

    /// (a) the expected taxonomy was emitted across the lifecycle (manifest model: no standalone trees).
    EXPECT_TRUE(hasType(events, CasEventType::BlobPut));
    EXPECT_TRUE(hasType(events, CasEventType::RootAdd))
        << "a fold must have recorded the manifest owner's blob edge (+1)";
    EXPECT_TRUE(hasType(events, CasEventType::RefDrop));
    EXPECT_TRUE(hasType(events, CasEventType::IndegZero));
    EXPECT_TRUE(hasType(events, CasEventType::GcRetireObserve)
        || hasType(events, CasEventType::GcRetireDecision)
        || hasType(events, CasEventType::GcRecheckVerdict))
        << "a GC retire/recheck transition must be recorded";
    EXPECT_TRUE(hasType(events, CasEventType::BlobDelete) || hasType(events, CasEventType::ManifestDelete))
        << "the single content-delete site must emit a delete row";

    /// (b) completeness mandate: every emitted event has a non-empty reason (the human WHY).
    for (const auto & e : events)
        EXPECT_FALSE(e.reason.empty())
            << "event " << toString(e.type) << " (" << e.object_hash << ") has an empty reason";

    /// (c) lifecycle reconstruction: filtering by the deleted blob's object_hash yields, in time
    /// order, at least its in-degree-zero -> retire-observe -> delete chain — its whole story.
    std::vector<CasEventType> chain;
    for (const auto & e : events)
        if (e.object_hash == blob_hash)
            chain.push_back(e.type);

    ASSERT_FALSE(chain.empty()) << "no rows reference the deleted blob " << blob_hash;

    /// The decisive ordering: the blob's in-degree hit 0 BEFORE GC observed/condemned it, which was
    /// BEFORE it was deleted. Find the first index of each and assert the order.
    auto firstIndexOf = [&](CasEventType t) -> int
    {
        for (size_t i = 0; i < chain.size(); ++i)
            if (chain[i] == t)
                return static_cast<int>(i);
        return -1;
    };
    const int i_indeg = firstIndexOf(CasEventType::IndegZero);
    const int i_observe = firstIndexOf(CasEventType::GcRetireObserve);
    const int i_delete = firstIndexOf(CasEventType::BlobDelete);
    ASSERT_GE(i_indeg, 0) << "the blob's indegree_zero must be in its chain";
    ASSERT_GE(i_observe, 0) << "the blob's gc_retire_observe must be in its chain";
    ASSERT_GE(i_delete, 0) << "the blob's blob_delete must be in its chain";
    EXPECT_LT(i_indeg, i_observe) << "in-degree hit 0 before GC observed it";
    EXPECT_LT(i_observe, i_delete) << "GC observed it before deleting it";
}
