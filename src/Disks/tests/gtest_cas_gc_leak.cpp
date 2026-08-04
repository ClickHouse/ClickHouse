#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/tests/cas_test_helpers.h>

#include <iostream>
#include <string>
#include <vector>

namespace DB::ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int ABORTED;
}

/// NO-LEAK property suite (C++ verification of the R0 INV-NO-LEAK invariant for the root-local
/// part-manifest model). Every dropped/abandoned closure must be FULLY reclaimed: after GC reaches a
/// fixpoint, NO blob or manifest object may remain for the reclaimed part, and the in-degree generation
/// must hold no stranded positive counter for a now-unreferenced blob.
///
/// The model has changed since the tree/snap era: a part is one immutable single-owner `ManifestId`
/// (only blobs stay content-addressed; manifests are NEVER shared across instances — backlog item B7).
/// The leak scenarios below therefore drive the REAL write flow (`stageManifest -> precommitAdd ->
/// putBlob -> promote`) and the real drop/abandon paths, then assert the reclaimed closure leaves no
/// debris. The old "adopt-by-tree relink" leak cases (B7) are REMOVED: there is no shared content id,
/// no subtree placement, `getPartTreeId` returns nullopt and `adoptPart` throws `NOT_IMPLEMENTED`; the
/// byte-stream-fallback relink is an ordinary publish covered by the no-leak displacement repros below.

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;
using DB::Cas::tests::inDegreeOf;
using DB::Cas::tests::publishCommittedTransition;

namespace
{

PoolPtr openTestPool(std::shared_ptr<InMemoryBackend> & out_backend)
{
    out_backend = std::make_shared<InMemoryBackend>();
    return Pool::open(out_backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// Whether the CURRENT retired list (any gc-shard) still holds an entry — the ack-floor deletion pipeline
/// (condemn -> graduate -> delete) is in flight while this is true.
bool anyRetiredPending(const PoolPtr & s)
{
    /// Retired-in-snapshot (T4): condemned state rides the adopted fold seal's kCondemned rows, not a
    /// separate retired list — reconstruct the in-flight set from the seal.
    return DB::Cas::tests::anyCondemnedInSeal(s->backend(), s->layout());
}

/// Drive regular GC to a fixpoint. A condemned blob is not deleted in the round that folds its removal:
/// it condemns, then graduates the round after (round-paced, unconditional), then the NEXT pass deletes
/// it. The loop renews the store's own heartbeat after each round (`renewWatermarkOnce`, unrelated to
/// graduation timing but keeping the build-watermark floor and lease current) and stays alive while ANY
/// work counter is nonzero OR the current retired list still holds an in-flight entry.
size_t runGcToFixpoint(const PoolPtr & s, Gc & gc, size_t max_rounds = 64)
{
    size_t rounds = 0;
    for (; rounds < max_rounds; ++rounds)
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
    return rounds;
}

/// A `ManifestEntry` for a Blob leaf at `path` referencing `payload`'s content hash.
ManifestEntry blobEntry(const String & path, const String & payload)
{
    ManifestEntry e;
    e.path = path;
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();
    return e;
}

/// Publish ONE ref naming a two-blob part through the REAL writer transaction sequence — the exact order
/// the wiring drives (EDGE-BEFORE-OBSERVE): `beginPartWrite -> stageManifest(entries) -> precommitAdd ->
/// putBlob(each body) -> promote`. The durable precommit closure names every blob hash before putBlob
/// makes the first backend observation. Returns the published `ManifestId` so a caller can later HEAD
/// its body / assert reclaim.
ManifestId publishTwoBlobPart(
    const PoolPtr & s, const RootNamespace & ns, const String & ref,
    const String & payload_a, const String & payload_b)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);

    const ManifestId id = build->stageManifest({blobEntry("data.bin", payload_a),
                                                blobEntry("data.cmrk3", payload_b)});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf(payload_a), BlobSource::fromString(payload_a));
    build->putBlob(idOf(payload_b), BlobSource::fromString(payload_b));
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

/// Publish ONE ref naming a single-blob part through the real writer sequence. Returns its ManifestId.
ManifestId publishOneBlobPart(
    const PoolPtr & s, const RootNamespace & ns, const String & ref, const String & payload)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);
    const ManifestId id = build->stageManifest({blobEntry("data.bin", payload)});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

/// The semantic `publishCommittedTransition` wrapper advances its same-life checkpoint. Keep this
/// assertion explicit so this leak fixture cannot silently depend on a historical checkpoint lag.
void assertSemanticTransitionCheckpoint(
    Backend & backend, const Layout & layout, const RootNamespace & ns, const RefTxnId & committed_through)
{
    const auto life = CasRefCatalog::lifeIfCataloged(backend, layout, ns);
    ASSERT_TRUE(life);
    const String key = layout.refCkptKey(*life);
    const auto before = backend.get(key);
    ASSERT_TRUE(before);

    RefCkpt ckpt = decodeRefCkpt(before->bytes);
    ASSERT_TRUE(ckpt.committed_through);
    EXPECT_EQ(*ckpt.committed_through, committed_through);
}

/// Whether a blob's body object is present in the backend (HEADs blobKey directly — the GC retire path
/// HEADs the object key, never the Pool's manifest decode cache).
bool blobPresent(const std::shared_ptr<InMemoryBackend> & b, const Layout & layout, const String & payload)
{
    return b->head(layout.blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of(payload))})).exists;
}

/// Whether a manifest body object is present in the backend.
bool manifestPresent(const std::shared_ptr<InMemoryBackend> & b, const Layout & layout, const ManifestId & id)
{
    return b->head(layout.manifestKey(id)).exists;
}

/// Stage partB's full closure (its two distinct blob bodies + its manifest body) through the REAL
/// writer primitives WITHOUT publishing an owner — `beginPartWrite -> putBlob(each) -> stageManifest`. The
/// bytes are durable in the backend but no journal owner names them yet; the caller installs partB as
/// the new owner via a REPOINT (see displaceAndGc). Returns partB's ManifestId.
ManifestId stagePartBClosure(
    const PoolPtr & s, const RootNamespace & ns, const String & ref,
    const String & payload_a, const String & payload_b)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);
    build->putBlob(idOf(payload_a), BlobSource::fromString(payload_a));
    build->putBlob(idOf(payload_b), BlobSource::fromString(payload_b));
    const ManifestId id = build->stageManifest({blobEntry("data.bin", payload_a),
                                                blobEntry("data.cmrk3", payload_b)});
    /// No precommitAdd / promote: the repoint below installs partB committed in ONE owner-move event.
    return id;
}

/// Reproduce displacement on the SAME (s, ns, ref) and run GC to a fixpoint. partB's distinct blobs
/// displace partA's via a REPOINT of the ref (one RootOwnerEvent old={Committed,ref,partA}/
/// new={Committed,ref,partB}) — the real production shape of last-owner-wins, NOT a body delete.
///
/// Crucially the test does NOT delete partA's manifest body. In the part-manifest model a true removal
/// (the repoint's -1) is derived by GC READING partA's body at removal-fold time; only GC may delete a
/// committed owner's body, and only AFTER the -1 is sealed (recheck cleanup, control #11). So GC folds
/// the repoint: -1 for partA's blobs (body present), +1 for partB's blobs, retires + deletes partA's
/// now-zero-in-degree blobs, and recheck cleanup deletes partA's owner-removed body. Returns the fsck
/// report so the caller can assert the no-leak end state (partA's blobs AND body gone, unreachable==0).
FsckReport displaceAndGc(
    const PoolPtr & s, const std::shared_ptr<InMemoryBackend> & b,
    const RootNamespace & ns, const String & ref, const ManifestId & part_a)
{
    /// Stage partB's full closure (blobs + body present), then repoint the ref from partA to partB.
    const ManifestId part_b = stagePartBClosure(s, ns, ref, "data-B", "mark-B");

    EXPECT_TRUE(b->head(s->layout().manifestKey(part_a)).exists)
        << "partA manifest body must still be present so GC can read its -1 edges at removal-fold";

    /// REPOINT: old={Committed,ref,partA} / new={Committed,ref,partB} in the single ordered journal.
    const uint64_t repoint_sequence = publishCommittedTransition(*b, s->layout(), ns, ref, part_a.ref, part_b.ref);
    assertSemanticTransitionCheckpoint(*b, s->layout(), ns, RefTxnId{1, repoint_sequence});

    /// The repoint dropped partA's owner; advance the watermark floor so partA's now-orphaned blobs are
    /// not spared as in-flight, then run GC to a fixpoint.
    s->renewWatermarkOnce();
    Gc gc(s, hexToU128("00000000000000000000000000000001"));
    runGcToFixpoint(s, gc);
    return runFsck(*s, /*detail=*/false);
}

}

/// NO-LEAK (S1, fold interleaved): partA is published and folded ONCE (its body present, +1 per blob),
/// then partB REPOINTS the ref away from partA (partA's body stays present so GC reads its -1 edges at
/// removal-fold; only GC deletes the owner-removed body, after the -1 is sealed). GC must reclaim partA's
/// blobs to a fixpoint: no blob/manifest object remains for partA and the in-degree generation holds no
/// stranded positive counter for partA's blobs.
TEST(CASGCLeak, DisplacedPartBlobsReclaimedFoldBetween)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"test/tbl"};
    const String ref = "all_0_0_0";

    const ManifestId part_a = publishTwoBlobPart(s, ns, ref, "data-A", "mark-A");

    /// A GC fold runs HERE, before any displacement — partA's body is present, so the fold records +1 for
    /// each of partA's blobs into the durable in-degree generation.
    {
        Gc gc(s, hexToU128("00000000000000000000000000000001"));
        runGcToFixpoint(s, gc);
    }
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("data-A")), 1) << "partA's data blob is pinned (+1)";
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("mark-A")), 1) << "partA's mark blob is pinned (+1)";

    const FsckReport after = displaceAndGc(s, b, ns, ref, part_a);

    EXPECT_EQ(after.dangling, 0u) << "S1 INV-NO-LOSS: displacement must never lose a reachable object";
    EXPECT_GT(after.reachable, 0u) << "S1: the live ref points at partB; partB's closure is reachable";
    EXPECT_EQ(after.unreachable, 0u)
        << "S1 INV-NO-LEAK: an interleaved fold recorded partA's edges; the removal -1 + retire must "
           "reclaim partA's blobs (unreachable=" << after.unreachable << ")";

    /// Backend-level no-debris: partA's blobs and body object are gone; the in-degree counters are 0.
    EXPECT_FALSE(blobPresent(b, s->layout(), "data-A")) << "S1: partA data blob object must be deleted";
    EXPECT_FALSE(blobPresent(b, s->layout(), "mark-A")) << "S1: partA mark blob object must be deleted";
    EXPECT_FALSE(manifestPresent(b, s->layout(), part_a)) << "S1: partA manifest body must be gone";
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("data-A")), 0) << "S1: no stranded positive in-degree";
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("mark-A")), 0) << "S1: no stranded positive in-degree";
}

/// NO-LEAK (S2, NO fold interleaved — the decisive worst case): partA is published, then IMMEDIATELY
/// repointed to partB before ANY GC fold runs. The single fold therefore folds partA's activation (+1)
/// and its removal (-1, read from partA's still-present body) in one pass; the retire reclaims partA's
/// blobs and recheck cleanup deletes partA's owner-removed body. No debris may remain.
TEST(CASGCLeak, DisplacedPartBlobsReclaimedNoFoldBetween)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"test/tbl"};
    const String ref = "all_0_0_0";

    const ManifestId part_a = publishTwoBlobPart(s, ns, ref, "data-A", "mark-A");

    const FsckReport after = displaceAndGc(s, b, ns, ref, part_a);

    EXPECT_EQ(after.dangling, 0u) << "S2 INV-NO-LOSS: displacement must never lose a reachable object";
    EXPECT_GT(after.reachable, 0u) << "S2: the live ref points at partB; partB's closure is reachable";
    EXPECT_EQ(after.unreachable, 0u)
        << "S2 INV-NO-LEAK: partA's blobs must be reclaimed even with no interleaved fold — the recorded "
           "owner edges drive the removal -1 + retire (unreachable=" << after.unreachable << ")";

    EXPECT_FALSE(blobPresent(b, s->layout(), "data-A")) << "S2: partA data blob object must be deleted";
    EXPECT_FALSE(blobPresent(b, s->layout(), "mark-A")) << "S2: partA mark blob object must be deleted";
    EXPECT_FALSE(manifestPresent(b, s->layout(), part_a)) << "S2: partA manifest body must be gone";
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("data-A")), 0) << "S2: no stranded positive in-degree";
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("mark-A")), 0) << "S2: no stranded positive in-degree";
}

/// NO-LEAK (drop): a fully-committed part is published, folded (+1 per blob), then its ref is dropped.
/// GC must reclaim the WHOLE closure — both blobs and the manifest body — leaving no debris and no
/// stranded positive in-degree.
TEST(CASGCLeak, DroppedPartFullyReclaimed)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"test/tbl"};
    const String ref = "all_1_1_0";

    const ManifestId id = publishTwoBlobPart(s, ns, ref, "drop-data", "drop-mark");
    {
        Gc gc(s, hexToU128("00000000000000000000000000000002"));
        runGcToFixpoint(s, gc);
    }
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("drop-data")), 1);
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("drop-mark")), 1);

    s->dropRef(ns, ref);
    s->renewWatermarkOnce();   /// advance the floor so the now-unreferenced closure is not spared

    Gc gc(s, hexToU128("00000000000000000000000000000002"));
    runGcToFixpoint(s, gc);

    const FsckReport after = runFsck(*s, /*detail=*/false);
    EXPECT_EQ(after.dangling, 0u) << "drop INV-NO-LOSS: nothing reachable was lost";
    EXPECT_EQ(after.unreachable, 0u)
        << "drop INV-NO-LEAK: the dropped closure's blobs + body must be fully reclaimed "
           "(unreachable=" << after.unreachable << ")";
    EXPECT_FALSE(blobPresent(b, s->layout(), "drop-data")) << "dropped data blob must be deleted";
    EXPECT_FALSE(blobPresent(b, s->layout(), "drop-mark")) << "dropped mark blob must be deleted";
    EXPECT_FALSE(manifestPresent(b, s->layout(), id)) << "dropped manifest body must be gone";
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("drop-data")), 0) << "no stranded positive in-degree";
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of("drop-mark")), 0) << "no stranded positive in-degree";
}

/// NO-LEAK (resurrect-reupload): a blob incarnation A is published, dropped, and condemned by ONE GC
/// round (retired, NOT yet deleted — it is still mid-pipeline). A fresh build then dedup-hits the SAME
/// content hash: `putBlob` HEADs A, sees it condemned via the per-hash freshness meta point-read, and —
/// per INV-1 (revival-from-source) — re-uploads a DISTINCT incarnation B at the same content-addressed key
/// (fresh `incarnation_tag`, never a GET of the dying object A). B is referenced by a second ref, then
/// that ref is dropped too. GC must fold B's own activation/removal exactly like any other incarnation
/// and reclaim it to a fixpoint: no blob object may remain for the content hash and the in-degree
/// generation must hold no stranded positive counter.
///
/// This reproduces RESURRECT-REUPLOAD-ORPHAN: if GC's bookkeeping keys off the content hash rather than
/// the (hash, token) incarnation identity, it may treat the hash as "already handled" from A's retire
/// cycle and never open a fresh condemn cycle for B once B's in-degree drops to zero — B then orphans
/// forever (unreachable > 0, its body never deleted).
TEST(CASGCLeak, ResurrectReplacedIncarnationReclaimed)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"test/tbl"};
    const String P = "resurrect-payload";

    /// 1. Publish ref r1 -> token A referenced; capture A.
    publishOneBlobPart(s, ns, "r1", P);
    const HeadResult hA = b->head(s->layout().blobKey(idOf(P)));
    ASSERT_TRUE(hA.exists);

    /// 2. Drop r1 -> A dereferenced.
    s->dropRef(ns, "r1");
    s->renewWatermarkOnce();   /// advance the floor so A is not spared as in-flight

    /// 3. ONE GC round: A transitions to in-degree 0 and is condemned (retired), NOT yet deleted.
    Gc gc(s, hexToU128("00000000000000000000000000000004"));
    DB::Cas::tests::runRegularRoundReclaiming(gc);
    {
        const auto lm = DB::Cas::tests::loadMetaForTest(*b, s->layout(), u128Of(P));
        ASSERT_TRUE(lm.has_value() && lm->meta.state == MetaState::Condemned)
            << "precondition: token A must be condemned before the resurrect";
    }
    ASSERT_TRUE(blobPresent(b, s->layout(), P)) << "A not yet deleted (still in the pipeline)";

    /// 4. RESURRECT: a fresh build dedup-hits P; putBlob sees A condemned -> re-uploads a DISTINCT
    /// incarnation B at the same content-addressed key (INV-1 revival-from-source).
    publishOneBlobPart(s, ns, "r2", P);
    const HeadResult hB = b->head(s->layout().blobKey(idOf(P)));
    ASSERT_TRUE(hB.exists);
    ASSERT_NE(hB.token.value, hA.token.value) << "resurrect must mint a new incarnation token B";

    /// 5. Drop r2 -> B dereferenced.
    s->dropRef(ns, "r2");
    s->renewWatermarkOnce();

    /// 6. Run GC to fixpoint. The replaced incarnation B MUST be reclaimed.
    runGcToFixpoint(s, gc);

    const FsckReport after = runFsck(*s, /*detail=*/false);
    EXPECT_EQ(after.dangling, 0u) << "resurrect INV-NO-LOSS: nothing reachable was lost";
    EXPECT_EQ(after.unreachable, 0u)
        << "resurrect INV-NO-LEAK: the resurrect-replaced incarnation B must not orphan "
           "(unreachable=" << after.unreachable << ")";
    EXPECT_FALSE(blobPresent(b, s->layout(), P)) << "B's object must be deleted";
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of(P)), 0) << "no stranded positive in-degree";
}

/// IDEMPOTENCY of the RESURRECT-REUPLOAD-ORPHAN fold: drives the exact same condemn-A / resurrect-B /
/// drop-B / reclaim sequence as `ResurrectReplacedIncarnationReclaimed` above, then keeps running the
/// regular round PAST the fixpoint. The re-condemn that reclaims the resurrect-replaced incarnation B
/// must fire exactly once: extra rounds on an already-reclaimed content hash must be no-ops (no
/// re-condemn churn, no duplicate retired entry) and must never manufacture fresh fsck debris.
TEST(CASGCLeak, ResurrectReplacedReclaimIsIdempotent)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"test/tbl"};
    const String P = "resurrect-payload-idem";

    /// 1. Publish ref r1 -> token A referenced, then drop it.
    publishOneBlobPart(s, ns, "r1", P);
    s->dropRef(ns, "r1");
    s->renewWatermarkOnce();   /// advance the floor so A is not spared as in-flight

    /// 2. ONE GC round: A transitions to in-degree 0 and is condemned (retired), NOT yet deleted.
    Gc gc(s, hexToU128("00000000000000000000000000000005"));
    DB::Cas::tests::runRegularRoundReclaiming(gc);

    /// 3. RESURRECT: r2 dedup-hits P while A is condemned -> mints a fresh incarnation B.
    publishOneBlobPart(s, ns, "r2", P);
    s->dropRef(ns, "r2");
    s->renewWatermarkOnce();

    /// 4. Reclaim B to a fixpoint (the RESURRECT-REUPLOAD-ORPHAN fold under test).
    runGcToFixpoint(s, gc);
    ASSERT_FALSE(blobPresent(b, s->layout(), P)) << "B must be reclaimed before the idempotency check";

    /// 5. Extra rounds past the fixpoint: nothing is left to do for this hash. The fold must not
    /// re-condemn it (that would be the churn/duplicate-entry bug) and must not resurrect any debris.
    for (int round = 0; round < 3; ++round)
    {
        const RoundReport r = DB::Cas::tests::runRegularRoundReclaiming(gc);
        if (r.acquired_lease)
            EXPECT_EQ(r.condemned, 0u) << "no re-condemn of an already-reclaimed hash on extra round " << round;
        s->renewWatermarkOnce();
    }

    EXPECT_FALSE(blobPresent(b, s->layout(), P)) << "stays deleted across extra rounds";
    EXPECT_EQ(inDegreeOf(*b, s->layout(), u128Of(P)), 0) << "no stranded positive in-degree";

    const FsckReport after = runFsck(*s, /*detail=*/false);
    EXPECT_EQ(after.unreachable, 0u) << "re-condemn churn must not manufacture a fresh unreachable object";
    EXPECT_EQ(after.dangling, 0u) << "idempotent extra rounds must never lose a reachable object";
}

/// WRITER-SIDE half of the RESURRECT-REUPLOAD-ORPHAN fold: after the round that folds the resurrect-
/// replaced incarnation B's dereference re-condemns B, a fresh writer dedup-hitting the SAME content hash
/// must see B as condemned via the per-hash freshness meta point-read — never as an adoptable live token.
/// If GC's bookkeeping instead kept treating B as adopt-eligible (the pre-fix bug), a concurrent writer's
/// `putBlob` would adopt the being-reclaimed B rather than resurrect a fresh incarnation, racing the
/// delete pipeline.
///
/// Depending on round timing, by the time the meta is checked B may be (a) still present and visibly
/// condemned, or (b) already physically deleted by the delete pipeline (meta dropped alongside it) — BOTH
/// outcomes prove B is never adoptable. The assertion only fails on the pre-fix shape: B present and NOT
/// condemned.
TEST(CASGCLeak, ResurrectReplacedTokenIsCondemnedInMeta)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"test/tbl"};
    Gc gc(s, hexToU128("00000000000000000000000000000006"));
    const String P = "resurrect-payload-view";

    /// 1. Publish ref r1 -> token A referenced; capture A, then drop it and condemn via ONE GC round.
    publishOneBlobPart(s, ns, "r1", P);
    const HeadResult hA = b->head(s->layout().blobKey(idOf(P)));
    ASSERT_TRUE(hA.exists);
    s->dropRef(ns, "r1");
    s->renewWatermarkOnce();   /// advance the floor so A is not spared as in-flight
    gc.runRegularRound();

    /// 2. RESURRECT: r2 dedup-hits P while A is condemned -> mints a fresh incarnation B.
    publishOneBlobPart(s, ns, "r2", P);
    const HeadResult hB = b->head(s->layout().blobKey(idOf(P)));
    ASSERT_TRUE(hB.exists);
    ASSERT_NE(hB.token.value, hA.token.value) << "resurrect must mint a distinct incarnation";
    s->dropRef(ns, "r2");
    s->renewWatermarkOnce();

    /// 3. The round that folds B's dereference re-condemns token B.
    gc.runRegularRound();

    const auto lm = DB::Cas::tests::loadMetaForTest(*b, s->layout(), u128Of(P));
    EXPECT_TRUE((lm.has_value() && lm->meta.state == MetaState::Condemned) || !blobPresent(b, s->layout(), P))
        << "the replaced incarnation B must be visible as condemned (or already reclaimed) so a "
           "dedup-hitting writer resurrects, not adopts";
}

/// (The NO-LEAK-on-abandon test `CASGCLeak.AbandonedPrecommitReclaimsOwnBlobs` was removed with the
/// snapshot+log ref model: it asserted GC AUTOMATICALLY reclaims a crashed build's abandoned precommit and
/// collects its own unique blob. Per spec §Responsibility Boundary that reclaim is now the WRITER's job
/// (it appends the exact `owner_transition` removal on recovery); GC never scans for or removes precommit
/// bindings. The writer-side abandon/recovery cleanup is exercised by the writer tests.)

/// REUSE-vs-GC race (no-LOSS half of the no-leak family): a build ADOPTS a committed blob B by tokenless
/// evidence (B present, not yet condemned), the committed ref pinning B is DROPPED, GC retires+deletes B
/// AND completes the round, and only THEN does the build try to publish a manifest naming B.
///
/// The promote gate re-observes the loss (it re-HEADs every blob leaf and fails closed on a deleted dep,
/// throwing a retryable ABORTED) — it must NEVER silently commit a dangling ref. The assertion is the
/// no-LOSS guarantee: `dangling==0`. (A tokenless adopt has no body to re-upload, so a real caller would
/// re-derive B from source on retry; here we only confirm the gate fails closed.)
TEST(CASReuseGcRace, ReuseOfBlobDeletedBeforePublish)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"test/tbl"};
    const String B = "shared-blob-payload";
    const String U = "build2-unique-blob";

    /// build1: commit part_1 -> manifest -> blob B.
    publishOneBlobPart(s, ns, "part_1", B);

    /// build2: adopt B by tokenless evidence (no HEAD) and upload its OWN unique blob U. It does NOT yet
    /// stage a manifest or precommit — the scenario is that GC deletes B BEFORE build2 publishes a manifest
    /// naming it. (Staging+precommitting BEFORE the drop would make the precommit's activating +1 PIN B —
    /// B would never reach in-degree 0 and GC could not delete it, so the race could not be reproduced.)
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/part_2";
    auto build2 = s->beginPartWrite(info);

    ManifestEntry eb;
    eb.path = "data.bin";
    eb.placement = EntryPlacement::Blob;
    eb.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(B))};

    eb.blob_size = B.size();
    build2->adoptEvidence(eb);                                   /// tokenless dep (no HEAD)
    build2->putBlob(idOf(U), BlobSource::fromString(U));         /// build2's own unique, protected blob

    /// Drop the committed pin on B and advance the watermark so B (owned by the finished build1) is not
    /// spared. No owner names B now (build2 has not staged/precommitted), so GC folds B to in-degree 0
    /// once part_1 is dropped.
    s->dropRef(ns, "part_1");
    s->renewWatermarkOnce();

    /// GC reclaims build1's manifest and the now-unreferenced B to a fixpoint, completing the rounds.
    {
        Gc gc(s, u128Of("gc-reuse-race"));
        runGcToFixpoint(s, gc);
    }
    ASSERT_FALSE(blobPresent(b, s->layout(), B))
        << "GC must have deleted the now-unreferenced reused blob B";

    /// Only NOW does build2 publish a manifest naming the (just-deleted) B: stage the body + precommit.
    const ManifestId id2 = build2->stageManifest({eb, blobEntry("uniq.bin", U)});
    build2->precommitAdd(ns, "part_2", id2);

    /// build2 promotes part_2 -> id2 -> {B, U}. §4 manifest-trust: B is a committed-source adopted leaf,
    /// so the promote gate TRUSTS it (no HEAD/loadMeta probe) and commits — it does NOT re-observe the
    /// deleted B. This is the accepted D4 trade-off. On the real reuse/relink path B CANNOT be deleted
    /// while build2's precommit edge is live: precommitAdd durably appends the Precommit OwnerTransition
    /// (CasPartWriteTxn.cpp precommitAdd) BEFORE promote, and promote re-proves that edge live (WPromote
    /// owner==bld) BEFORE it trusts the leaf — so B has in-degree >= 1 and GC (the sole deleter) cannot
    /// collect it. This test injects the loss DIRECTLY (raw GC-to-fixpoint after dropping EVERY owner,
    /// with build2 not yet precommitted), which the live-precommit invariant excludes. So the dangle is
    /// not prevented at promote under §4 — it is DETECTED by fsck (the backstop).
    EXPECT_NO_THROW(build2->promote(ns, "part_2", build2->buildId(), id2));

    /// THE BACKSTOP (INV-NO-DANGLE-via-fsck): fsck's reachable-but-absent scan reports the committed-yet-
    /// deleted B as dangling. This is where an absent adopted blob surfaces under §4 — not at the promote
    /// gate. Detection moved, it did not disappear.
    const FsckReport rep = runFsck(*s, /*detail=*/true);
    EXPECT_GE(rep.dangling, 1u)
        << "§4 D4 backstop: promote trusts the adopted leaf and commits; the deleted B must surface as an "
           "fsck dangling finding (dangling=" << rep.dangling << ", reachable=" << rep.reachable << ")";
}
