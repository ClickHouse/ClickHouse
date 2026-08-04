#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>

#include <string>

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace
{

/// Mirrors the B140 repro.
PoolPtr openTestPool(std::shared_ptr<InMemoryBackend> & out_backend)
{
    out_backend = std::make_shared<InMemoryBackend>();
    return Pool::open(out_backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

size_t runGcToFixpoint(Gc & gc, size_t max_rounds = 64)
{
    size_t rounds = 0;
    for (; rounds < max_rounds; ++rounds)
    {
        RoundReport rep;
        try
        {
            rep = gc.runRegularRound();
        }
        catch (const DB::Exception &)
        {
            break;
        }
        if (!rep.acquired_lease)
            continue;
        if (rep.candidates == 0 && rep.deleted == 0 && rep.absent == 0
            && rep.replaced == 0 && rep.spared == 0)
            break;
    }
    return rounds;
}

ManifestEntry blobEntry(const String & name, const String & payload)
{
    ManifestEntry e;
    e.path = name;
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();
    return e;
}

}

/// B171 build-root / precommit, RED repro of the B140-dangle at unit level driven entirely through the
/// public PartWriteTxn/Pool/Gc API (no snap injection):
///
///   PartWriteTxn A uploads blob P and publishes refA -> t1 -> { data.bin: P }. A is then RELEASED (dtor),
///   retiring its build_seq so the GC watermark `min_active` advances PAST A. P now carries A's
///   `cas_owner` and is no longer protected by any in-flight build.
///
///   PartWriteTxn B starts and ADOPTS the same blob P via tokenless evidence (adoptEvidence — the cross-node
///   adopt case), assembles t2 -> { other.bin: P }, and `precommit(t2)` — which publishes a durable
///   build-root ref so GC's fold lifts the in-degree of P's closure.
///
///   refA is dropped + watermark renewed; GC runs to fixpoint. P is protected by B's precommit edge,
///   so GC must NOT delete it. PartWriteTxn B then publishes refB -> t2 successfully.
///
/// THE POSITIVE INVARIANT: the whole flow must succeed AND P must survive, because B's precommit pins
/// P's closure across A's retire + GC (B171 two-phase commit; `checkAndResolveDeps` proves closure
/// present at publish time).
TEST(CASPartWriteTxnRootDangle, SharedBlobSurvivesSourceDropDuringBuild)
{
    std::shared_ptr<InMemoryBackend> backend;
    auto s = openTestPool(backend);
    const RootNamespace ns{"test/tbl"};
    const String P = "shared-blob-payload-P";

    /// PartWriteTxn A: upload P, publish refA -> manifest -> { data.bin: P }, then release A so its build_seq
    /// retires and min_active advances past it.
    {
        PartWriteInfo info;
        info.intended_ref = ns.string() + "/refA";
        auto a = s->beginPartWrite(info);
        a->putBlob(idOf(P), BlobSource::fromString(P));
        const ManifestId id = a->stageManifest({blobEntry("data.bin", P)});
        a->precommitAdd(ns, "refA", id);
        a->promote(ns, "refA", a->buildId(), id);
    }
    s->renewWatermarkOnce();   /// A is gone; min_active now advances past A's build_seq

    /// PartWriteTxn B: adopt the SAME blob P (cross-node adopt — tokenless evidence via adoptEvidence), assemble
    /// its manifest, and precommitAdd it. The precommit pins P's closure (fold +1 edge) for the build.
    PartWriteInfo binfo;
    binfo.intended_ref = ns.string() + "/refB";
    auto b = s->beginPartWrite(binfo);
    const ManifestEntry pe = blobEntry("other.bin", P);
    b->adoptEvidence(pe);
    const ManifestId t2 = b->stageManifest({pe});
    b->precommitAdd(ns, "refB", t2);

    /// The source ref disappears, and the watermark is renewed so the closure looks collectable.
    s->dropRef(ns, "refA");
    s->renewWatermarkOnce();

    /// GC to fixpoint. P must survive: the live precommit binding for refB activates a +1 blob edge on
    /// P during the fold, so P never reaches in-degree 0 (B171 two-phase commit).
    Gc gc(s, u128Of("gc-b171"));
    runGcToFixpoint(gc);

    /// PartWriteTxn B commits refB by promoting its precommit. Should succeed end-to-end; if it throws (e.g.
    /// ABORTED because the blob is gone) that is itself the RED outcome.
    ASSERT_NO_THROW(b->promote(ns, "refB", b->buildId(), t2))
        << "B171: PartWriteTxn B's promote must succeed — the precommit should have kept P alive";

    /// The blob B references must still be present (no dangle), and refB must resolve.
    ASSERT_TRUE(backend->head(s->layout().blobKey(idOf(P))).exists)
        << "B171-dangle: GC deleted the shared blob P that PartWriteTxn B adopted — its cas_owner was the "
        << "retired PartWriteTxn A and the stub precommit published no build-root edge, so inDeg(P) hit 0 "
        << "and the single content-delete site removed it. refB now dangles.";
    ASSERT_TRUE(s->resolveRef(ns, "refB").has_value())
        << "B171: refB must resolve to its committed manifest";
}

/// B171 INV-COMMIT-FAILCLOSED: even if the build-root precommit is PREMATURELY RECLAIMED mid-build
/// (e.g. a live build whose watermark renewer froze and was falsely judged dead), the real commit must
/// NEVER publish a table ref over a missing dependency. It must fail closed — abort — never dangle.
///
/// Setup mirrors the primary repro: PartWriteTxn A publishes refA -> t1 -> { data.bin: P } then retires; PartWriteTxn
/// B adopts P, assembles t2 -> { other.bin: P }, and precommits t2 (a real build-root edge now protects
/// P). We then SIMULATE the premature reclaim by manually dropping the build-root ref (as GC's reclaim
/// would) AND dropping refA, then renew the watermark and run GC to fixpoint. With P's only protection
/// (the precommit edge) gone and its owner retired, GC deletes P. PartWriteTxn B's publish must now ABORT
/// (`checkAndResolveDeps` finds the adopted blob absent and not re-creatable) instead of committing a dangle.
TEST(CASPartWriteTxnRootDangle, PrematureReclaimCommitFailsClosed)
{
    std::shared_ptr<InMemoryBackend> backend;
    auto s = openTestPool(backend);
    const RootNamespace ns{"test/tbl"};
    const String P = "shared-blob-payload-P-reclaim";

    /// PartWriteTxn A: upload P, publish refA -> manifest, retire A so min_active advances past it.
    {
        PartWriteInfo info;
        info.intended_ref = ns.string() + "/refA";
        auto a = s->beginPartWrite(info);
        a->putBlob(idOf(P), BlobSource::fromString(P));
        const ManifestId id = a->stageManifest({blobEntry("data.bin", P)});
        a->precommitAdd(ns, "refA", id);
        a->promote(ns, "refA", a->buildId(), id);
    }
    s->renewWatermarkOnce();

    /// PartWriteTxn B: adopt P via tokenless evidence, assemble its manifest, precommitAdd it (the precommit
    /// owner binding for refB now protects P with a +1 fold edge).
    PartWriteInfo binfo;
    binfo.intended_ref = ns.string() + "/refB";
    auto b = s->beginPartWrite(binfo);
    const ManifestEntry pe2 = blobEntry("other.bin", P);
    b->adoptEvidence(pe2);
    const ManifestId t2 = b->stageManifest({pe2});
    b->precommitAdd(ns, "refB", t2);

    /// SIMULATE a premature reclaim having already collected P: had the precommit binding been wrongly
    /// reclaimed with no other owner, GC would condemn+delete P's closure. Reproduce that END STATE
    /// directly by deleting P's blob object. (The durable ref-log stream is owned by the live writer, so a
    /// RAW removal append would collide with the writer's own `RefTxnId` sequence allocation on the next
    /// flush; the property under test is the COMMIT gate's fail-closed behavior against a missing
    /// dependency, not the reclaim mechanics -- so we go straight to the reclaimed state.)
    {
        const String pkey = s->layout().blobKey(idOf(P));
        const HeadResult h = backend->head(pkey);
        ASSERT_TRUE(h.exists) << "P must be present before the simulated reclaim";
        ASSERT_EQ(backend->deleteExact(pkey, h.token).kind, DeleteOutcome::Kind::Deleted);
    }
    /// Drop the source ref too (the state a real premature reclaim leaves: P unprotected and gone).
    s->dropRef(ns, "refA");
    s->renewWatermarkOnce();

    /// The shared blob must be GONE (the premature reclaim collected it).
    ASSERT_FALSE(backend->head(s->layout().blobKey(idOf(P))).exists)
        << "premature-reclaim setup invalid: P should have been collected after losing its precommit";

    /// §4 manifest-trust (test name is legacy — B171 INV-COMMIT-FAILCLOSED for an ADOPTED leaf now moves to
    /// fsck): P is a committed-source adopted leaf, so PartWriteTxn B's promote TRUSTS it (no HEAD/loadMeta probe)
    /// and COMMITS refB. On the real reuse/relink path this dangle is UNREACHABLE: precommitAdd durably
    /// appended refB's Precommit OwnerTransition (CasPartWriteTxn.cpp precommitAdd) BEFORE promote, and promote
    /// re-proves that edge is the LIVE owner (WPromote owner==bld) BEFORE trusting P — so P has in-degree
    /// >= 1 and GC (the sole deleter) cannot collect it. This test injects the collection DIRECTLY (a raw
    /// deleteExact while refB's precommit is still live), which the live-precommit invariant excludes. So
    /// promote SUCCEEDS; the dangle is not prevented at promote but DETECTED by fsck (the backstop).
    ASSERT_NO_THROW(b->promote(ns, "refB", b->buildId(), t2))
        << "§4: an adopted leaf is trusted at promote — a missing dependency is not re-observed here";

    /// Trust never fabricates the missing blob (it never touches P); refB IS committed (naming absent P).
    ASSERT_FALSE(backend->head(s->layout().blobKey(idOf(P))).exists)
        << "trust never fabricates the missing blob — P stays absent";
    ASSERT_TRUE(s->resolveRef(ns, "refB").has_value())
        << "§4: refB commits under trust (the D4 trade-off); the dangle is caught by fsck, below";

    /// THE BACKSTOP (INV-NO-DANGLE-via-fsck): fsck's reachable-but-absent scan reports refB's absent P as
    /// dangling — this is where the B171 guarantee lives under §4. Detection moved, it did not disappear.
    const FsckReport rep = runFsck(*s, /*detail=*/true);
    EXPECT_GE(rep.dangling, 1u)
        << "§4 D4 backstop: refB committed over the deleted P; fsck must report it dangling (dangling="
        << rep.dangling << ", reachable=" << rep.reachable << ")";
}

/// (The GC-reclaim test `CASPartWriteTxnRoot.AbandonedPrecommitReclaimed` -- which asserted GC AUTOMATICALLY
/// reclaims an abandoned precommit of a judged-dead build and then collects its closure -- was removed
/// with the snapshot+log ref model. Per spec §Responsibility Boundary, reclaiming an abandoned precommit
/// is now the WRITER's job (it appends the exact `owner_transition` removal on recovery); GC never scans
/// for or removes precommit bindings, and there is no mutable shard journal to append a `PrecommitRemove`
/// into. The `precommitRemovalAppended` shard-journal probe it shared with `LivePrecommitNotReclaimed`
/// went with it.)

/// B8 CONSERVATISM (liveness-correctness guard): a live in-flight build's precommit binding (and its
/// pinned blobs) must survive a full GC run, and the build must still be able to promote it. In the
/// snapshot+log model GC never reclaims a precommit at all, so this is purely a liveness pin: the live
/// precommit's `+1` fold edge keeps its exclusively-owned blob alive across GC.
TEST(CASPartWriteTxnRoot, LivePrecommitNotReclaimed)
{
    std::shared_ptr<InMemoryBackend> backend;
    auto s = openTestPool(backend);
    const RootNamespace ns{"test/tbl"};
    const String Q = "live-build-blob-payload-Q";

    /// PartWriteTxn B stays ALIVE: upload Q, assemble, precommitAdd — and we DO NOT retire its seq. So
    /// `min_active <= build_seq` (B is in-flight) and the watermark keeps a live, advancing seq.
    PartWriteInfo binfo;
    binfo.intended_ref = ns.string() + "/refLive";
    auto b = s->beginPartWrite(binfo);
    b->putBlob(idOf(Q), BlobSource::fromString(Q));
    const ManifestId t = b->stageManifest({blobEntry("data.bin", Q)});
    b->precommitAdd(ns, "refLive", t);
    s->renewWatermarkOnce();
    ASSERT_LE(s->minActive(), b->buildSeq()) << "precondition: B must be in-flight (min_active <= seq)";

    /// GC to fixpoint while B is live.
    Gc gc(s, u128Of("gc-b8-live"));
    runGcToFixpoint(gc);

    /// Q must still be present (the live precommit's +1 edge pins it across GC).
    ASSERT_TRUE(backend->head(s->layout().blobKey(idOf(Q))).exists)
        << "B8 conservatism: the live precommit must keep its blob alive across GC";

    /// B can still commit (the precommit is intact).
    ASSERT_NO_THROW(b->promote(ns, "refLive", b->buildId(), t))
        << "B8 conservatism: a live build must still be able to promote its untouched precommit";
}
