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

/// The dangle is about the SINGLE snap shard's in-degree, and one cursor_key covers both refs.
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
            /// The fail-closed coherence guard refused this round (CORRUPTED_DATA): no delete
            /// happened, the live blob is safe. Stop — re-running would just throw again.
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

}

/// B140-DANGLE — the soak's INV-NO-LOSS finding, ported to the root-local part-manifest model.
///
/// THE PROPERTY (unchanged across the redesign): a content-shared / deduplicated blob `B` referenced
/// by TWO live parts must NEVER be deleted when only ONE of those refs is dropped. In the old tree
/// model the loss arose from a `GcSnap` cursor-skip under-count (the committed `folded_cursor` ran
/// ahead of the snap's edges, so the second live part's edge was never folded). That white-box
/// failure mode is structurally IMPOSSIBLE in the manifest model: there is no separate snap; per-blob
/// in-degree is derived by folding the ONE ordered `RootOwnerEvent` journal, the fold cursor lives in
/// the `CasFoldSeal` (one durable unit with the sealed deltas, never diverging), and each part's blob
/// edges come from reading its OWN manifest body at fold time. So this is now a black-box no-loss
/// oracle: two live refs share `B`, drop one, GC to a fixpoint, assert `B` survives (`dangling == 0`)
/// because the surviving ref's manifest still contributes its +1 edge — `B`'s in-degree never reaches 0.
TEST(CASGCDangle, SharedBlobSurvivesDropOfOneOfTwoLiveRefs)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"srv1/tbl"};

    /// rb_live -> manifest { data.bin: B }. B is uploaded here.
    {
        PartWriteInfo info;
        info.intended_ref = ns.string() + "/rb_live";
        auto build = s->beginPartWrite(info);
        build->putBlob(idOf("B"), BlobSource::fromString("B"));
        ManifestEntry e;
        e.path = "data.bin";
        e.placement = EntryPlacement::Blob;
        e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of("B"))};

        e.blob_size = std::string("B").size();
        const ManifestId id = build->stageManifest({e});
        build->precommitAdd(ns, "rb_live", id);
        build->promote(ns, "rb_live", build->buildId(), id);
        s->renewWatermarkOnce();
    }

    /// rb_cur -> a DISTINCT manifest { other.bin: B } that REUSES the same shared blob B (tokenless
    /// adopt — the soak's cross-node `adopt`). Still live.
    {
        PartWriteInfo info;
        info.intended_ref = ns.string() + "/rb_cur";
        auto build = s->beginPartWrite(info);
        ManifestEntry e;
        e.path = "other.bin";
        e.placement = EntryPlacement::Blob;
        e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of("B"))};

        e.blob_size = std::string("B").size();
        build->adoptEvidence(e);   /// tokenless dep (no HEAD) — the cross-node adopt
        const ManifestId id = build->stageManifest({e});
        build->precommitAdd(ns, "rb_cur", id);
        build->promote(ns, "rb_cur", build->buildId(), id);
        s->renewWatermarkOnce();
    }

    /// Drop rb_live: its manifest's -1 on B lands, but rb_cur's manifest still contributes +1, so B's
    /// in-degree stays >= 1 and B is never a zero-in-degree candidate.
    s->dropRef(ns, "rb_live");
    s->renewWatermarkOnce();

    Gc gc(s, hexToU128("00000000000000000000000000000001"));
    const size_t rounds = runGcToFixpoint(gc);

    /// rb_cur is still LIVE and still resolves through a present manifest — its blob B must survive.
    ASSERT_TRUE(s->resolveRef(ns, "rb_cur").has_value());

    const FsckReport rep = runFsck(*s, /*detail=*/true);

    /// THE DANGLE ASSERTION: GC must NEVER delete a blob a live ref references.
    EXPECT_EQ(rep.dangling, 0u)
        << "B140-dangle: GC deleted shared blob B still referenced by the live ref rb_cur "
        << "after " << rounds << " rounds (dangling=" << rep.dangling << ", reachable=" << rep.reachable
        << ", B_present=" << b->head(s->layout().blobKey(idOf("B"))).exists << ").";
    EXPECT_TRUE(b->head(s->layout().blobKey(idOf("B"))).exists)
        << "shared blob B must remain present while rb_cur references it";
}
