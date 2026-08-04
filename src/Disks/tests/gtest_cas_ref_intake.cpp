#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Common/Exception.h>

using namespace DB::Cas;

namespace
{

ManifestRef mr(uint64_t epoch, uint64_t seq, uint32_t ordinal = 1)
{
    return ManifestRef{epoch, seq, ordinal};
}

RefTxnId rid(uint64_t epoch, uint64_t seq)
{
    return RefTxnId{epoch, seq};
}

RefOp addOwner(RefOwnerKind kind, const String & ref, const ManifestRef & manifest)
{
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.new_binding = RefOwnerBinding{kind, ref, manifest};
    return op;
}

RefOp removeOwner(RefOwnerKind kind, const String & ref, const ManifestRef & manifest)
{
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{kind, ref, manifest};
    return op;
}

RefOp promote(const String & ref, const ManifestRef & manifest)
{
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref, manifest};
    op.new_binding = RefOwnerBinding{RefOwnerKind::Committed, ref, manifest};
    return op;
}

/// A raw `owner_transition` op from explicit optional bindings, bypassing every shape-builder above --
/// used by the rejection tests to construct shapes `classifyOwnerTransitionShape` does not recognize.
RefOp rawOwnerTransition(std::optional<RefOwnerBinding> old_binding, std::optional<RefOwnerBinding> new_binding)
{
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = std::move(old_binding);
    op.new_binding = std::move(new_binding);
    return op;
}

RefLogTxn txn(const String & ns, RefTxnId id, std::vector<RefOp> ops)
{
    RefLogTxn t;
    t.ns = ns;
    t.txn_id = id;
    t.ops = std::move(ops);
    return t;
}

}

/// spec §gc-step-produce-manifest-edge-delta: each explicit operation states its own edge change.
TEST(CASRefIntake, ManifestEdgesPerOperationShape)
{
    /// Add precommit => one +1.
    {
        const auto edges = manifestEdgesOfTxn(txn("db/t", rid(1, 1), {addOwner(RefOwnerKind::Precommit, "p", mr(1, 5))}));
        ASSERT_EQ(edges.size(), 1u);
        EXPECT_EQ(edges[0].change, 1);
        EXPECT_EQ(edges[0].manifest_id, (ManifestId{RootNamespace{"db/t"}, mr(1, 5)}));
        EXPECT_EQ(edges[0].op_ordinal, 0u);
        EXPECT_EQ(edges[0].edge_ordinal, 1u);
    }
    /// Remove committed => one -1.
    {
        const auto edges = manifestEdgesOfTxn(txn("db/t", rid(1, 2), {removeOwner(RefOwnerKind::Committed, "p", mr(1, 5))}));
        ASSERT_EQ(edges.size(), 1u);
        EXPECT_EQ(edges[0].change, -1);
        EXPECT_EQ(edges[0].manifest_id, (ManifestId{RootNamespace{"db/t"}, mr(1, 5)}));
    }
    /// Remove precommit => one -1 (the fourth classified shape, distinct from remove committed only by
    /// `old_binding.kind`).
    {
        const auto edges = manifestEdgesOfTxn(txn("db/t", rid(1, 25), {removeOwner(RefOwnerKind::Precommit, "p", mr(1, 5))}));
        ASSERT_EQ(edges.size(), 1u);
        EXPECT_EQ(edges[0].change, -1);
        EXPECT_EQ(edges[0].owner_kind, RefOwnerKind::Precommit);
        EXPECT_EQ(edges[0].manifest_id, (ManifestId{RootNamespace{"db/t"}, mr(1, 5)}));
    }
    /// Promote same manifest => no net edge (spec §Promote).
    {
        const auto edges = manifestEdgesOfTxn(txn("db/t", rid(1, 3), {promote("p", mr(1, 5))}));
        EXPECT_TRUE(edges.empty());
    }
    /// set_published_at / namespace_birth / remove_namespace => no edge.
    {
        RefOp set_published_at;
        set_published_at.kind = RefOpKind::SetPublishedAt;
        set_published_at.ref_name = "p";
        set_published_at.expected_manifest_ref = mr(1, 5);
        EXPECT_TRUE(manifestEdgesOfTxn(txn("db/t", rid(1, 4), {set_published_at})).empty());

        RefOp birth;
        birth.kind = RefOpKind::NamespaceBirth;
        EXPECT_TRUE(manifestEdgesOfTxn(txn("db/t", rid(1, 5), {birth})).empty());
    }
    /// Replace one manifest by a different one (two explicit ops) => -1 old, +1 new.
    {
        const auto edges = manifestEdgesOfTxn(txn("db/t", rid(1, 6),
            {removeOwner(RefOwnerKind::Committed, "p", mr(1, 5)), addOwner(RefOwnerKind::Precommit, "p", mr(1, 6))}));
        ASSERT_EQ(edges.size(), 2u);
        EXPECT_EQ(edges[0].change, -1);
        EXPECT_EQ(edges[0].manifest_id.ref, mr(1, 5));
        EXPECT_EQ(edges[1].change, 1);
        EXPECT_EQ(edges[1].manifest_id.ref, mr(1, 6));
    }
}

/// `manifestEdgesOfTxn` rejects every `owner_transition` shape outside the four `classifyOwnerTransitionShape`
/// recognizes (Pool/CasRefProtocol.cpp) -- it must never silently assign edge meaning to a shape the
/// writer/replay state machine would refuse to apply. Each case throws `CORRUPTED_DATA`.
TEST(CASRefIntake, ManifestEdgesRejectsUnrecognizedShapes)
{
    /// Neither binding: a degenerate owner_transition that names no owner change at all.
    EXPECT_THROW(manifestEdgesOfTxn(txn("db/t", rid(1, 1), {rawOwnerTransition(std::nullopt, std::nullopt)})),
                 DB::Exception);

    /// old+new naming DIFFERENT manifests in ONE op (the never-legal "replace" shape; an atomic
    /// manifest replace is always two ops -- an explicit removal then a same-manifest promote).
    EXPECT_THROW(manifestEdgesOfTxn(txn("db/t", rid(1, 2),
        {rawOwnerTransition(RefOwnerBinding{RefOwnerKind::Committed, "p", mr(1, 5)},
                             RefOwnerBinding{RefOwnerKind::Precommit, "p", mr(1, 6)})})),
                 DB::Exception);

    /// Promote-shaped kinds (old=Precommit, new=Committed) but with MISMATCHED ref_names.
    EXPECT_THROW(manifestEdgesOfTxn(txn("db/t", rid(1, 3),
        {rawOwnerTransition(RefOwnerBinding{RefOwnerKind::Precommit, "p", mr(1, 5)},
                             RefOwnerBinding{RefOwnerKind::Committed, "q", mr(1, 5)})})),
                 DB::Exception);

    /// Add with new.kind == Committed (only Precommit is a legal add target).
    EXPECT_THROW(manifestEdgesOfTxn(txn("db/t", rid(1, 4),
        {rawOwnerTransition(std::nullopt, RefOwnerBinding{RefOwnerKind::Committed, "p", mr(1, 5)})})),
                 DB::Exception);

    /// old+new both Committed, same manifest: not a promote (promote requires old.kind == Precommit).
    EXPECT_THROW(manifestEdgesOfTxn(txn("db/t", rid(1, 5),
        {rawOwnerTransition(RefOwnerBinding{RefOwnerKind::Committed, "p", mr(1, 5)},
                             RefOwnerBinding{RefOwnerKind::Committed, "p", mr(1, 5)})})),
                 DB::Exception);
}

/// Namespaces are edge-distinct even with identical ManifestRef tuples (spec §gc-inputs-and-output).
TEST(CASRefIntake, EdgesAreNamespaceQualified)
{
    const auto a = manifestEdgesOfTxn(txn("db/a", rid(1, 1), {addOwner(RefOwnerKind::Precommit, "p", mr(1, 5))}));
    const auto b = manifestEdgesOfTxn(txn("db/b", rid(1, 1), {addOwner(RefOwnerKind::Precommit, "p", mr(1, 5))}));
    ASSERT_EQ(a.size(), 1u);
    ASSERT_EQ(b.size(), 1u);
    EXPECT_NE(a[0].manifest_id, b[0].manifest_id);
}

TEST(CASRefIntake, RemovalTxnIdDetection)
{
    RefOp remove_ns;
    remove_ns.kind = RefOpKind::RemoveNamespace;
    const auto with_removal = txn("db/t", rid(3, 8), {removeOwner(RefOwnerKind::Committed, "p", mr(1, 5)), remove_ns});
    ASSERT_TRUE(removalTxnId(with_removal).has_value());
    EXPECT_EQ(*removalTxnId(with_removal), rid(3, 8));

    const auto ordinary = txn("db/t", rid(3, 9), {addOwner(RefOwnerKind::Precommit, "p", mr(1, 5))});
    EXPECT_FALSE(removalTxnId(ordinary).has_value());
}

/// spec §Step 1: one global LIST groups by table, split by kind, sorted; the reconstructed namespace is
/// re-validated (VERIFY-AT-T12) and a malformed ref key aborts ref folding (throws).
TEST(CASRefIntake, GroupRefKeys)
{
    const Layout layout{"p"};
    const RootNamespace ns{"db/t"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(ns);

    std::vector<String> keys{
        layout.refSnapshotKey(life, rid(1, 4)),
        layout.refLogKey(life, rid(1, 5)),
        layout.refLogKey(life, rid(1, 3)),
        layout.refCkptKey(life),        /// state-family keys are outside the hot stream LIST
        "p/cas/manifests/db/t/foo",   /// outside the ref prefix -> ignored
    };
    const auto grouped = groupRefKeys(layout, keys);
    ASSERT_EQ(grouped.size(), 1u);
    const RefTableListing & t = grouped.at(life.incarnation);
    EXPECT_EQ(t.logs, (std::vector<RefTxnId>{rid(1, 3), rid(1, 5)}));
    EXPECT_EQ(t.snapshots, (std::vector<RefTxnId>{rid(1, 4)}));
    /// Checkpoints live under `cas/ns/state/` and are deliberately absent from the hot stream listing.

    /// A key under the ref prefix that is not a valid ref object aborts (a leftover old-format shard key).
    EXPECT_THROW(groupRefKeys(layout, {"p/cas/ns/stream/0"}), DB::Exception);
    /// A malformed physical id under a valid stream prefix aborts.
    EXPECT_THROW(groupRefKeys(layout, {"p/cas/ns/stream/not-an-id/_log/" + renderRefTxnId(rid(1, 1))}), DB::Exception);
}

/// A LIST can observe a snapshot after its PUT but before the `_ckpt` CAS makes it a recovery base.
/// That physical object proves nothing by itself: without a checkpoint-named triple, cleanup leaks
/// rather than deleting either the genesis log or the unacknowledged snapshot.
TEST(CASRefIntake, PlanRefCleanupRequiresCheckpointNamedBase)
{
    RefTableListing listing;
    listing.logs = {rid(1, 1), rid(1, 2), rid(1, 3)};
    listing.snapshots = {rid(1, 2)};   /// newest observed snapshot X = (1,2)

    /// Even a complete-looking listing and cursor do not license cleanup without the checkpoint's
    /// exact base. This is the snapshot-PUT-before-checkpoint-CAS sabotage.
    {
        const auto plan = planRefCleanup(listing, rid(1, 3), {});
        EXPECT_TRUE(plan.deletable_logs.empty());
        EXPECT_TRUE(plan.deletable_snapshots.empty());
    }
    /// A smaller cursor cannot turn that incomplete authority into a cleanup range.
    {
        const auto plan = planRefCleanup(listing, rid(1, 1), {});
        EXPECT_TRUE(plan.deletable_logs.empty());
    }
    /// Nor may a newer listed snapshot reclaim an older listed snapshot before `_ckpt` names a base.
    {
        RefTableListing two_snaps = listing;
        two_snaps.snapshots = {rid(1, 1), rid(1, 2)};
        const auto plan = planRefCleanup(two_snaps, rid(1, 3), {});
        EXPECT_TRUE(plan.deletable_snapshots.empty());
    }
    /// No snapshot => no coverage boundary => empty plan (condition 2).
    {
        RefTableListing no_snap;
        no_snap.logs = {rid(1, 1)};
        const auto plan = planRefCleanup(no_snap, rid(1, 5), {});
        EXPECT_TRUE(plan.deletable_logs.empty());
        EXPECT_TRUE(plan.deletable_snapshots.empty());
    }
}

/// The checkpoint recovery anchor is a triple: `_ckpt`, its same-id `_snap`, and the same-id ordinary
/// `_log` that proves the id is not an `EpochSeal`. Cleanup may reclaim older covered logs, but must
/// retain that one witness for recovery and fsck.
TEST(CASRefIntake, PlanRefCleanupRetainsCheckpointBaseLog)
{
    RefTableListing listing;
    listing.logs = {rid(1, 1), rid(1, 2), rid(1, 3)};
    listing.snapshots = {rid(1, 2)};

    const RefCleanupPlan plan = planRefCleanup(listing, rid(1, 3), rid(1, 2));
    EXPECT_EQ(plan.deletable_logs, (std::vector<RefTxnId>{rid(1, 1)}));
    EXPECT_TRUE(plan.deletable_snapshots.empty());
}
