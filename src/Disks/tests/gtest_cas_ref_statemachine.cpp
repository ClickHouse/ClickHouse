#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <algorithm>
#include <cstdint>
#include <optional>
#include <random>
#include <vector>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
}

using namespace DB::Cas;
using DB::Cas::tests::expectThrowsCode;

namespace
{

/// ===================================================================================
/// Small builders (mirrors gtest_cas_ref_codecs.cpp's local helpers)
/// ===================================================================================

ManifestRef manifestRef(uint64_t epoch, uint64_t seq, uint32_t ordinal)
{
    return ManifestRef{epoch, seq, ordinal};
}

RefLogTxn makeTxn(const String & ns, RefTxnId id, std::vector<RefOp> ops)
{
    RefLogTxn txn;
    txn.ns = ns;
    txn.txn_id = id;
    txn.ops = std::move(ops);
    return txn;
}

RefOp birthOp()
{
    RefOp op;
    op.kind = RefOpKind::NamespaceBirth;
    return op;
}

RefOp addPrecommitOp(const String & name, const ManifestRef & mref)
{
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, name, mref};
    return op;
}

RefOp removePrecommitOp(const String & name, const ManifestRef & mref)
{
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, name, mref};
    return op;
}

RefOp promoteOp(const String & name, const ManifestRef & mref)
{
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, name, mref};
    op.new_binding = RefOwnerBinding{RefOwnerKind::Committed, name, mref};
    return op;
}

RefOp removeCommittedOp(const String & name, const ManifestRef & mref)
{
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Committed, name, mref};
    return op;
}

RefOp setPublishedAtOp(const String & name, const ManifestRef & mref, uint64_t ts = 0)
{
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = name;
    op.expected_manifest_ref = mref;
    op.published_at_ms = ts;
    return op;
}

RefOp removeNamespaceOp()
{
    RefOp op;
    op.kind = RefOpKind::RemoveNamespace;
    return op;
}

/// Field-by-field comparison (via getters) rather than a `RefTableState::operator==` addition: the
/// class is the plan's verbatim-normative interface and gains no member beyond what it specifies.
void expectStatesEqual(const RefTableState & a, const RefTableState & b)
{
    EXPECT_EQ(a.getLifecycle(), b.getLifecycle());
    EXPECT_EQ(a.getRemoveTxnId(), b.getRemoveTxnId());
    EXPECT_EQ(a.getGreatestApplied(), b.getGreatestApplied());
    EXPECT_EQ(a.getCommitted(), b.getCommitted());
    EXPECT_EQ(a.getPrecommits(), b.getPrecommits());
    /// Also compare the incremental budget counters: in release builds (no `debugAssertBodyCounters`)
    /// this is the only cross-check that catches counter drift between two equal-looking states.
    EXPECT_EQ(a.getSnapshotBodyBytes(), b.getSnapshotBodyBytes());
    EXPECT_EQ(a.getRemovalBodyBytes(), b.getRemovalBodyBytes());
}

/// The spec's own construction for a hypothetical `remove_namespace` transaction (§Remove Namespace):
/// an exact owner-removal op for every committed ref and precommit, then `remove_namespace`. Built
/// independently of `CasRefStateMachine.cpp`'s internal helper of the same shape, purely from the
/// public `RefTableState` fields, so the admission-budget property tests below measure against a
/// ground truth this test file derives on its own.
RefLogTxn buildRemovalTxnForTest(const RefTableState & state, const String & ns, RefTxnId id)
{
    std::vector<RefOp> ops;
    for (const auto [name, row] : state.getCommitted())
        ops.push_back(removeCommittedOp(name, row.manifest_ref));
    for (const auto & [name, mref] : state.getPrecommits())
        ops.push_back(removePrecommitOp(name, mref));
    ops.push_back(removeNamespaceOp());
    return makeTxn(ns, id, std::move(ops));
}

constexpr const char * kNs = "srv1/db/table@cas@";

/// A validated state with "a" committed to manifest (1,1,1) -- the base the fail-closed replay/append
/// tests below reuse to build a tail whose add-precommit op would collide cross-owner (name the SAME
/// manifest under a DIFFERENT ref_name).
RefTableSnapshot buildCollidingBaseSnapshotForTest()
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));
    return snapshotOf(state, kNs);
}

}

/// ===================================================================================
/// NamespaceBirth
/// ===================================================================================

TEST(CASRefStateMachine, BirthFromNeverBornAccepts)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    EXPECT_EQ(state.getLifecycle(), RefLifecycle::Live);
    EXPECT_FALSE(state.getRemoveTxnId().has_value());
    EXPECT_EQ(state.getGreatestApplied(), (RefTxnId{1, 1}));
}

TEST(CASRefStateMachine, BirthWhileLiveRejectedAndStateUnchanged)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {birthOp()})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, BirthAfterRemovalAccepts)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removeNamespaceOp()}));
    ASSERT_EQ(state.getLifecycle(), RefLifecycle::Removed);
    ASSERT_TRUE(state.getRemoveTxnId().has_value());

    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3}, {birthOp()}));
    EXPECT_EQ(state.getLifecycle(), RefLifecycle::Live);
    EXPECT_FALSE(state.getRemoveTxnId().has_value());
}

/// ===================================================================================
/// Ops rejected outside Live (never-born and Removed) except birth
/// ===================================================================================

TEST(CASRefStateMachine, OwnerTransitionWhileNeverBornRejected)
{
    RefTableState state;
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {addPrecommitOp("a", manifestRef(1, 1, 1))})); });
}

TEST(CASRefStateMachine, SetPublishedAtWhileNeverBornRejected)
{
    RefTableState state;
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {setPublishedAtOp("a", manifestRef(1, 1, 1))})); });
}

TEST(CASRefStateMachine, RemoveNamespaceWhileNeverBornRejected)
{
    RefTableState state;
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {removeNamespaceOp()})); });
}

TEST(CASRefStateMachine, OpsWhileRemovedRejectedExceptBirth)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removeNamespaceOp()}));
    const RefTableState after_removal = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3}, {addPrecommitOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(after_removal, state);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 4}, {setPublishedAtOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(after_removal, state);

    /// Repeated removal is corruption at THIS layer (spec §Remove Namespace: idempotent-success is
    /// the API layer's job, not the state machine's).
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 5}, {removeNamespaceOp()})); });
    expectStatesEqual(after_removal, state);
}

/// ===================================================================================
/// Add precommit (spec §Add Precommit)
/// ===================================================================================

TEST(CASRefStateMachine, AddPrecommitAccepts)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}));
    EXPECT_TRUE(state.getPrecommits().contains({"a", manifestRef(1, 1, 1)}));
}

TEST(CASRefStateMachine, AddPrecommitRejectsExactDuplicate)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, AddPrecommitRejectsConflictingManifestUnderDifferentName)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}));
    const RefTableState before = state;

    /// Same manifest_ref, a DIFFERENT ref_name: "no conflicting owner may name the same manifest".
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("b", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, AddPrecommitRejectsManifestAlreadyCommittedElsewhere)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));
    ASSERT_TRUE(state.getCommitted().contains("a"));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("b", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, AddPrecommitAllowsDifferentManifestsRacingForSameName)
{
    /// Two builds racing for the same final ref name (same shape gtest_cas_ref_codecs.cpp's
    /// RoundTripPrecommitsSameNameDifferentManifest round-trips): distinct manifest_ref, no conflict.
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("same", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("same", manifestRef(1, 2, 1))}));
    EXPECT_TRUE(state.getPrecommits().contains({"same", manifestRef(1, 1, 1)}));
    EXPECT_TRUE(state.getPrecommits().contains({"same", manifestRef(1, 2, 1)}));
}

/// ===================================================================================
/// Remove precommit / remove committed (spec §Remove Precommit, §Remove Committed Ref)
/// ===================================================================================

TEST(CASRefStateMachine, RemovePrecommitAccepts)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removePrecommitOp("a", manifestRef(1, 1, 1))}));
    EXPECT_TRUE(state.getPrecommits().empty());
}

TEST(CASRefStateMachine, RemovePrecommitRejectsAbsentBinding)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removePrecommitOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, RemovePrecommitRejectsWrongManifest)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removePrecommitOp("a", manifestRef(1, 2, 1))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, RemoveCommittedAccepts)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removeCommittedOp("a", manifestRef(1, 1, 1))}));
    EXPECT_TRUE(state.getCommitted().empty());
}

TEST(CASRefStateMachine, RemoveCommittedRejectsAbsentRef)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removeCommittedOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, RemoveCommittedRejectsWrongManifest)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removeCommittedOp("a", manifestRef(9, 9, 9))})); });
    expectStatesEqual(before, state);
}

/// ===================================================================================
/// Promote (spec §Promote): exact precommit required, atomicity, invalid shapes
/// ===================================================================================

TEST(CASRefStateMachine, PromoteRejectsAbsentPrecommit)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {promoteOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, PromoteAtomicityNoOwnerlessIntermediate)
{
    /// A bare promote (no set_published_at in the same transaction) is itself a complete, valid, and
    /// OBSERVABLE transaction -- there is no partial-op state exposed here, only the choice of
    /// whether the timestamp arrives in this txn or a later one (spec §Promote).
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {promoteOp("a", manifestRef(1, 1, 1))}));

    EXPECT_FALSE(state.getPrecommits().contains({"a", manifestRef(1, 1, 1)}));
    ASSERT_TRUE(state.getCommitted().contains("a"));
    EXPECT_EQ(state.getCommitted().at("a").manifest_ref, manifestRef(1, 1, 1));
    EXPECT_EQ(state.getCommitted().at("a").published_at_ms, 0u);
}

TEST(CASRefStateMachine, PromoteWithSetPublishedAtInSameTxnInstallsTimestamp)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2},
        {promoteOp("a", manifestRef(1, 1, 1)), setPublishedAtOp("a", manifestRef(1, 1, 1), 42)}));

    ASSERT_TRUE(state.getCommitted().contains("a"));
    EXPECT_EQ(state.getCommitted().at("a").published_at_ms, 42u);
}

TEST(CASRefStateMachine, PromoteRejectsDisplacingAnotherCommittedManifest)
{
    /// A challenger precommit under the SAME ref_name as an already-committed (different) manifest is
    /// legal to stage (spec §Add Precommit only restricts manifest identity, not ref_name), but a bare
    /// promote of it must not silently displace the stale committed row.
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("a", manifestRef(1, 2, 1))}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3}, {promoteOp("a", manifestRef(1, 2, 1))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, PromoteAcceptsAfterExplicitRemovalOfStaleCommitted)
{
    /// The correct atomic-replace sequence: an explicit removal of the old committed row, followed by
    /// the promote, in the SAME transaction -- both ops are recorded, so GC sees the old manifest's
    /// "-1" edge explicitly rather than losing it to a silent displacement.
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("a", manifestRef(1, 2, 1))}));

    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3},
        {removeCommittedOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 2, 1))}));

    ASSERT_TRUE(state.getCommitted().contains("a"));
    EXPECT_EQ(state.getCommitted().at("a").manifest_ref, manifestRef(1, 2, 1));
    EXPECT_FALSE(state.getPrecommits().contains({"a", manifestRef(1, 2, 1)}));
}

TEST(CASRefStateMachine, OwnerTransitionRejectsInvalidCombinations)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));

    /// old=None, new=Committed: not a recognized shape (committed rows are only reached via promote).
    {
        RefOp op;
        op.kind = RefOpKind::OwnerTransition;
        op.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "a", manifestRef(1, 1, 1)};
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
            [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {op})); });
    }

    /// A promote-shaped op (Precommit -> Committed) with mismatched ref_name is not a legal promote.
    /// The rejected transaction above left `greatest_applied` untouched, so THIS one is still {1, 2}.
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("a", manifestRef(1, 1, 1))}));
    {
        RefOp op;
        op.kind = RefOpKind::OwnerTransition;
        op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "a", manifestRef(1, 1, 1)};
        op.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "b", manifestRef(1, 1, 1)};
        const RefTableState before = state;
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
            [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3}, {op})); });
        expectStatesEqual(before, state);
    }

    /// old=Committed, new=Precommit: moving a committed ref "backwards" is not a recognized shape.
    {
        RefOp op;
        op.kind = RefOpKind::OwnerTransition;
        op.old_binding = RefOwnerBinding{RefOwnerKind::Committed, "a", manifestRef(1, 1, 1)};
        op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "a", manifestRef(1, 1, 1)};
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
            [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3}, {op})); });
    }
}

/// ===================================================================================
/// SetPublishedAt (spec §Update Payload)
/// ===================================================================================

TEST(CASRefStateMachine, SetPublishedAtRejectsWhenRefAbsent)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {setPublishedAtOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, SetPublishedAtRejectsManifestMismatch)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {setPublishedAtOp("a", manifestRef(9, 9, 9))})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, SetPublishedAtAcceptsAndReplacesTimestamp)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {setPublishedAtOp("a", manifestRef(1, 1, 1), 10)}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3}, {setPublishedAtOp("a", manifestRef(1, 1, 1), 20)}));

    EXPECT_EQ(state.getCommitted().at("a").published_at_ms, 20u);
    EXPECT_EQ(state.getCommitted().at("a").manifest_ref, manifestRef(1, 1, 1));   /// unchanged: no edge move
}

/// ===================================================================================
/// RemoveNamespace ordering lens (spec §Remove Namespace; codec deliberately doesn't check this)
/// ===================================================================================

TEST(CASRefStateMachine, RemoveNamespaceAloneOnEmptyTableAccepted)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removeNamespaceOp()}));
    EXPECT_EQ(state.getLifecycle(), RefLifecycle::Removed);
    ASSERT_TRUE(state.getRemoveTxnId().has_value());
    EXPECT_EQ(*state.getRemoveTxnId(), (RefTxnId{1, 2}));
}

TEST(CASRefStateMachine, CatalogedNeverBornLifeAcceptsAtomicEmptyBirthAndRemoval)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), removeNamespaceOp()}));
    EXPECT_EQ(state.getLifecycle(), RefLifecycle::Removed);
    ASSERT_TRUE(state.getRemoveTxnId().has_value());
    EXPECT_EQ(*state.getRemoveTxnId(), (RefTxnId{1, 1}));
    EXPECT_TRUE(state.getCommitted().empty());
    EXPECT_TRUE(state.getPrecommits().empty());
}

TEST(CASRefStateMachine, RemoveNamespaceDrainingOwnersInSameTxnAccepted)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), addPrecommitOp("b", manifestRef(1, 2, 1)),
         promoteOp("b", manifestRef(1, 2, 1))}));
    ASSERT_TRUE(state.getPrecommits().contains({"a", manifestRef(1, 1, 1)}));
    ASSERT_TRUE(state.getCommitted().contains("b"));

    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2},
        {removePrecommitOp("a", manifestRef(1, 1, 1)), removeCommittedOp("b", manifestRef(1, 2, 1)),
         removeNamespaceOp()}));
    EXPECT_EQ(state.getLifecycle(), RefLifecycle::Removed);
    EXPECT_TRUE(state.getCommitted().empty());
    EXPECT_TRUE(state.getPrecommits().empty());
}

TEST(CASRefStateMachine, RemoveNamespaceRejectsWhenOwnersRemain)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), addPrecommitOp("b", manifestRef(1, 2, 1))}));
    const RefTableState before = state;

    /// Only "a" is drained; "b" remains -- remove_namespace's own precondition (empty owner sets)
    /// must fail, and the WHOLE transaction (including the "a" removal) must not apply.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2},
            {removePrecommitOp("a", manifestRef(1, 1, 1)), removeNamespaceOp()})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, RemoveNamespaceMustBeFinalOp)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removeNamespaceOp(), birthOp()})); });
    expectStatesEqual(before, state);
}

TEST(CASRefStateMachine, RemoveNamespaceRejectsNonRemovalEarlierOp)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));
    const RefTableState before = state;

    /// set_published_at before remove_namespace: not an owner-removal transition.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2},
            {setPublishedAtOp("a", manifestRef(1, 1, 1)), removeCommittedOp("a", manifestRef(1, 1, 1)),
             removeNamespaceOp()})); });
    expectStatesEqual(before, state);

    /// An ADD (not a removal) owner_transition before remove_namespace: also rejected.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3},
            {addPrecommitOp("c", manifestRef(1, 3, 1)), removeCommittedOp("a", manifestRef(1, 1, 1)),
             removeNamespaceOp()})); });
    expectStatesEqual(before, state);
}

/// ===================================================================================
/// Whole-transaction atomicity: a failing LAST op leaves the whole txn (and earlier ops) unapplied
/// ===================================================================================

TEST(CASRefStateMachine, WholeTxnAtomicityLastOpFailureLeavesStateUntouched)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    const RefTableState before = state;

    /// ops[0] (add "a") would succeed in isolation; ops[1] (remove absent "b") fails -- the whole
    /// transaction, including "a", must be rejected.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2},
            {addPrecommitOp("a", manifestRef(1, 1, 1)), removePrecommitOp("b", manifestRef(9, 9, 9))})); });

    expectStatesEqual(before, state);
    EXPECT_FALSE(state.getPrecommits().contains({"a", manifestRef(1, 1, 1)}));
}

/// ===================================================================================
/// Contiguous txn ids (INV-1)
/// ===================================================================================

/// A table's durable ids are DENSE within `(namespace, epoch)`: the only admissible id is the one
/// `nextRefTxnId` derives from `greatest_applied`, which is also the only id the writer ever mints.
/// Equal, lower, and skipped ids are all corruption -- the last of those is what makes "I can see ids
/// 1..T" mean "nothing is missing", the property the whole invariant exists to provide.
TEST(CASRefStateMachine, ContiguousTxnIdsRejectEqualLowerAndSkipped)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    const RefTableState before = state;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {addPrecommitOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{0, 999}, {addPrecommitOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);

    /// Strictly greater but SKIPPED: admitted before INV-1, corruption now.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3}, {addPrecommitOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);

    /// A new epoch restarts the sequence, so it must start at 1 -- carrying the previous epoch's
    /// numbering forward would read exactly like a lost first transaction of the new stream.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{2, 2}, {addPrecommitOp("a", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);

    /// The successor applies; then the next epoch's first id does.
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("a", manifestRef(1, 1, 1))}));
    EXPECT_EQ(state.getGreatestApplied(), (RefTxnId{1, 2}));

    /// Crossing into a new epoch needs INV-2's chain link as well as INV-1's id: without it the reader
    /// cannot tell an EMPTY epoch from a lost one, so a sequence-1 transaction that names no seal is
    /// refused on a Live table. Both halves are pinned, since either alone would be silently weaker.
    RefLogTxn crossing = makeTxn(kNs, RefTxnId{2, 1}, {addPrecommitOp("b", manifestRef(2, 1, 1))});
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { applyRefLogTxn(state, crossing); });
    crossing.prev_epoch_seal = RefTxnId{1, 3};   /// the seal that closed epoch 1, one past its last id
    applyRefLogTxn(state, crossing);
    EXPECT_EQ(state.getGreatestApplied(), (RefTxnId{2, 1}));
}

/// ===================================================================================
/// snapshotOf: canonical sort + terminal-state refusal
/// ===================================================================================

TEST(CASRefStateMachine, SnapshotOfSortsCommittedAndPrecommits)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("zzz", manifestRef(1, 3, 1)), addPrecommitOp("aaa", manifestRef(1, 1, 1)),
         promoteOp("aaa", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("mmm", manifestRef(1, 2, 1))}));

    const RefTableSnapshot snap = snapshotOf(state, kNs);
    ASSERT_EQ(snap.committed.size(), 1u);
    EXPECT_EQ(snap.committed[0].ref_name, "aaa");
    ASSERT_EQ(snap.precommits.size(), 2u);
    EXPECT_EQ(snap.precommits[0].ref_name, "mmm");
    EXPECT_EQ(snap.precommits[1].ref_name, "zzz");
    EXPECT_EQ(snap.snapshot_id, (RefTxnId{1, 2}));

    /// The result must actually be encodable (canonical shape) -- a real round trip through the codec.
    const String bytes = encodeRefTableSnapshot(snap);
    const RefTableSnapshot decoded = decodeRefTableSnapshot(bytes, kNs, snap.snapshot_id);
    EXPECT_EQ(decoded, snap);
}

TEST(CASRefStateMachine, SnapshotOfRefusesTerminalState)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {removeNamespaceOp()}));

    EXPECT_EQ(state.getLifecycle(), RefLifecycle::Removed);
    ASSERT_TRUE(state.getRemoveTxnId().has_value());
    EXPECT_EQ(*state.getRemoveTxnId(), (RefTxnId{1, 2}));
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { snapshotOf(state, kNs); });
}

/// ===================================================================================
/// replay: TableState = Replay(S_X.state, tail(X))
/// ===================================================================================

TEST(CASRefStateMachine, ReplayFromNoSnapshot)
{
    std::vector<RefLogTxn> tail{
        makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}),
        makeTxn(kNs, RefTxnId{1, 2}, {promoteOp("a", manifestRef(1, 1, 1))}),
    };
    const RefTableState state = replay(std::nullopt, tail);
    EXPECT_EQ(state.getLifecycle(), RefLifecycle::Live);
    EXPECT_TRUE(state.getCommitted().contains("a"));
    EXPECT_EQ(state.getGreatestApplied(), (RefTxnId{1, 2}));
}

TEST(CASRefStateMachine, ReplayFromSnapshotPlusTail)
{
    RefTableState built;
    applyRefLogTxn(built, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}));
    const RefTableSnapshot snap = snapshotOf(built, kNs);

    std::vector<RefLogTxn> tail{makeTxn(kNs, RefTxnId{1, 2}, {promoteOp("a", manifestRef(1, 1, 1))})};
    const RefTableState state = replay(snap, tail);
    EXPECT_TRUE(state.getCommitted().contains("a"));
    EXPECT_EQ(state.getGreatestApplied(), (RefTxnId{1, 2}));
}

TEST(CASRefStateMachine, StateFromSnapshotConstructsLiveState)
{
    RefTableSnapshot snap;
    snap.ns = kNs;
    snap.snapshot_id = RefTxnId{1, 1};

    const RefTableState state = stateFromSnapshot(snap);
    EXPECT_EQ(state.getLifecycle(), RefLifecycle::Live);
    EXPECT_FALSE(state.getRemoveTxnId().has_value());
}

TEST(CASRefStateMachine, ReplayRejectsTailNsMismatchAgainstSnapshot)
{
    RefTableState built;
    applyRefLogTxn(built, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    const RefTableSnapshot snap = snapshotOf(built, kNs);

    std::vector<RefLogTxn> tail{makeTxn("other-ns", RefTxnId{1, 2}, {addPrecommitOp("a", manifestRef(1, 1, 1))})};
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(snap, tail); });
}

TEST(CASRefStateMachine, ReplayRejectsTailNsMismatchAcrossEntries)
{
    std::vector<RefLogTxn> tail{
        makeTxn("ns-a", RefTxnId{1, 1}, {birthOp()}),
        makeTxn("ns-b", RefTxnId{1, 2}, {addPrecommitOp("a", manifestRef(1, 1, 1))}),
    };
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(std::nullopt, tail); });
}

TEST(CASRefStateMachine, ReplayRejectsHandBuiltSnapshotWithDuplicateCommittedName)
{
    /// A hand-built RefTableSnapshot (never passed through decodeRefTableSnapshot -- exactly what
    /// fsck hands to replay) with two committed rows sharing one ref_name must be rejected, not
    /// silently collapsed to one row via std::map::emplace (the phantom-alive class of bug fixed in
    /// stateFromSnapshot).
    RefTableSnapshot snap;
    snap.ns = kNs;
    snap.snapshot_id = RefTxnId{1, 1};
    RefCommittedRow row1;
    row1.ref_name = "a";
    row1.manifest_ref = manifestRef(1, 1, 1);
    RefCommittedRow row2;
    row2.ref_name = "a";
    row2.manifest_ref = manifestRef(1, 2, 1);
    snap.committed.push_back(row1);
    snap.committed.push_back(row2);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(snap, {}); });
}

TEST(CASRefStateMachine, ReplayRejectsHandBuiltSnapshotWithUnsortedPrecommits)
{
    RefTableSnapshot snap;
    snap.ns = kNs;
    snap.snapshot_id = RefTxnId{1, 1};
    snap.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "b", manifestRef(1, 1, 1)});
    snap.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "a", manifestRef(1, 2, 1)});

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(snap, {}); });
}

/// Randomized replay equation: replay(snapshotOf(mid-state), tail) == full replay (spec §Table State).
TEST(CASRefStateMachine, ReplayEquationPropertyTest)
{
    std::mt19937 rng(4242); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    const std::vector<String> names{"a", "b", "c"};

    for (int trial = 0; trial < 30; ++trial)
    {
        std::vector<RefLogTxn> history;
        uint64_t seq = 1;
        history.push_back(makeTxn(kNs, RefTxnId{1, seq++}, {birthOp()}));

        /// Track our own model of legal next actions so every generated op is guaranteed valid --
        /// this test exercises the replay equation, not the rejection paths (covered above).
        std::vector<std::pair<String, ManifestRef>> open_precommits;
        std::vector<std::pair<String, ManifestRef>> open_committed;
        uint64_t next_build_seq = 1;

        const int steps = 15;
        for (int step = 0; step < steps; ++step)
        {
            const uint32_t choice = rng() % 4;
            if (choice == 0 || (open_precommits.empty() && open_committed.empty()))
            {
                /// Add precommit under a fresh manifest_ref (never collides, so always legal).
                const String & name = names[rng() % names.size()];
                const ManifestRef mref = manifestRef(1, next_build_seq++, 1);
                history.push_back(makeTxn(kNs, RefTxnId{1, seq++}, {addPrecommitOp(name, mref)}));
                open_precommits.emplace_back(name, mref);
            }
            else if (choice == 1 && !open_precommits.empty())
            {
                /// Only a name NOT already committed is eligible for a BARE promote: promoting into an
                /// already-committed name requires an explicit prior removal in the same transaction
                /// (spec §Promote; see PromoteRejectsDisplacingAnotherCommittedManifest) -- a distinct
                /// scenario from the one this equation test exercises.
                std::vector<size_t> eligible;
                for (size_t i = 0; i < open_precommits.size(); ++i)
                {
                    const bool already_committed = std::any_of(open_committed.begin(), open_committed.end(),
                        [&](const auto & c) { return c.first == open_precommits[i].first; });
                    if (!already_committed)
                        eligible.push_back(i);
                }
                if (!eligible.empty())
                {
                    const size_t idx = eligible[rng() % eligible.size()];
                    const auto [name, mref] = open_precommits[idx];
                    open_precommits.erase(open_precommits.begin() + static_cast<int64_t>(idx));
                    history.push_back(makeTxn(kNs, RefTxnId{1, seq++}, {promoteOp(name, mref)}));
                    open_committed.emplace_back(name, mref);
                }
            }
            else if (choice == 2 && !open_committed.empty())
            {
                const size_t idx = rng() % open_committed.size();
                const auto & [name, mref] = open_committed[idx];
                const uint64_t this_id = seq++;
                history.push_back(makeTxn(kNs, RefTxnId{1, this_id},
                    {setPublishedAtOp(name, mref, this_id)}));
            }
            else if (!open_precommits.empty())
            {
                const size_t idx = rng() % open_precommits.size();
                const auto [name, mref] = open_precommits[idx];
                open_precommits.erase(open_precommits.begin() + static_cast<int64_t>(idx));
                history.push_back(makeTxn(kNs, RefTxnId{1, seq++}, {removePrecommitOp(name, mref)}));
            }
            else if (!open_committed.empty())
            {
                const size_t idx = rng() % open_committed.size();
                const auto [name, mref] = open_committed[idx];
                open_committed.erase(open_committed.begin() + static_cast<int64_t>(idx));
                history.push_back(makeTxn(kNs, RefTxnId{1, seq++}, {removeCommittedOp(name, mref)}));
            }
        }

        const RefTableState full = replay(std::nullopt, history);

        const size_t cut = rng() % (history.size() + 1);
        const std::vector<RefLogTxn> head(history.begin(), history.begin() + static_cast<int64_t>(cut));
        const std::vector<RefLogTxn> tail(history.begin() + static_cast<int64_t>(cut), history.end());
        const RefTableState mid = replay(std::nullopt, head);
        const std::optional<RefTableSnapshot> mid_snapshot =
            cut == 0 ? std::nullopt : std::make_optional(snapshotOf(mid, kNs));
        const RefTableState resumed = replay(mid_snapshot, tail);

        expectStatesEqual(full, resumed);
    }
}

/// ===================================================================================
/// Fail-closed replay + snapshot validation: a corrupted history or snapshot naming one manifest under
/// two owners must be REJECTED in EVERY build (post-consult). The cross-owner uniqueness check is O(1)
/// via `owned_manifests`, so it runs unconditionally -- on the writer's append path AND on replay --
/// rather than being elided into a debug-only assertion. `stateFromSnapshot` enforces the same
/// invariant across snapshot rows (the codec never did).
/// ===================================================================================

/// (Add path, committed collision) The writer's append-time contract rejects a fresh precommit that
/// names a manifest already committed under a DIFFERENT ref_name, and leaves the state unchanged.
TEST(CASRefStateMachine, LiveAppendRejectsAddPrecommitCollidingWithCommitted)
{
    RefTableState state = stateFromSnapshot(buildCollidingBaseSnapshotForTest());
    const RefTableState before = state;
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("b", manifestRef(1, 1, 1))})); });
    expectStatesEqual(before, state);
}

/// (Replay path, committed collision) A tail whose add-precommit collides cross-owner with an existing
/// committed owner makes `replay` THROW -- it must NOT be silently accepted. This is the exact behavior
/// the deleted `TrustedReplaySkipsCrossOwnerScanInRelease` test pinned as *desired*; post-consult it is
/// the opposite: fail closed.
TEST(CASRefStateMachine, ReplayRejectsTailAddPrecommitCollidingWithCommitted)
{
    const RefTableSnapshot snap = buildCollidingBaseSnapshotForTest();
    const std::vector<RefLogTxn> tail{makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("b", manifestRef(1, 1, 1))})};
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(snap, tail); });
}

/// (Replay path, precommit collision) The same, but the base already holds a PRECOMMIT for the manifest
/// and the tail adds a second precommit for it under another ref_name (precommit/precommit collision).
TEST(CASRefStateMachine, ReplayRejectsTailAddPrecommitCollidingWithPrecommit)
{
    const std::vector<RefLogTxn> tail{
        makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}),
        makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("b", manifestRef(1, 1, 1))}),   // collides cross-owner
    };
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(std::nullopt, tail); });
}

/// (Snapshot validation, committed/committed) A hand-built snapshot with two committed rows naming ONE
/// manifest passes the codec (it checks only sortedness + no-duplicate ref_name) but must be rejected by
/// `stateFromSnapshot`/`replay` as semantically corrupt.
TEST(CASRefStateMachine, ReplayRejectsSnapshotWithTwoCommittedRowsNamingOneManifest)
{
    RefTableSnapshot snap;
    snap.ns = kNs;
    snap.snapshot_id = RefTxnId{1, 1};
    RefCommittedRow row1;
    row1.ref_name = "a";
    row1.manifest_ref = manifestRef(1, 1, 1);
    RefCommittedRow row2;
    row2.ref_name = "b";                       // distinct ref_name (codec-legal)...
    row2.manifest_ref = manifestRef(1, 1, 1);  // ...but the SAME manifest (corrupt)
    snap.committed.push_back(row1);
    snap.committed.push_back(row2);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(snap, {}); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)stateFromSnapshot(snap); });
}

/// (Snapshot validation, committed/precommit) A committed row and a precommit binding sharing one
/// manifest -- also codec-legal (different owner kinds, sorted independently) but corrupt.
TEST(CASRefStateMachine, ReplayRejectsSnapshotWithCommittedAndPrecommitSharingManifest)
{
    RefTableSnapshot snap;
    snap.ns = kNs;
    snap.snapshot_id = RefTxnId{1, 1};
    RefCommittedRow row;
    row.ref_name = "a";
    row.manifest_ref = manifestRef(1, 1, 1);
    snap.committed.push_back(row);
    snap.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "b", manifestRef(1, 1, 1)});

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(snap, {}); });
}

/// (Snapshot validation, precommit/precommit) Two precommit bindings under different ref_names naming
/// one manifest -- sorted by (ref_name, manifest_ref), so codec-legal, but corrupt.
TEST(CASRefStateMachine, ReplayRejectsSnapshotWithTwoPrecommitsSharingManifest)
{
    RefTableSnapshot snap;
    snap.ns = kNs;
    snap.snapshot_id = RefTxnId{1, 1};
    snap.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "a", manifestRef(1, 1, 1)});
    snap.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "b", manifestRef(1, 1, 1)});

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(snap, {}); });
}

/// Positive equivalence: a VALID tail replayed via `replay` (the in-place trusted path) produces a state
/// byte-identical (getters + encoded snapshot) to the same tail applied via the public strong-guarantee
/// `applyRefLogTxn` -- the apply strategy changes nothing a legal transaction produces.
TEST(CASRefStateMachine, TrustedReplayEquivalentToLiveAppendOnValidTail)
{
    const std::vector<RefLogTxn> tail{
        makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}),
        makeTxn(kNs, RefTxnId{1, 2},
            {promoteOp("a", manifestRef(1, 1, 1)), addPrecommitOp("b", manifestRef(1, 2, 1))}),
        makeTxn(kNs, RefTxnId{1, 3}, {setPublishedAtOp("a", manifestRef(1, 1, 1), 7)}),
        makeTxn(kNs, RefTxnId{1, 4}, {promoteOp("b", manifestRef(1, 2, 1))}),
    };

    RefTableState full_state;
    for (const RefLogTxn & txn : tail)
        applyRefLogTxn(full_state, txn);   // LiveAppend (default)

    const RefTableState trusted_state = replay(std::nullopt, tail);   // replay uses the in-place trusted path internally

    expectStatesEqual(full_state, trusted_state);
    EXPECT_EQ(encodeRefTableSnapshot(snapshotOf(full_state, kNs)),
              encodeRefTableSnapshot(snapshotOf(trusted_state, kNs)));
}

/// ===================================================================================
/// E3: apply strategy per validation mode
///   - LiveAppend: two-phase scratch copy, "throw => state byte-for-byte unchanged"
///   - TrustedReplay (replay): in-place, poison-on-throw, discarded by the sole caller
/// ===================================================================================

namespace
{
/// A populated, MATERIALIZED Live state -- committed "a"->(1,1,1) plus a pending precommit
/// ("p",(1,2,1)) -- built through the public LiveAppend path, then materialized so its COW overlays are
/// empty (exactly the shape the writer's live state has at each flush boundary). The E3 LiveAppend-path
/// tests mutate a COPY of this and assert the original-equivalent captured bytes/getters are intact
/// after a rejected transaction.
RefTableState buildPopulatedLiveState()
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1)),
         addPrecommitOp("p", manifestRef(1, 2, 1))}));
    state.materializeCommitted();
    return state;
}
}

/// LiveAppend-path atomicity, LATER-op throw ("populated" abort path): the first two ops touch committed,
/// precommits, the owned-manifest index and the body counters; the third is illegal. The whole
/// transaction is rejected and `state` is byte-for-byte unchanged -- getters AND encoded-snapshot
/// bytes. This is the writer's live-state contract, preserved verbatim by E3's `LiveAppend` arm.
TEST(CASRefStateMachine, E3LiveAppendLaterOpThrowLeavesPopulatedStateByteIdentical)
{
    RefTableState state = buildPopulatedLiveState();
    const RefTableState before = state;
    const String before_bytes = encodeRefTableSnapshot(snapshotOf(state, kNs));

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {
            addPrecommitOp("q", manifestRef(1, 3, 1)),       // touches precommits + index + counters
            removeCommittedOp("a", manifestRef(1, 1, 1)),     // touches committed + index + counters
            removePrecommitOp("absent", manifestRef(9, 9, 9)) // ILLEGAL: exact binding absent -> throws
        })); });

    expectStatesEqual(before, state);
    EXPECT_EQ(before_bytes, encodeRefTableSnapshot(snapshotOf(state, kNs)));
    /// Neither surviving-looking earlier op leaked into the live state.
    EXPECT_FALSE(state.getPrecommits().contains({"q", manifestRef(1, 3, 1)}));
    EXPECT_TRUE(state.getCommitted().contains("a"));
}

/// LiveAppend-path atomicity, FIRST-op throw ("empty" abort path -- nothing applied before the throw): the
/// symmetric guarantee still holds. Distinct from the case above because no op ever mutated the
/// scratch, exercising the throw-before-any-effect branch.
TEST(CASRefStateMachine, E3LiveAppendFirstOpThrowLeavesPopulatedStateByteIdentical)
{
    RefTableState state = buildPopulatedLiveState();
    const RefTableState before = state;
    const String before_bytes = encodeRefTableSnapshot(snapshotOf(state, kNs));

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {
            removeCommittedOp("absent", manifestRef(9, 9, 9)), // ILLEGAL first op
            addPrecommitOp("q", manifestRef(1, 3, 1))
        })); });

    expectStatesEqual(before, state);
    EXPECT_EQ(before_bytes, encodeRefTableSnapshot(snapshotOf(state, kNs)));
}

/// `admits` previews an op against `state` and must leave it byte-for-byte unchanged whether the op
/// fits (true) or overflows (false) -- it is a pure query. Verified against both getters and encoded
/// bytes, for both the accept and the reject verdicts.
TEST(CASRefStateMachine, E3AdmitsPreviewLeavesStateByteIdentical)
{
    RefTableState state = buildPopulatedLiveState();
    /// Must be an independent snapshot -- `state` is queried and potentially mutated below, and
    /// comparing against a reference would make the check vacuous.
    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization)
    const RefTableState before = state;
    const String before_bytes = encodeRefTableSnapshot(snapshotOf(state, kNs));

    const RefOp grow = addPrecommitOp("q", manifestRef(1, 3, 1));

    /// Accept verdict (ample budget): state untouched.
    EXPECT_TRUE(admits(state, grow, 1'000'000, 1'000'000));
    expectStatesEqual(before, state);
    EXPECT_EQ(before_bytes, encodeRefTableSnapshot(snapshotOf(state, kNs)));

    /// Reject verdict (snapshot budget one byte short of the grown size): state STILL untouched.
    RefTableState grown = state;
    applyRefLogTxn(grown, makeTxn(kNs, RefTxnId{1, 2}, {grow}));
    const size_t grown_size = encodeRefTableSnapshot(snapshotOf(grown, "")).size();
    EXPECT_FALSE(admits(state, grow, grown_size - 1, 1'000'000));
    expectStatesEqual(before, state);
    EXPECT_EQ(before_bytes, encodeRefTableSnapshot(snapshotOf(state, kNs)));
}

/// TrustedReplay in-place apply, SUCCESS path across every `applyOp` arm: a tail that births, adds,
/// promotes, replaces a committed manifest, removes a committed and a precommit, restamps a timestamp,
/// and finally removes the namespace, replayed via `replay` (TrustedReplay, in place) must produce a
/// state byte-identical to the SAME tail applied op-by-op through `LiveAppend` (scratch copy). This is the
/// test only E3's in-place machinery can fail: a mis-maintained counter, a dropped owned-manifest
/// index entry, or a lost `greatest_applied` update on the no-copy path would diverge here.
TEST(CASRefStateMachine, E3TrustedReplayInPlaceMatchesLiveAppendAcrossAllArms)
{
    const std::vector<RefLogTxn> tail{
        makeTxn(kNs, RefTxnId{1, 1}, {birthOp(),
            addPrecommitOp("a", manifestRef(1, 1, 1)), addPrecommitOp("b", manifestRef(1, 2, 1))}),
        makeTxn(kNs, RefTxnId{1, 2}, {
            promoteOp("a", manifestRef(1, 1, 1)),                     // precommit -> committed
            setPublishedAtOp("a", manifestRef(1, 1, 1), 42)}),        // restamp published_at_ms
        makeTxn(kNs, RefTxnId{1, 3}, {removePrecommitOp("b", manifestRef(1, 2, 1))}),   // drop precommit
        makeTxn(kNs, RefTxnId{1, 4}, {
            removeCommittedOp("a", manifestRef(1, 1, 1)),             // evict stale committed...
            addPrecommitOp("a", manifestRef(1, 9, 1)),                // ...then re-add under same name
            promoteOp("a", manifestRef(1, 9, 1))}),                   // and promote the replacement
        makeTxn(kNs, RefTxnId{1, 5}, {
            removeCommittedOp("a", manifestRef(1, 9, 1)),             // drain the last owner...
            removeNamespaceOp()}),                                     // ...then remove the namespace
    };

    RefTableState full_state;
    for (const RefLogTxn & txn : tail)
        applyRefLogTxn(full_state, txn);   // LiveAppend (default): two-phase scratch copy

    const RefTableState replayed = replay(std::nullopt, tail);   // TrustedReplay in-place

    expectStatesEqual(full_state, replayed);
    EXPECT_THROW(encodeRefTableSnapshot(snapshotOf(full_state, kNs)), DB::Exception);
    EXPECT_THROW(encodeRefTableSnapshot(snapshotOf(replayed, kNs)), DB::Exception);
    EXPECT_EQ(replayed.getLifecycle(), RefLifecycle::Removed);
    EXPECT_EQ(replayed.getRemoveTxnId(), std::make_optional(RefTxnId{1, 5}));
}

/// TrustedReplay in-place apply, THROW path: a tail whose LAST transaction is illegal makes `replay`
/// throw `CORRUPTED_DATA`. The in-place apply poisons a state that is entirely internal to the failed
/// `replay` call (it is never assigned to a caller on a throw), so an INDEPENDENT replay of just the
/// valid prefix is completely unaffected -- pinning that the poison never escapes.
TEST(CASRefStateMachine, E3TrustedReplayPoisonOnBadTailIsInternal)
{
    const std::vector<RefLogTxn> good_prefix{
        makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}),
        makeTxn(kNs, RefTxnId{1, 2}, {promoteOp("a", manifestRef(1, 1, 1))}),
    };
    std::vector<RefLogTxn> bad_tail = good_prefix;
    /// A third txn whose op removes an absent precommit -- legal txn_id ordering, illegal effect, so it
    /// throws mid-apply AFTER the good prefix has already been applied in place to the internal state.
    bad_tail.push_back(makeTxn(kNs, RefTxnId{1, 3}, {removePrecommitOp("absent", manifestRef(9, 9, 9))}));

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { replay(std::nullopt, bad_tail); });

    /// The failed replay's poisoned internal state never leaked: a fresh replay of the valid prefix is
    /// byte-identical to one built entirely via LiveAppend, and reflects exactly the prefix.
    const RefTableState from_prefix = replay(std::nullopt, good_prefix);
    RefTableState full_prefix;
    for (const RefLogTxn & txn : good_prefix)
        applyRefLogTxn(full_prefix, txn);
    expectStatesEqual(full_prefix, from_prefix);
    EXPECT_TRUE(from_prefix.getCommitted().contains("a"));
    EXPECT_EQ(from_prefix.getGreatestApplied(), (RefTxnId{1, 2}));
}

/// ===================================================================================
/// admits(): dual-bound admission budget (spec §Snapshot Format)
/// ===================================================================================

TEST(CASRefStateMachine, AdmitsAcceptsWellUnderBudget)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    EXPECT_TRUE(admits(state, addPrecommitOp("a", manifestRef(1, 1, 1)), 1'000'000, 1'000'000));
}

TEST(CASRefStateMachine, AdmitsRejectsGrowthPastSnapshotBudgetOwnerTransitionAdd)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));

    const RefOp op = addPrecommitOp("a", manifestRef(1, 1, 1));
    RefTableState scratch = state;
    applyRefLogTxn(scratch, makeTxn(kNs, RefTxnId{1, 2}, {op}));
    const size_t true_size = encodeRefTableSnapshot(snapshotOf(scratch, "")).size();

    EXPECT_TRUE(admits(state, op, true_size, 1'000'000));
    EXPECT_FALSE(admits(state, op, true_size - 1, 1'000'000));
}

TEST(CASRefStateMachine, AdmitsRejectsGrowthPastSnapshotBudgetSetPublishedAt)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));

    const RefOp op = setPublishedAtOp("a", manifestRef(1, 1, 1), 1700000000000ull);
    RefTableState scratch = state;
    applyRefLogTxn(scratch, makeTxn(kNs, RefTxnId{1, 2}, {op}));
    const size_t true_size = encodeRefTableSnapshot(snapshotOf(scratch, "")).size();

    EXPECT_TRUE(admits(state, op, true_size, 1'000'000));
    EXPECT_FALSE(admits(state, op, true_size - 1, 1'000'000));
}

TEST(CASRefStateMachine, AdmitsRejectsGrowthPastSnapshotBudgetPromoteWithSetPublishedAt)
{
    /// The "promote-with-set_published_at" growth class: the owner_transition half of a promote is
    /// admitted cheaply (published_at_ms starts unset), but the immediately-following set_published_at
    /// that installs the REAL initial timestamp is where the growth actually happens.
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {promoteOp("a", manifestRef(1, 1, 1))}));
    ASSERT_TRUE(state.getCommitted().contains("a"));
    ASSERT_EQ(state.getCommitted().at("a").published_at_ms, 0u);

    const RefOp op = setPublishedAtOp("a", manifestRef(1, 1, 1), 1700000000099ull);
    RefTableState scratch = state;
    applyRefLogTxn(scratch, makeTxn(kNs, RefTxnId{1, 3}, {op}));
    const size_t true_size = encodeRefTableSnapshot(snapshotOf(scratch, "")).size();

    EXPECT_TRUE(admits(state, op, true_size, 1'000'000));
    EXPECT_FALSE(admits(state, op, true_size - 1, 1'000'000));
}

TEST(CASRefStateMachine, AdmitsRejectsGrowthPastRemovalBudget)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(), addPrecommitOp("a", manifestRef(1, 1, 1)), promoteOp("a", manifestRef(1, 1, 1))}));

    const RefOp op = setPublishedAtOp("a", manifestRef(1, 1, 1), 1700000000300ull);
    RefTableState scratch = state;
    applyRefLogTxn(scratch, makeTxn(kNs, RefTxnId{1, 2}, {op}));
    const String removal_bytes = encodeRefLogTxn(buildRemovalTxnForTest(scratch, "", RefTxnId{1, 1}));
    const size_t true_removal_size = removal_bytes.size();

    /// A generous snapshot budget isolates the removal-budget bound specifically.
    EXPECT_TRUE(admits(state, op, 1'000'000, true_removal_size));
    EXPECT_FALSE(admits(state, op, 1'000'000, true_removal_size - 1));
}

/// Randomized exactness property test: admits()'s internal size computation must exactly match the
/// real encoders' output, for both bounds, across randomized states and candidate growing ops.
TEST(CASRefStateMachine, AdmitsExactnessPropertyTest)
{
    std::mt19937 rng(777); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.

    for (int trial = 0; trial < 20; ++trial)
    {
        RefTableState state;
        applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
        uint64_t seq = 2;
        uint64_t next_build_seq = 1;
        std::vector<std::pair<String, ManifestRef>> open_precommits;
        std::vector<std::pair<String, ManifestRef>> open_committed;

        /// Build up a random but valid mid-state (a handful of precommits/committed rows/timestamps).
        const int setup_steps = 1 + static_cast<int>(rng() % 5);
        for (int i = 0; i < setup_steps; ++i)
        {
            const String name = "ref" + std::to_string(rng() % 4);
            const ManifestRef mref = manifestRef(1, next_build_seq++, 1);
            applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, seq++}, {addPrecommitOp(name, mref)}));
            open_precommits.emplace_back(name, mref);

            /// A bare promote may not target a name already committed under a different manifest
            /// (spec §Promote; see PromoteRejectsDisplacingAnotherCommittedManifest) -- skip promoting
            /// this iteration's precommit when an earlier iteration already committed the same name.
            const bool name_already_committed = std::any_of(open_committed.begin(), open_committed.end(),
                [&](const auto & c) { return c.first == name; });
            if (!name_already_committed && rng() % 2 == 0)
            {
                const auto [pname, pmref] = open_precommits.back();
                open_precommits.pop_back();
                applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, seq++}, {promoteOp(pname, pmref)}));
                open_committed.emplace_back(pname, pmref);
            }
        }

        /// Pick a random candidate growing op against this state.
        RefOp candidate;
        const uint32_t kind = rng() % 3;
        if (kind == 0 || open_committed.empty())
        {
            candidate = addPrecommitOp("fresh-" + std::to_string(trial), manifestRef(1, next_build_seq++, 1));
        }
        else if (kind == 1)
        {
            const auto & [name, mref] = open_committed[rng() % open_committed.size()];
            candidate = setPublishedAtOp(name, mref, rng());
        }
        else
        {
            /// A genuinely distinct third shape: a racing precommit under an ALREADY-committed name
            /// (legal -- spec §Add Precommit only restricts manifest identity, never ref_name).
            const String & name = open_committed[rng() % open_committed.size()].first;
            candidate = addPrecommitOp(name, manifestRef(1, next_build_seq++, 1));
        }

        RefTableState scratch = state;
        applyRefLogTxn(scratch, makeTxn(kNs, RefTxnId{1, seq}, {candidate}));
        const size_t true_snapshot_size = encodeRefTableSnapshot(snapshotOf(scratch, "")).size();
        const size_t true_removal_size =
            encodeRefLogTxn(buildRemovalTxnForTest(scratch, "", RefTxnId{1, 1})).size();

        EXPECT_TRUE(admits(state, candidate, true_snapshot_size, true_removal_size));
        EXPECT_FALSE(admits(state, candidate, true_snapshot_size - 1, true_removal_size));
        EXPECT_FALSE(admits(state, candidate, true_snapshot_size, true_removal_size - 1));
    }
}

/// ===================================================================================
/// Snapshot size helpers: framing + Σ per-row must equal a full encode, byte for byte.
/// ===================================================================================
TEST(CASRefSnapshotSizeHelpers, FramingPlusRowsEqualsFullEncode)
{
    /// Build a non-trivial Live table: two committed rows (one with a stamped published_at_ms) and one
    /// precommit.
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(),
         addPrecommitOp("alpha", manifestRef(1, 1, 1)), promoteOp("alpha", manifestRef(1, 1, 1)),
         addPrecommitOp("beta", manifestRef(1, 2, 1)), promoteOp("beta", manifestRef(1, 2, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2},
        {setPublishedAtOp("alpha", manifestRef(1, 1, 1), 42)}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3}, {addPrecommitOp("gamma", manifestRef(1, 3, 1))}));

    const RefTableSnapshot snap = snapshotOf(state, "");
    const size_t full = encodeRefTableSnapshot(snap).size();

    size_t rebuilt = snapshotFramingSize("", snap.snapshot_id, snap.committed.size() + snap.precommits.size());
    for (const RefCommittedRow & row : snap.committed)
        rebuilt += committedRowEncodedSize(row);
    for (const RefOwnerBinding & pc : snap.precommits)
        rebuilt += precommitRowEncodedSize(pc);

    EXPECT_EQ(rebuilt, full);
}

/// ===================================================================================
/// Removal-txn size helpers: framing + Σ per-owner-op must equal a full removal-txn encode.
/// ===================================================================================
TEST(CASRefLogSizeHelpers, FramingPlusOpsEqualsFullRemovalEncode)
{
    RefTableState state;
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1},
        {birthOp(),
         addPrecommitOp("alpha", manifestRef(1, 1, 1)), promoteOp("alpha", manifestRef(1, 1, 1)),
         addPrecommitOp("beta", manifestRef(1, 2, 1))}));

    /// Ground truth: the whole-namespace removal txn this test file already builds independently.
    const RefLogTxn removal = buildRemovalTxnForTest(state, "", RefTxnId{1, 1});
    const size_t full = encodeRefLogTxn(removal).size();

    size_t rebuilt = removalFramingSize("", RefTxnId{1, 1},
                                        state.getCommitted().size() + state.getPrecommits().size() + 1);
    for (const auto [name, row] : state.getCommitted())
        rebuilt += removalOpEncodedSize(RefOwnerKind::Committed, name, row.manifest_ref);
    for (const auto & [name, mref] : state.getPrecommits())
        rebuilt += removalOpEncodedSize(RefOwnerKind::Precommit, name, mref);

    EXPECT_EQ(rebuilt, full);
}

/// ===================================================================================
/// Body-byte counters: snapshot_body_bytes / removal_body_bytes are a pure function of the rows.
/// ===================================================================================
namespace
{
uint64_t recomputeSnapshotBody(const RefTableState & s)
{
    uint64_t total = 0;
    for (const auto [name, row] : s.getCommitted())
        total += committedRowEncodedSize(row);
    for (const auto & [name, mref] : s.getPrecommits())
        total += precommitRowEncodedSize(RefOwnerBinding{RefOwnerKind::Precommit, name, mref});
    return total;
}
uint64_t recomputeRemovalBody(const RefTableState & s)
{
    uint64_t total = 0;
    for (const auto [name, row] : s.getCommitted())
        total += removalOpEncodedSize(RefOwnerKind::Committed, name, row.manifest_ref);
    for (const auto & [name, mref] : s.getPrecommits())
        total += removalOpEncodedSize(RefOwnerKind::Precommit, name, mref);
    return total;
}
}

TEST(CASRefStateCounters, CountersTrackRowsThroughEveryOpKind)
{
    RefTableState state;
    EXPECT_EQ(state.getSnapshotBodyBytes(), 0u);
    EXPECT_EQ(state.getRemovalBodyBytes(), 0u);

    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 2}, {addPrecommitOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 3}, {promoteOp("a", manifestRef(1, 1, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 4},
        {setPublishedAtOp("a", manifestRef(1, 1, 1), 5)}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 5}, {addPrecommitOp("b", manifestRef(1, 2, 1))}));
    EXPECT_EQ(state.getSnapshotBodyBytes(), recomputeSnapshotBody(state));
    EXPECT_EQ(state.getRemovalBodyBytes(), recomputeRemovalBody(state));

    /// Shrink back down: remove the precommit, then the committed row.
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 6}, {removePrecommitOp("b", manifestRef(1, 2, 1))}));
    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 7}, {removeCommittedOp("a", manifestRef(1, 1, 1))}));
    EXPECT_EQ(state.getSnapshotBodyBytes(), recomputeSnapshotBody(state));
    EXPECT_EQ(state.getRemovalBodyBytes(), recomputeRemovalBody(state));
    EXPECT_EQ(state.getSnapshotBodyBytes(), 0u);
    EXPECT_EQ(state.getRemovalBodyBytes(), 0u);
}

/// ===================================================================================
/// Budget-size accessors equal the real encoders across randomized states.
/// ===================================================================================
TEST(CASRefBudgetSize, AccessorsEqualFullEncodeRandomized)
{
    std::mt19937 rng(1234); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducibility.
    for (int trial = 0; trial < 30; ++trial)
    {
        RefTableState state;
        applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, 1}, {birthOp()}));
        uint64_t seq = 2;
        uint64_t build = 1;
        std::vector<std::pair<String, ManifestRef>> committed_names;

        const int steps = 1 + static_cast<int>(rng() % 6);
        for (int i = 0; i < steps; ++i)
        {
            const String name = "r" + std::to_string(rng() % 5);
            const ManifestRef mref = manifestRef(1, build++, 1);
            applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, seq++}, {addPrecommitOp(name, mref)}));
            const bool already = std::any_of(committed_names.begin(), committed_names.end(),
                [&](const auto & c) { return c.first == name; });
            if (!already && rng() % 2 == 0)
            {
                applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, seq++}, {promoteOp(name, mref)}));
                committed_names.emplace_back(name, mref);
                if (rng() % 2 == 0)
                    applyRefLogTxn(state, makeTxn(kNs, RefTxnId{1, seq++},
                        {setPublishedAtOp(name, mref, rng())}));
            }
        }

        const size_t true_snapshot = encodeRefTableSnapshot(snapshotOf(state, "")).size();
        const size_t true_removal = encodeRefLogTxn(buildRemovalTxnForTest(state, "", RefTxnId{1, 1})).size();
        EXPECT_EQ(encodedSnapshotBudgetSize(state), true_snapshot);
        EXPECT_EQ(encodedRemovalBudgetSize(state), true_removal);
    }
}
