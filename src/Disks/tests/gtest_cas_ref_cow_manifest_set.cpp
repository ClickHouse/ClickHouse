#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCowManifestSet.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

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

ManifestRef mref(uint64_t epoch, uint64_t seq, uint32_t ordinal)
{
    return ManifestRef{epoch, seq, ordinal};
}

}

/// ===================================================================================
/// Keyed ops: contains/insert/erase across base+overlay (the "E2 owned-manifest index" work).
/// ===================================================================================

TEST(CASRefCowManifestSet, EmptySetHasNoMembers)
{
    RefCowManifestSet s;
    EXPECT_TRUE(s.empty());
    EXPECT_EQ(s.size(), 0u);
    EXPECT_FALSE(s.contains(mref(1, 1, 1)));
}

TEST(CASRefCowManifestSet, InsertThenContains)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    EXPECT_TRUE(s.contains(mref(1, 1, 1)));
    EXPECT_EQ(s.size(), 1u);
    EXPECT_FALSE(s.contains(mref(2, 2, 2)));
}

TEST(CASRefCowManifestSet, InsertMultipleThenContainsEachIndependently)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.insert(mref(1, 1, 2));
    s.insert(mref(2, 1, 1));
    EXPECT_EQ(s.size(), 3u);
    EXPECT_TRUE(s.contains(mref(1, 1, 1)));
    EXPECT_TRUE(s.contains(mref(1, 1, 2)));
    EXPECT_TRUE(s.contains(mref(2, 1, 1)));
    EXPECT_FALSE(s.contains(mref(3, 3, 3)));
}

TEST(CASRefCowManifestSet, EraseRemovesAnOverlayOnlyMember)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.erase(mref(1, 1, 1));
    EXPECT_FALSE(s.contains(mref(1, 1, 1)));
    EXPECT_EQ(s.size(), 0u);
    EXPECT_TRUE(s.empty());
    EXPECT_EQ(s.overlayEntriesForTest(), 0u);   /// pure-overlay member: erase removes it outright
}

TEST(CASRefCowManifestSet, TombstoneThenReinsertWhilePurelyInOverlay)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.erase(mref(1, 1, 1));
    s.insert(mref(1, 1, 1));   /// re-insert -- must not be treated as "still present"
    EXPECT_TRUE(s.contains(mref(1, 1, 1)));
    EXPECT_EQ(s.size(), 1u);
}

/// ===================================================================================
/// materialize()
/// ===================================================================================

TEST(CASRefCowManifestSet, MaterializeFoldsOverlayIntoBaseAndEmptiesOverlay)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.insert(mref(1, 1, 2));
    EXPECT_GT(s.overlayEntriesForTest(), 0u);

    s.materialize();
    EXPECT_EQ(s.overlayEntriesForTest(), 0u);
    EXPECT_TRUE(s.contains(mref(1, 1, 1)));
    EXPECT_TRUE(s.contains(mref(1, 1, 2)));
    EXPECT_EQ(s.size(), 2u);
}

TEST(CASRefCowManifestSet, MaterializeOnAnEmptyOverlayIsANoOp)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.materialize();
    const int64_t use_count_before = s.baseUseCountForTest();
    s.materialize();   /// overlay is already empty
    EXPECT_EQ(s.baseUseCountForTest(), use_count_before);
    EXPECT_TRUE(s.contains(mref(1, 1, 1)));
}

TEST(CASRefCowManifestSet, EraseAfterMaterializeTombstonesABaseMember)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.insert(mref(1, 1, 2));
    s.materialize();   /// both now live in `base`

    s.erase(mref(1, 1, 1));
    EXPECT_FALSE(s.contains(mref(1, 1, 1)));
    EXPECT_TRUE(s.contains(mref(1, 1, 2)));
    EXPECT_EQ(s.size(), 1u);

    s.materialize();   /// tombstone folds away; base member actually removed
    EXPECT_FALSE(s.contains(mref(1, 1, 1)));
    EXPECT_EQ(s.size(), 1u);
}

TEST(CASRefCowManifestSet, TombstoneThenReinsertAcrossMaterializedBase)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.materialize();   /// mref(1,1,1) now lives in `base`

    s.erase(mref(1, 1, 1));            /// tombstone shadowing the base member
    s.insert(mref(1, 1, 1));           /// revive the tombstone -- must read as present again
    EXPECT_TRUE(s.contains(mref(1, 1, 1)));
    EXPECT_EQ(s.size(), 1u);

    s.materialize();
    EXPECT_TRUE(s.contains(mref(1, 1, 1)));
    EXPECT_EQ(s.size(), 1u);
}

/// ===================================================================================
/// materialize() fast path: fold into a uniquely-owned base IN PLACE, no O(N) copy (E5).
/// ===================================================================================

TEST(CASRefCowManifestSet, MaterializeReusesBaseWhenUniquelyOwned)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.materialize();                    /// mref(1,1,1) now in base; base is uniquely owned
    const void * base_before = s.baseIdentityForTest();
    ASSERT_EQ(s.baseUseCountForTest(), 1);

    s.insert(mref(2, 2, 2));            /// pure-overlay addition
    s.erase(mref(1, 1, 1));            /// tombstone a base member
    s.materialize();

    EXPECT_EQ(s.baseIdentityForTest(), base_before);   /// folded in place: same base allocation
    EXPECT_EQ(s.overlayEntriesForTest(), 0u);
    EXPECT_FALSE(s.contains(mref(1, 1, 1)));            /// tombstone erased from base
    EXPECT_TRUE(s.contains(mref(2, 2, 2)));
    EXPECT_EQ(s.size(), 1u);                            /// net_delta reset, size still exact
}

TEST(CASRefCowManifestSet, MaterializeBuildsFreshBaseWhenBaseIsShared)
{
    RefCowManifestSet original;
    original.insert(mref(1, 1, 1));
    original.materialize();
    const void * shared_base = original.baseIdentityForTest();

    RefCowManifestSet writer = original;   /// shares the base (use_count 2)
    ASSERT_EQ(writer.baseUseCountForTest(), 2);
    writer.insert(mref(9, 9, 9));
    writer.erase(mref(1, 1, 1));
    writer.materialize();                  /// base is shared -> must build a fresh one, mutate nothing shared

    /// Load-bearing correctness pin: the OTHER holder's view is byte-unchanged.
    EXPECT_EQ(original.baseIdentityForTest(), shared_base);
    EXPECT_TRUE(original.contains(mref(1, 1, 1)));
    EXPECT_FALSE(original.contains(mref(9, 9, 9)));
    EXPECT_EQ(original.size(), 1u);

    /// The writer folded its overlay into a fresh base of its own.
    EXPECT_NE(writer.baseIdentityForTest(), shared_base);
    EXPECT_FALSE(writer.contains(mref(1, 1, 1)));
    EXPECT_TRUE(writer.contains(mref(9, 9, 9)));
    EXPECT_EQ(writer.size(), 1u);
}

TEST(CASRefCowManifestSet, MaterializeEmptyOverlayIsANoOpEvenWhenUniquelyOwned)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.materialize();
    ASSERT_EQ(s.baseUseCountForTest(), 1);
    const void * base_before = s.baseIdentityForTest();
    s.materialize();   /// overlay already empty: no fold, no reallocation
    EXPECT_EQ(s.baseIdentityForTest(), base_before);
    EXPECT_TRUE(s.contains(mref(1, 1, 1)));
}

/// ===================================================================================
/// Copy-on-write isolation + O(1)-copy assertion.
/// ===================================================================================

TEST(CASRefCowManifestSet, CopyIsIsolatedFromOriginal)
{
    RefCowManifestSet original;
    original.insert(mref(1, 1, 1));
    original.materialize();

    RefCowManifestSet copy = original;
    copy.insert(mref(9, 9, 9));
    copy.erase(mref(1, 1, 1));

    EXPECT_TRUE(original.contains(mref(1, 1, 1)));
    EXPECT_FALSE(original.contains(mref(9, 9, 9)));

    EXPECT_FALSE(copy.contains(mref(1, 1, 1)));
    EXPECT_TRUE(copy.contains(mref(9, 9, 9)));
}

TEST(CASRefCowManifestSet, CopySharesBaseUntilEitherSideMaterializesANewOne)
{
    RefCowManifestSet original;
    original.insert(mref(1, 1, 1));
    original.materialize();

    RefCowManifestSet copy = original;
    /// A copy shares the SAME base object (refcount bump, no per-element allocation) until a write
    /// forces a new base into existence via `materialize()`.
    EXPECT_EQ(original.baseUseCountForTest(), 2);
    EXPECT_EQ(copy.baseUseCountForTest(), 2);

    copy.insert(mref(2, 2, 2));   /// writes go to `copy`'s overlay; `base` is untouched
    EXPECT_EQ(original.baseUseCountForTest(), 2);
    EXPECT_EQ(copy.baseUseCountForTest(), 2);
    EXPECT_FALSE(original.contains(mref(2, 2, 2)));

    copy.materialize();   /// NOW `copy` points at a fresh base of its own
    EXPECT_EQ(original.baseUseCountForTest(), 1);
    EXPECT_EQ(copy.baseUseCountForTest(), 1);
}

/// ===================================================================================
/// size()/net_delta correctness across a longer op sequence, mixing base and overlay changes.
/// ===================================================================================

TEST(CASRefCowManifestSet, SizeTracksNetDeltaAcrossMixedOps)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.insert(mref(1, 1, 2));
    s.insert(mref(1, 1, 3));
    EXPECT_EQ(s.size(), 3u);
    s.materialize();
    EXPECT_EQ(s.size(), 3u);

    s.erase(mref(1, 1, 2));         /// base member removed via overlay tombstone
    EXPECT_EQ(s.size(), 2u);
    s.insert(mref(1, 1, 4));        /// pure-overlay addition
    EXPECT_EQ(s.size(), 3u);
    s.erase(mref(1, 1, 4));         /// pure-overlay addition removed outright
    EXPECT_EQ(s.size(), 2u);
    s.insert(mref(1, 1, 2));        /// revive the earlier tombstone
    EXPECT_EQ(s.size(), 3u);

    s.materialize();
    EXPECT_EQ(s.size(), 3u);
    EXPECT_TRUE(s.contains(mref(1, 1, 1)));
    EXPECT_TRUE(s.contains(mref(1, 1, 2)));
    EXPECT_TRUE(s.contains(mref(1, 1, 3)));
    EXPECT_FALSE(s.contains(mref(1, 1, 4)));
}

/// ===================================================================================
/// Drift-detection misuse (throws `CORRUPTED_DATA` in EVERY build, post-consult -- previously a
/// debug-only `chassert`): `insert` requires absence, `erase` requires presence. The ref table's own
/// uniqueness invariant guarantees both before either is ever called, so a violation here means the
/// index has drifted, not that a legitimate caller can trigger it. Failing closed (rather than a silent
/// release-build `net_delta` drift) is what keeps a corrupted history from later hiding a still-live
/// owner.
/// ===================================================================================

TEST(CASRefCowManifestSet, InsertThrowsWhenAlreadyPresentInOverlay)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { s.insert(mref(1, 1, 1)); });
}

TEST(CASRefCowManifestSet, InsertThrowsWhenAlreadyPresentInBase)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.materialize();
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { s.insert(mref(1, 1, 1)); });
}

TEST(CASRefCowManifestSet, EraseThrowsWhenAbsent)
{
    RefCowManifestSet s;
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { s.erase(mref(1, 1, 1)); });
}

TEST(CASRefCowManifestSet, EraseThrowsWhenAlreadyTombstoned)
{
    RefCowManifestSet s;
    s.insert(mref(1, 1, 1));
    s.materialize();
    s.erase(mref(1, 1, 1));
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { s.erase(mref(1, 1, 1)); });
}

/// ===================================================================================
/// Fast-vs-forced-slow materialize parity (E5 xhigh review): the in-place fold (uniquely-owned base)
/// and the build-fresh-and-swap fold (a copy still shares the base) must agree on membership and size
/// across randomized op sequences. No iteration surface here, so membership is probed over a fixed
/// keyspace. insert/erase preconditions are respected (guarded by the shared membership) so the two
/// sets never drift and never trip the fail-closed CORRUPTED_DATA guards.
/// ===================================================================================

TEST(CASRefCowManifestSet, FastAndForcedSlowMaterializeAgreeOverRandomOps)
{
    std::mt19937 rng(20260722); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible coverage.

    std::vector<ManifestRef> keyspace;
    for (uint64_t k = 0; k < 10; ++k)
        keyspace.push_back(mref(1, k, 1));

    for (int trial = 0; trial < 60; ++trial)
    {
        RefCowManifestSet fast;   /// never copied -> in-place (uniquely-owned) materialize
        RefCowManifestSet slow;   /// a live copy is held across each materialize -> forced fresh-base path

        for (int step = 0; step < 150; ++step)
        {
            const ManifestRef m = keyspace[rng() % keyspace.size()];
            const bool present = fast.contains(m);   /// identical in both sets by construction
            switch (rng() % 5)
            {
                case 0:
                    if (!present)   /// respect the insert precondition (absent)
                    {
                        fast.insert(m);
                        slow.insert(m);
                    }
                    break;
                case 1:
                    if (present)    /// respect the erase precondition (present)
                    {
                        fast.erase(m);
                        slow.erase(m);
                    }
                    break;
                case 2:   /// materialize both, each via its intended path
                {
                    ASSERT_EQ(fast.baseUseCountForTest(), 1) << "fast set must be uniquely owned";
                    fast.materialize();   /// in-place fast path
                    {
                        RefCowManifestSet pin = slow;   /// shares slow's base
                        ASSERT_EQ(slow.baseUseCountForTest(), 2) << "slow set must be forced onto the copy path";
                        slow.materialize();             /// build-fresh-and-swap slow path
                    }
                    EXPECT_EQ(fast.overlayEntriesForTest(), 0u) << "trial " << trial << " step " << step;
                    EXPECT_EQ(slow.overlayEntriesForTest(), 0u) << "trial " << trial << " step " << step;
                    break;
                }
                default:
                    break;   /// accumulate overlay without materializing
            }

            ASSERT_EQ(fast.size(), slow.size()) << "trial " << trial << " step " << step;
            for (const auto & probe : keyspace)
                ASSERT_EQ(fast.contains(probe), slow.contains(probe)) << "trial " << trial << " step " << step;
        }

        fast.materialize();
        {
            RefCowManifestSet pin = slow;
            slow.materialize();
        }
        EXPECT_EQ(fast.overlayEntriesForTest(), 0u) << "trial " << trial;
        EXPECT_EQ(slow.overlayEntriesForTest(), 0u) << "trial " << trial;
        EXPECT_EQ(fast.size(), slow.size()) << "trial " << trial;
        for (const auto & probe : keyspace)
            EXPECT_EQ(fast.contains(probe), slow.contains(probe)) << "trial " << trial;
    }
}
