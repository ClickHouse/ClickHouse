#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCowMap.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>

#include <map>
#include <random>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace DB::Cas;

namespace
{

RefCommittedRow row(uint64_t epoch, uint64_t seq, uint32_t ordinal)
{
    RefCommittedRow r;
    r.manifest_ref = ManifestRef{epoch, seq, ordinal};
    return r;
}

}

/// ===================================================================================
/// Keyed ops
/// ===================================================================================

TEST(CASRefCowMap, EmptyMapHasNoEntries)
{
    RefCowMap m;
    EXPECT_TRUE(m.empty());
    EXPECT_EQ(m.size(), 0u);
    EXPECT_FALSE(m.contains("a"));
    EXPECT_FALSE(m.contains("a"));
}

TEST(CASRefCowMap, EmplaceThenFind)
{
    RefCowMap m;
    const auto [it, inserted] = m.emplace("a", row(1, 1, 1));
    EXPECT_TRUE(inserted);
    EXPECT_EQ(m.size(), 1u);
    ASSERT_TRUE(m.contains("a"));
    EXPECT_EQ(it->second.manifest_ref, (ManifestRef{1, 1, 1}));
    EXPECT_EQ(m.at("a").manifest_ref, (ManifestRef{1, 1, 1}));
}

TEST(CASRefCowMap, EmplaceDoesNotOverwriteExisting)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    const auto [it, inserted] = m.emplace("a", row(2, 2, 2));
    EXPECT_FALSE(inserted);
    EXPECT_EQ(it->second.manifest_ref, (ManifestRef{1, 1, 1}));
    EXPECT_EQ(m.at("a").manifest_ref, (ManifestRef{1, 1, 1}));   /// unchanged
}

TEST(CASRefCowMap, InsertOrAssignOverwritesExisting)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    const auto [it, inserted] = m.insert_or_assign("a", row(2, 2, 2));
    EXPECT_FALSE(inserted);
    EXPECT_EQ(it->second.manifest_ref, (ManifestRef{2, 2, 2}));
    EXPECT_EQ(m.at("a").manifest_ref, (ManifestRef{2, 2, 2}));
}

TEST(CASRefCowMap, InsertOrAssignInsertsWhenAbsent)
{
    RefCowMap m;
    const auto [it, inserted] = m.insert_or_assign("a", row(1, 1, 1));
    EXPECT_TRUE(inserted);
    EXPECT_EQ(m.size(), 1u);
    EXPECT_EQ(it->second.manifest_ref, (ManifestRef{1, 1, 1}));
}

TEST(CASRefCowMap, EraseByKey)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    EXPECT_EQ(m.erase("a"), 1u);
    EXPECT_FALSE(m.contains("a"));
    EXPECT_EQ(m.size(), 0u);
    EXPECT_EQ(m.erase("a"), 0u);              /// already gone: no-op
    EXPECT_EQ(m.erase("nonexistent"), 0u);
}

TEST(CASRefCowMap, AtThrowsOnMissingKey)
{
    RefCowMap m;
    EXPECT_THROW(m.at("missing"), std::out_of_range);
}

TEST(CASRefCowMap, CountMatchesContains)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    EXPECT_EQ(m.count("a"), 1u);
    EXPECT_EQ(m.count("b"), 0u);
}

/// ===================================================================================
/// Ordered iteration -- overlay overrides/tombstones a materialized base (spec: "Ordered
/// iteration: merge-iterate base and overlay ... a standard two-sorted-range merge").
/// ===================================================================================

TEST(CASRefCowMap, OrderedIterationOverAllBaseRowsIsSorted)
{
    RefCowMap m;
    m.emplace("c", row(1, 3, 1));
    m.emplace("a", row(1, 1, 1));
    m.emplace("b", row(1, 2, 1));

    std::vector<String> names;
    for (const auto [name, r] : m)
        names.push_back(name);
    EXPECT_EQ(names, (std::vector<String>{"a", "b", "c"}));
}

TEST(CASRefCowMap, MergedIterationAppliesTombstonesAndOverrides)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    m.emplace("b", row(1, 2, 1));
    m.emplace("c", row(1, 3, 1));
    m.materialize();   /// a, b, c now live in `base`

    m.insert_or_assign("b", row(9, 9, 9));   /// override b via the overlay
    m.erase("c");                             /// tombstone c via the overlay
    m.emplace("d", row(9, 9, 2));             /// pure-overlay addition (not in base)

    std::vector<std::pair<String, ManifestRef>> seen;
    for (const auto [name, r] : m)
        seen.emplace_back(name, r.manifest_ref);

    const std::vector<std::pair<String, ManifestRef>> expected = {
        {"a", ManifestRef{1, 1, 1}},
        {"b", ManifestRef{9, 9, 9}},
        {"d", ManifestRef{9, 9, 2}},
    };
    EXPECT_EQ(seen, expected);
    EXPECT_EQ(m.size(), 3u);
}

TEST(CASRefCowMap, FindOverlayOnlyKeyIteratesIntoBase)
{
    RefCowMap m;
    m.emplace("A", row(1, 1, 1));
    m.emplace("D", row(1, 4, 1));
    m.materialize();   /// A, D now live in `base`

    m.insert_or_assign("B", row(2, 2, 1));   /// overlay-only key between base keys "A" and "D"

    auto it = m.find("B");
    ASSERT_NE(it, m.end());
    EXPECT_EQ(it->first, "B");
    ++it;
    ASSERT_NE(it, m.end());   /// must land on "D", not collapse straight to end()
    EXPECT_EQ(it->first, "D");
}

TEST(CASRefCowMap, EraseByIteratorReturnsNextAndRemovesTheRow)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    m.emplace("b", row(1, 2, 1));
    m.emplace("c", row(1, 3, 1));

    auto it = m.find("b");
    ASSERT_TRUE(it != m.end());
    auto next = m.erase(it);
    ASSERT_TRUE(next != m.end());
    EXPECT_EQ(next->first, "c");
    EXPECT_FALSE(m.contains("b"));
    EXPECT_EQ(m.size(), 2u);
}

TEST(CASRefCowMap, EraseByIteratorOfLastElementReturnsEnd)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    auto it = m.find("a");
    auto next = m.erase(it);
    EXPECT_TRUE(next == m.end());
    EXPECT_TRUE(m.empty());
}

/// ===================================================================================
/// materialize() (spec §Materialization)
/// ===================================================================================

TEST(CASRefCowMap, MaterializeFoldsOverlayIntoFreshBaseAndKeepsValuesUnchanged)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    m.emplace("b", row(1, 2, 1));
    m.erase("a");
    EXPECT_GT(m.overlayEntriesForTest(), 0u);

    m.materialize();
    EXPECT_EQ(m.overlayEntriesForTest(), 0u);
    EXPECT_FALSE(m.contains("a"));
    ASSERT_TRUE(m.contains("b"));
    EXPECT_EQ(m.at("b").manifest_ref, (ManifestRef{1, 2, 1}));
    EXPECT_EQ(m.size(), 1u);
}

TEST(CASRefCowMap, MaterializeOnAnEmptyOverlayIsANoOp)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    m.materialize();
    const int64_t use_count_before = m.baseUseCountForTest();
    m.materialize();   /// overlay is already empty
    EXPECT_EQ(m.baseUseCountForTest(), use_count_before);
    EXPECT_TRUE(m.contains("a"));
}

TEST(CASRefCowMap, MaterializeDoesNotAffectACopyTakenBeforeIt)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    RefCowMap snapshot_before = m;   /// copy shares m's pre-materialize base, owns its own overlay
    m.insert_or_assign("a", row(2, 2, 2));
    m.materialize();

    EXPECT_EQ(m.at("a").manifest_ref, (ManifestRef{2, 2, 2}));
    EXPECT_EQ(snapshot_before.at("a").manifest_ref, (ManifestRef{1, 1, 1}));
}

/// ===================================================================================
/// materialize() fast path: fold into a uniquely-owned base IN PLACE, no O(N) copy (E5).
/// ===================================================================================

TEST(CASRefCowMap, MaterializeReusesBaseWhenUniquelyOwned)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    m.materialize();                          /// "a" now in base; base is uniquely owned
    const void * base_before = m.baseIdentityForTest();
    ASSERT_EQ(m.baseUseCountForTest(), 1);

    m.insert_or_assign("b", row(2, 2, 2));    /// pure-overlay addition
    m.erase("a");                             /// tombstone a base member
    m.materialize();

    EXPECT_EQ(m.baseIdentityForTest(), base_before);   /// folded in place: same base allocation
    EXPECT_EQ(m.overlayEntriesForTest(), 0u);
    EXPECT_FALSE(m.contains("a"));                      /// tombstone erased from base
    ASSERT_TRUE(m.contains("b"));
    EXPECT_EQ(m.at("b").manifest_ref, (ManifestRef{2, 2, 2}));
    EXPECT_EQ(m.size(), 1u);                            /// net_delta reset, size still exact
}

TEST(CASRefCowMap, MaterializeBuildsFreshBaseWhenBaseIsShared)
{
    RefCowMap original;
    original.emplace("a", row(1, 1, 1));
    original.materialize();
    const void * shared_base = original.baseIdentityForTest();

    RefCowMap writer = original;              /// shares the base (use_count 2)
    ASSERT_EQ(writer.baseUseCountForTest(), 2);
    writer.insert_or_assign("a", row(9, 9, 9));
    writer.emplace("b", row(9, 9, 2));
    writer.materialize();                     /// base is shared -> must build a fresh one, mutate nothing shared

    /// Load-bearing correctness pin: the OTHER holder's view is byte-unchanged.
    EXPECT_EQ(original.baseIdentityForTest(), shared_base);
    EXPECT_EQ(original.at("a").manifest_ref, (ManifestRef{1, 1, 1}));
    EXPECT_FALSE(original.contains("b"));
    EXPECT_EQ(original.size(), 1u);

    /// The writer folded its overlay into a fresh base of its own.
    EXPECT_NE(writer.baseIdentityForTest(), shared_base);
    EXPECT_EQ(writer.at("a").manifest_ref, (ManifestRef{9, 9, 9}));
    EXPECT_TRUE(writer.contains("b"));
    EXPECT_EQ(writer.size(), 2u);
}

TEST(CASRefCowMap, MaterializeEmptyOverlayIsANoOpEvenWhenUniquelyOwned)
{
    RefCowMap m;
    m.emplace("a", row(1, 1, 1));
    m.materialize();
    ASSERT_EQ(m.baseUseCountForTest(), 1);
    const void * base_before = m.baseIdentityForTest();
    m.materialize();   /// overlay already empty: no fold, no reallocation
    EXPECT_EQ(m.baseIdentityForTest(), base_before);
    EXPECT_TRUE(m.contains("a"));
}

TEST(CASRefCowMap, EqualityComparesEffectiveContentsNotInternalLayout)
{
    RefCowMap a;
    a.emplace("x", row(1, 1, 1));
    a.materialize();   /// "x" lives in `base`

    RefCowMap b;
    b.emplace("x", row(1, 1, 1));   /// same logical content, but lives entirely in `overlay`

    EXPECT_EQ(a.overlayEntriesForTest(), 0u);
    EXPECT_GT(b.overlayEntriesForTest(), 0u);
    EXPECT_TRUE(a == b);
}

/// ===================================================================================
/// Copy-on-write isolation + O(1)-copy assertion (spec §Correctness & testing)
/// ===================================================================================

TEST(CASRefCowMap, CopyIsIsolatedFromOriginal)
{
    RefCowMap original;
    original.emplace("a", row(1, 1, 1));
    original.materialize();

    RefCowMap copy = original;
    copy.insert_or_assign("a", row(9, 9, 9));
    copy.emplace("b", row(9, 9, 9));

    EXPECT_EQ(original.at("a").manifest_ref, (ManifestRef{1, 1, 1}));
    EXPECT_FALSE(original.contains("b"));

    EXPECT_EQ(copy.at("a").manifest_ref, (ManifestRef{9, 9, 9}));
    EXPECT_TRUE(copy.contains("b"));
}

TEST(CASRefCowMap, CopySharesBaseUntilEitherSideMaterializesANewOne)
{
    RefCowMap original;
    original.emplace("a", row(1, 1, 1));
    original.materialize();

    RefCowMap copy = original;
    /// A copy shares the SAME base object (refcount bump, no per-row allocation) until a write
    /// forces a new base into existence via `materialize()` (spec §Mechanism: "Copy = O(1)").
    EXPECT_EQ(original.baseUseCountForTest(), 2);
    EXPECT_EQ(copy.baseUseCountForTest(), 2);

    copy.insert_or_assign("a", row(2, 2, 2));   /// writes go to `copy`'s overlay; `base` is untouched
    EXPECT_EQ(original.baseUseCountForTest(), 2);
    EXPECT_EQ(copy.baseUseCountForTest(), 2);

    copy.materialize();   /// NOW `copy` points at a fresh base of its own
    EXPECT_EQ(original.baseUseCountForTest(), 1);
    EXPECT_EQ(copy.baseUseCountForTest(), 1);
}

/// ===================================================================================
/// Randomized exactness property test: RefCowMap must behave IDENTICALLY to
/// std::map<String, RefCommittedRow> across randomized op sequences (spec §Correctness &
/// testing: "random op sequences ... including copy-then-mutate isolation ... and
/// tombstone/override correctness on the merged iterator").
/// ===================================================================================

TEST(CASRefCowMap, PropertyMatchesStdMapOverRandomOps)
{
    std::mt19937 rng(20260717); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.

    for (int trial = 0; trial < 50; ++trial)
    {
        RefCowMap actual;
        std::map<String, RefCommittedRow> oracle;

        for (int step = 0; step < 200; ++step)
        {
            const String key = "ref" + std::to_string(rng() % 12);
            const uint32_t action = rng() % 6;
            switch (action)
            {
                case 0:   /// emplace
                {
                    RefCommittedRow r = row(1, static_cast<uint64_t>(step) + 1, 1);
                    const bool oracle_inserted = oracle.emplace(key, r).second;
                    const bool actual_inserted = actual.emplace(key, r).second;
                    EXPECT_EQ(oracle_inserted, actual_inserted) << "trial " << trial << " step " << step;
                    break;
                }
                case 1:   /// insert_or_assign
                {
                    RefCommittedRow r = row(2, static_cast<uint64_t>(step) + 1, 2);
                    oracle[key] = r;
                    actual.insert_or_assign(key, r);
                    break;
                }
                case 2:   /// erase by key
                {
                    const size_t oracle_erased = oracle.erase(key);
                    const size_t actual_erased = actual.erase(key);
                    EXPECT_EQ(oracle_erased, actual_erased) << "trial " << trial << " step " << step;
                    break;
                }
                case 3:   /// find/contains/at (read-only)
                {
                    EXPECT_EQ(oracle.contains(key), actual.contains(key)) << "trial " << trial << " step " << step;
                    if (oracle.contains(key))
                        EXPECT_EQ(oracle.at(key), actual.at(key)) << "trial " << trial << " step " << step;
                    break;
                }
                case 4:   /// erase via a found iterator
                {
                    if (auto it = actual.find(key); it != actual.end())
                    {
                        oracle.erase(key);
                        actual.erase(it);
                    }
                    break;
                }
                case 5:   /// materialize -- must not change observable content
                {
                    actual.materialize();
                    break;
                }
                default:
                    UNREACHABLE();
            }

            ASSERT_EQ(oracle.size(), actual.size()) << "trial " << trial << " step " << step;

            auto oit = oracle.begin();
            auto ait = actual.begin();
            for (; oit != oracle.end() && ait != actual.end(); ++oit, ++ait)
            {
                ASSERT_EQ(oit->first, ait->first) << "trial " << trial << " step " << step;
                ASSERT_EQ(oit->second, ait->second) << "trial " << trial << " step " << step;
            }
            ASSERT_TRUE(oit == oracle.end()) << "trial " << trial << " step " << step;
            ASSERT_TRUE(ait == actual.end()) << "trial " << trial << " step " << step;
        }
    }
}

/// ===================================================================================
/// Fast-vs-forced-slow materialize parity (E5 xhigh review): the in-place fold (uniquely-owned base)
/// and the build-fresh-and-swap fold (a copy still shares the base) must produce IDENTICAL merged
/// content, size, and empty overlay across randomized op sequences. This pins that the two code paths
/// -- which handle `net_delta`, tombstones, and overrides differently -- never diverge.
/// ===================================================================================

TEST(CASRefCowMap, FastAndForcedSlowMaterializeAgreeOverRandomOps)
{
    std::mt19937 rng(20260722); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible coverage.

    for (int trial = 0; trial < 60; ++trial)
    {
        RefCowMap fast;   /// never copied -> `materialize` always takes the in-place (uniquely-owned) path
        RefCowMap slow;   /// a live copy is held across each `materialize` -> forced fresh-base path

        for (int step = 0; step < 150; ++step)
        {
            const String key = "ref" + std::to_string(rng() % 10);
            switch (rng() % 5)
            {
                case 0:
                {
                    RefCommittedRow r = row(1, static_cast<uint64_t>(step) + 1, 1);
                    fast.emplace(key, r);
                    slow.emplace(key, r);
                    break;
                }
                case 1:
                {
                    RefCommittedRow r = row(2, static_cast<uint64_t>(step) + 1, 2);
                    fast.insert_or_assign(key, r);
                    slow.insert_or_assign(key, r);
                    break;
                }
                case 2:
                {
                    fast.erase(key);
                    slow.erase(key);
                    break;
                }
                case 3:   /// materialize both, each via its intended path
                {
                    ASSERT_EQ(fast.baseUseCountForTest(), 1) << "fast map must be uniquely owned";
                    fast.materialize();   /// in-place fast path
                    {
                        RefCowMap pin = slow;   /// shares slow's base
                        ASSERT_EQ(slow.baseUseCountForTest(), 2) << "slow map must be forced onto the copy path";
                        slow.materialize();     /// build-fresh-and-swap slow path
                    }
                    EXPECT_EQ(fast.overlayEntriesForTest(), 0u) << "trial " << trial << " step " << step;
                    EXPECT_EQ(slow.overlayEntriesForTest(), 0u) << "trial " << trial << " step " << step;
                    break;
                }
                default:
                    break;   /// accumulate overlay without materializing
            }

            /// Content + size parity holds at EVERY step, materialized or not.
            ASSERT_EQ(fast.size(), slow.size()) << "trial " << trial << " step " << step;
            auto fi = fast.begin();
            auto si = slow.begin();
            for (; fi != fast.end() && si != slow.end(); ++fi, ++si)
            {
                ASSERT_EQ(fi->first, si->first) << "trial " << trial << " step " << step;
                ASSERT_EQ(fi->second, si->second) << "trial " << trial << " step " << step;
            }
            ASSERT_TRUE(fi == fast.end() && si == slow.end()) << "trial " << trial << " step " << step;
        }

        /// A final materialize of both via their two paths must leave identical, fully-folded state.
        fast.materialize();
        {
            RefCowMap pin = slow;
            slow.materialize();
        }
        EXPECT_EQ(fast.overlayEntriesForTest(), 0u) << "trial " << trial;
        EXPECT_EQ(slow.overlayEntriesForTest(), 0u) << "trial " << trial;
        EXPECT_TRUE(fast == slow) << "trial " << trial;
        EXPECT_EQ(fast.size(), slow.size()) << "trial " << trial;
    }
}
