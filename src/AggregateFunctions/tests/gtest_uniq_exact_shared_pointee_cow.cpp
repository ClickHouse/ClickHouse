#include <AggregateFunctions/UniqExactSet.h>
#include <Common/HashTable/HashSet.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <vector>

using namespace DB;

namespace
{
using Key = UInt64;
constexpr size_t initial_size_degree = 4;
using SingleLevelSet = HashSetWithStackMemory<Key, HashCRC32<Key>, initial_size_degree>;
using TwoLevelSet = TwoLevelHashSet<Key, HashCRC32<Key>>;
using TestSet = UniqExactSet<SingleLevelSet, TwoLevelSet>;

void fillSet(TestSet & set, size_t start, size_t count)
{
    for (size_t i = start; i < start + count; ++i)
    {
        Key key = i;
        set.insert<Key, SetLevelHint::unknown>(std::move(key));
    }
}

/// Extract the exact, sorted key set through the public serialization interface. Both levels write the
/// same format (varint count, then raw keys; see `HashTable::write` and `writeAsSingleLevel`).
std::vector<Key> collectKeys(const TestSet & set)
{
    WriteBufferFromOwnString out;
    set.write(out);

    ReadBufferFromString in(out.str());
    size_t count = 0;
    readVarUInt(count, in);
    std::vector<Key> keys(count);
    for (auto & key : keys)
        readBinaryLittleEndian(key, in);

    std::sort(keys.begin(), keys.end());
    return keys;
}

std::unique_ptr<TestSet> makeTwoLevel(size_t start, size_t count)
{
    auto set = std::make_unique<TestSet>();
    fillSet(*set, start, count);
    set->convertToTwoLevel();
    return set;
}

/// Identity of the two-level pointee a `UniqExactSet` currently owns, as an opaque token. `getTwoLevelSet()`
/// returns `shared_ptr<TwoLevelSet>` before the fix and `shared_ptr<SharedTwoLevelSet>` after it; taking `.get()`
/// as `const void *` compiles against both, and identity (not contents) is all this test compares.
const void * pointeeAddress(const TestSet & set)
{
    return set.getTwoLevelSet().get();
}
}

/// Regression for the ThreadSanitizer data race STID 1464-31ed in `uniqExact` parallel merge (issue #108912).
///
/// The race: the two-level pointee P of a `UniqExactSet` is shared across several `UniqExactSet` objects (the
/// merge fast path adopts one P into an empty destination; `parallelizeMergeMulti` pre-fetches raw
/// `TwoLevelSet *` to every state and reads their buckets across threads). Before the fix, `doDeepCopyIfNeeded`
/// forked P only when `shared_ptr::use_count() > 1`. `use_count()` is not a synchronization primitive: when a
/// sibling holder is destroyed (count 2 -> 1) with no happens-before against P's memory, a state can observe
/// `use_count == 1` and write P's buckets in place while another thread is still reading the same P.
///
/// This is a DIRECT, deterministic test of that lifetime transition, not a concurrent one, and that is
/// deliberate. A concurrent reader cannot form a valid fails-before/passes-after gate here: any holder that
/// keeps P alive for the reader also contributes to `use_count()`, so the buggy `use_count() > 1` guard would
/// fork too and the pre-fix code would pass; dropping that holder to force `use_count == 1` means the fix (which
/// forks and releases the sole-owned P) frees P under the raw reader -> use-after-free on the passes-after run.
/// So the necessary and sufficient property is asserted directly: once P has escaped to another `UniqExactSet`
/// and a sibling holder is dropped (count 2 -> 1), a mutating merge into the surviving state must FORK P (leave
/// the escaped instance untouched for any in-flight reader), never mutate it in place.
///
/// Observable proxy for the fork-vs-in-place decision is the pointee address: an in-place mutation keeps the
/// same address; a fork installs a freshly allocated copy whose address, allocated while P is still alive, is
/// necessarily distinct from P's. Before the fix the merge mutates P in place -> address unchanged -> `EXPECT_NE`
/// fails; after the fix it forks -> new address -> `EXPECT_NE` passes. The captured address is only ever compared
/// as an opaque token, never dereferenced, so the fork releasing the old pointee is fine.
TEST(UniqExactSharedPointeeCoW, ForksEscapedPointeeAfterUseCountDrop)
{
    constexpr size_t P_N = 130'000;    /// > worthConvertingToTwoLevel threshold (100k), so the fast-path adopt fires

    auto writer = std::make_unique<TestSet>();
    fillSet(*writer, 0, P_N);
    writer->convertToTwoLevel();
    ASSERT_TRUE(writer->isTwoLevel());

    const void * escaped_pointee = nullptr;
    {
        /// Fast-path adopt: empty `sibling` merges `writer`, so `sibling.two_level_set = writer.getTwoLevelSet()`
        /// and `sibling` ends up sharing writer's pointee P, exactly as merging one state into an empty
        /// ROLLUP / CUBE / GROUPING SETS destination does. This is the production path that lets P escape to a
        /// second `UniqExactSet` (and, in the fixed code, marks P shared). P's use_count is now 2.
        TestSet sibling;
        sibling.merge(*writer);
        ASSERT_TRUE(sibling.isTwoLevel());
        escaped_pointee = pointeeAddress(sibling);
        ASSERT_EQ(pointeeAddress(*writer), escaped_pointee);   /// writer and sibling share the same P
    }
    /// `sibling` destroyed: P's use_count drops 2 -> 1 while, in production, `parallelizeMergeMulti`'s
    /// pre-fetched raw `TwoLevelSet *` to P is still in flight. This is the exact transition of STID 1464-31ed.

    auto other = makeTwoLevel(P_N * 4, P_N);   /// disjoint keys [520000, 650000), two-level

    /// Production write path: merge -> asTwoLevelChecked() -> doDeepCopyIfNeeded() decides fork vs in-place.
    writer->merge(*other);

    /// Fails before the fix (in-place mutation of the escaped P -> same address), passes after it (fork).
    EXPECT_NE(pointeeAddress(*writer), escaped_pointee);

    /// The (forked) set still holds P's keys plus the newly merged disjoint keys.
    EXPECT_EQ(writer->size(), 2 * P_N);
}

/// Regression for the prefetched merge-into path (`HashSetTable::mergeInto`): merging FROM a shared (adopted)
/// pointee must leave the source pointee untouched. `mergeInto` is a const member that iterates the source
/// strictly read-only; this test pins that no in-place source mutation (or `const_cast` shortcut) sneaks into
/// any of the merge routing branches.
///
/// Shape: P escapes via the merge fast path into an empty destination (adoption, which marks P shared), then
/// two distinct non-empty destinations merge from the same shared P one after the other — the sequential shape
/// of ROLLUP / CUBE / GROUPING SETS merging one source state into several destinations, possibly concurrently.
/// Afterwards P's identity, size, and exact key set (zero key included) must be unchanged in both holders, and
/// each destination must hold the exact expected union.
TEST(UniqExactSharedPointeeCoW, SharedSourceReadOnlyAcrossTwoDestinationMerges)
{
    constexpr size_t P_N = 130'000;   /// > worthConvertingToTwoLevel threshold (100k)

    TestSet source;
    {
        Key zero = 0;
        source.insert<Key, SetLevelHint::unknown>(std::move(zero));   /// the shared pointee also carries the zero key
    }
    fillSet(source, 1, P_N);   /// [1, P_N + 1)
    source.convertToTwoLevel();
    ASSERT_TRUE(source.isTwoLevel());

    /// Adoption fast path: an empty destination adopts P, which marks P shared.
    TestSet adopter;
    adopter.merge(source);
    ASSERT_TRUE(adopter.isTwoLevel());
    const void * shared_pointee = pointeeAddress(source);
    ASSERT_EQ(pointeeAddress(adopter), shared_pointee);   /// aliasing: both states hold the same P

    const auto source_keys_before = collectKeys(source);
    ASSERT_EQ(source.size(), P_N + 1);

    /// Two non-empty two-level destinations merge from the same shared P, one after the other.
    auto dst_a = makeTwoLevel(P_N * 2, P_N);   /// [260000, 390000), disjoint from P
    auto dst_b = makeTwoLevel(P_N * 4, P_N);   /// [520000, 650000), disjoint from P
    dst_a->merge(source);
    dst_b->merge(source);

    /// Each destination holds exactly its own keys plus all of P's, zero included.
    std::vector<Key> expected_a;
    expected_a.reserve(2 * P_N + 1);
    expected_a.push_back(0);
    for (Key k = 1; k < P_N + 1; ++k)
        expected_a.push_back(k);
    for (Key k = P_N * 2; k < P_N * 3; ++k)
        expected_a.push_back(k);

    std::vector<Key> expected_b;
    expected_b.reserve(2 * P_N + 1);
    expected_b.push_back(0);
    for (Key k = 1; k < P_N + 1; ++k)
        expected_b.push_back(k);
    for (Key k = P_N * 4; k < P_N * 5; ++k)
        expected_b.push_back(k);

    EXPECT_EQ(dst_a->size(), 2 * P_N + 1);
    EXPECT_EQ(dst_b->size(), 2 * P_N + 1);
    EXPECT_EQ(collectKeys(*dst_a), expected_a);
    EXPECT_EQ(collectKeys(*dst_b), expected_b);

    /// The shared source is undisturbed: same pointee in both holders, same size, same exact keys.
    EXPECT_EQ(pointeeAddress(source), shared_pointee);
    EXPECT_EQ(pointeeAddress(adopter), shared_pointee);
    EXPECT_EQ(source.size(), P_N + 1);
    EXPECT_EQ(adopter.size(), P_N + 1);
    EXPECT_EQ(collectKeys(source), source_keys_before);
}
