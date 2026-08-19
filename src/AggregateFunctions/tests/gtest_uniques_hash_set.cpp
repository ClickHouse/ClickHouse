#include <AggregateFunctions/UniquesHashSet.h>
#include <Common/HashTable/Hash.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <random>
#include <unordered_set>

using namespace DB;

namespace
{

/// The same concrete type that the `uniq` aggregate function uses.
using TestSet = UniquesHashSet<DefaultHash<UInt64>>;

std::string serialize(const TestSet & set, bool use_legacy_format)
{
    WriteBufferFromOwnString out;
    set.write(out, use_legacy_format);
    return out.str();
}

TestSet deserialize(const std::string & data, bool use_legacy_format)
{
    ReadBufferFromString in(data);
    TestSet set;
    set.read(in, use_legacy_format);
    return set;
}

/// Craft a serialized state with the given skip degree and raw (already hashed) sampled values,
/// to test the estimate at cardinalities that would be too slow to reach by actual insertion.
template <typename StoredValue>
std::string craftState(UInt8 skip_degree, const std::vector<StoredValue> & values, bool use_legacy_format)
{
    WriteBufferFromOwnString out;
    if (!use_legacy_format)
    {
        UInt8 flags = sizeof(StoredValue) == sizeof(UInt32) ? 1 : 0;
        writeBinaryLittleEndian(flags, out);
    }
    writeBinaryLittleEndian(skip_degree, out);
    writeVarUInt(values.size(), out);
    for (StoredValue value : values)
        writeBinaryLittleEndian(value, out);
    return out.str();
}

}


TEST(UniquesHashSet, ExactAtSmallCardinality)
{
    TestSet set;
    for (UInt64 i = 0; i < 50000; ++i)
        set.insert(i);

    /// Below the thinning threshold the set stores all distinct 64-bit hashes,
    /// and collisions among 50000 of them are next to impossible.
    EXPECT_EQ(set.size(), 50000u);
}

TEST(UniquesHashSet, EstimateAtModerateCardinality)
{
    TestSet set;
    for (UInt64 i = 0; i < 10000000; ++i)
        set.insert(i);

    /// The typical error of the estimate is about 0.6%; allow 3%.
    EXPECT_NEAR(static_cast<double>(set.size()), 10000000.0, 300000.0);
}

TEST(UniquesHashSet, MergeDeduplicates)
{
    TestSet a;
    TestSet b;
    for (UInt64 i = 0; i < 40000; ++i)
        a.insert(i);
    for (UInt64 i = 20000; i < 60000; ++i)
        b.insert(i);

    a.merge(b);
    EXPECT_EQ(a.size(), 60000u);
}

TEST(UniquesHashSet, RoundTripNewFormat)
{
    TestSet set;
    for (UInt64 i = 0; i < 300000; ++i)
        set.insert(i);

    std::string serialized = serialize(set, /*use_legacy_format=*/ false);
    TestSet restored = deserialize(serialized, /*use_legacy_format=*/ false);

    EXPECT_EQ(set.size(), restored.size());

    /// The state must remain usable after the round trip.
    for (UInt64 i = 300000; i < 400000; ++i)
        restored.insert(i);
    EXPECT_NEAR(static_cast<double>(restored.size()), 400000.0, 12000.0);
}

TEST(UniquesHashSet, RoundTripLegacyFormat)
{
    TestSet set;
    for (UInt64 i = 0; i < 300000; ++i)
        set.insert(i);

    std::string legacy = serialize(set, /*use_legacy_format=*/ true);
    TestSet restored = deserialize(legacy, /*use_legacy_format=*/ true);

    EXPECT_TRUE(restored.isCompat32());

    /// The estimates of the downgraded and the original state may differ slightly
    /// (only 32 bits of each hash remain), but at this cardinality both are accurate.
    EXPECT_NEAR(static_cast<double>(restored.size()), static_cast<double>(set.size()), 10000.0);

    /// A state restored from the legacy format is written in the new format with the compat flag,
    /// and survives that round trip as well.
    std::string compat = serialize(restored, /*use_legacy_format=*/ false);
    TestSet restored2 = deserialize(compat, /*use_legacy_format=*/ false);
    EXPECT_TRUE(restored2.isCompat32());
    EXPECT_EQ(restored2.size(), restored.size());
}

TEST(UniquesHashSet, MergeWithLegacyStateDeduplicates)
{
    /// Elements 20000..40000 are present on both sides: on the left with full 64-bit hashes,
    /// on the right with truncated ones (after a round trip through the legacy format).
    /// The merge must not double-count them.
    TestSet a;
    for (UInt64 i = 0; i < 40000; ++i)
        a.insert(i);

    TestSet b;
    for (UInt64 i = 20000; i < 60000; ++i)
        b.insert(i);
    TestSet b32 = deserialize(serialize(b, /*use_legacy_format=*/ true), /*use_legacy_format=*/ true);

    a.merge(b32);
    EXPECT_TRUE(a.isCompat32());

    /// The reference: the same union computed with truncated hashes from the start.
    TestSet reference;
    for (UInt64 i = 0; i < 60000; ++i)
        reference.insert(i);
    TestSet reference32 = deserialize(serialize(reference, /*use_legacy_format=*/ true), /*use_legacy_format=*/ true);

    /// At this cardinality the thinning has not started, so the estimate is exactly the number
    /// of distinct 32-bit hashes, and it must match the reference exactly: had the merge failed
    /// to deduplicate the common elements, the estimate would be larger by about 20000.
    EXPECT_EQ(a.size(), reference32.size());

    /// Merging in the other direction (a 64-bit state into a legacy state) gives the same result.
    TestSet a2;
    for (UInt64 i = 0; i < 40000; ++i)
        a2.insert(i);
    TestSet b32_copy = deserialize(serialize(b, /*use_legacy_format=*/ true), /*use_legacy_format=*/ true);
    b32_copy.merge(a2);
    EXPECT_EQ(b32_copy.size(), reference32.size());
}

TEST(UniquesHashSet, InsertIntoLegacyState)
{
    /// Inserting into a state restored from the legacy format must deduplicate
    /// against the values that are already there.
    TestSet set;
    for (UInt64 i = 0; i < 30000; ++i)
        set.insert(i);

    TestSet restored = deserialize(serialize(set, /*use_legacy_format=*/ true), /*use_legacy_format=*/ true);
    for (UInt64 i = 0; i < 60000; ++i)
        restored.insert(i);

    EXPECT_NEAR(static_cast<double>(restored.size()), 60000.0, 100.0);
}

TEST(UniquesHashSet, NoOverflowAtHugeCardinality)
{
    /// Simulate the state left by 10^11 distinct elements: with the thinning degree of 21,
    /// about 10^11 / 2^21 sampled hashes remain, each with 21 zero low-order bits.
    constexpr UInt64 cardinality = 100000000000ULL;
    constexpr UInt8 skip_degree = 21;
    constexpr size_t num_samples = cardinality >> skip_degree;

    std::mt19937_64 rng(42);
    std::unordered_set<UInt64> samples;
    while (samples.size() < num_samples)
        samples.insert(rng() << skip_degree);

    TestSet set = deserialize(
        craftState<UInt64>(skip_degree, std::vector<UInt64>(samples.begin(), samples.end()), /*use_legacy_format=*/ false),
        /*use_legacy_format=*/ false);

    /// Before the switch to 64-bit hashes the estimate overflowed to a value around 1.8 * 10^19 here.
    EXPECT_NEAR(static_cast<double>(set.size()), static_cast<double>(cardinality), 0.001 * cardinality);
}

TEST(UniquesHashSet, LegacyEstimateSaturatesInsteadOfOverflow)
{
    /// A fully saturated legacy state: at the thinning degree of 16 every possible sampled
    /// 32-bit hash (all multiples of 2^16) is present. The corrected estimate used to be
    /// computed as round(2^32 * log(2^32 / 0)) = inf, and casting it to an integer
    /// was undefined behavior that produced values like 18446743978444128518.
    std::vector<UInt32> values(65536);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i << 16);

    TestSet set = deserialize(craftState<UInt32>(16, values, /*use_legacy_format=*/ true), /*use_legacy_format=*/ true);

    /// The saturated estimate is 2^32 * ln(2^32), about 9.5 * 10^10.
    size_t estimate = set.size();
    EXPECT_GE(estimate, 90000000000ULL);
    EXPECT_LE(estimate, 100000000000ULL);
}

TEST(UniquesHashSet, RejectsCorruptedStates)
{
    {
        /// Too large skip degree.
        std::string data = craftState<UInt64>(64, {}, /*use_legacy_format=*/ false);
        ReadBufferFromString in(data);
        TestSet set;
        EXPECT_THROW(set.read(in, /*use_legacy_format=*/ false), Poco::Exception);
    }
    {
        /// Unknown flags.
        WriteBufferFromOwnString out;
        writeBinaryLittleEndian(static_cast<UInt8>(2), out);
        std::string data = out.str();
        ReadBufferFromString in(data);
        TestSet set;
        EXPECT_THROW(set.read(in, /*use_legacy_format=*/ false), Poco::Exception);
    }
}
