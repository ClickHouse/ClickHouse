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

/// Craft a serialized state with the given raw (already hashed and thinned out) values,
/// to test the states of cardinalities that would be too slow to reach by actual insertion.
/// The wide set values are appended when present.
std::string craftState(
    UInt8 skip_degree,
    const std::vector<UInt32> & values,
    bool use_legacy_format,
    UInt8 wide_skip_degree = 0,
    const std::vector<UInt64> & wide_values = {})
{
    WriteBufferFromOwnString out;
    if (!use_legacy_format)
    {
        UInt8 flags = wide_values.empty() ? 0 : 1;
        writeBinaryLittleEndian(flags, out);
    }
    writeBinaryLittleEndian(skip_degree, out);
    writeVarUInt(values.size(), out);
    for (UInt32 value : values)
        writeBinaryLittleEndian(value, out);

    if (!use_legacy_format && !wide_values.empty())
    {
        writeBinaryLittleEndian(wide_skip_degree, out);
        writeVarUInt(wide_values.size(), out);
        for (UInt64 value : wide_values)
            writeBinaryLittleEndian(value, out);
    }
    return out.str();
}

/// Distinct pseudo-random values with the given number of zero low-order bits.
template <typename StoredValue>
std::vector<StoredValue> randomSampledValues(size_t count, UInt8 skip_degree, UInt64 seed)
{
    std::mt19937_64 rng(seed);
    std::unordered_set<StoredValue> values;
    while (values.size() < count)
    {
        StoredValue value = static_cast<StoredValue>(rng() << skip_degree);
        if (value != 0)
            values.insert(value);
    }
    return std::vector<StoredValue>(values.begin(), values.end());
}

}


TEST(UniquesHashSet, ExactAtSmallCardinality)
{
    TestSet set;
    for (UInt64 i = 0; i < 50000; ++i)
        set.insert(i);

    /// Below the thinning threshold the set stores all distinct 32-bit hashes.
    /// Among 50000 of them, an occasional collision is possible.
    EXPECT_GE(set.size(), 49998u);
    EXPECT_LE(set.size(), 50000u);
}

TEST(UniquesHashSet, EstimateAtModerateCardinality)
{
    TestSet set;
    for (UInt64 i = 0; i < 10000000; ++i)
        set.insert(i);

    /// The typical error of the estimate is about 0.6%; allow 3%.
    EXPECT_NEAR(static_cast<double>(set.size()), 10000000.0, 300000.0);

    /// The wide set holds about one value per 262144 distinct elements.
    EXPECT_GE(set.wideSetSize(), 20u);
    EXPECT_LE(set.wideSetSize(), 60u);
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
    EXPECT_GE(a.size(), 59997u);
    EXPECT_LE(a.size(), 60000u);
}

TEST(UniquesHashSet, RoundTripNewFormat)
{
    TestSet set;
    for (UInt64 i = 0; i < 1000000; ++i)
        set.insert(i);

    std::string serialized = serialize(set, /*use_legacy_format=*/ false);
    TestSet restored = deserialize(serialized, /*use_legacy_format=*/ false);

    EXPECT_EQ(set.size(), restored.size());
    EXPECT_EQ(set.wideSetSize(), restored.wideSetSize());

    /// The state must remain usable after the round trip.
    for (UInt64 i = 1000000; i < 1100000; ++i)
        restored.insert(i);
    EXPECT_NEAR(static_cast<double>(restored.size()), 1100000.0, 33000.0);
}

TEST(UniquesHashSet, LegacyFormatIsThePlainMainSet)
{
    TestSet set;
    for (UInt64 i = 0; i < 1000000; ++i)
        set.insert(i);

    /// The new format is a flags byte, the main set in the legacy layout, then the wide set.
    std::string legacy = serialize(set, /*use_legacy_format=*/ true);
    std::string versioned = serialize(set, /*use_legacy_format=*/ false);
    EXPECT_EQ(versioned[0], 1);
    EXPECT_EQ(versioned.substr(1, legacy.size()), legacy);
    EXPECT_GT(versioned.size(), 1 + legacy.size());

    /// A state without the wide set differs only by the flags byte.
    TestSet small;
    for (UInt64 i = 0; i < 100; ++i)
        small.insert(i);
    if (small.wideSetSize() == 0)
    {
        std::string small_legacy = serialize(small, /*use_legacy_format=*/ true);
        std::string small_versioned = serialize(small, /*use_legacy_format=*/ false);
        EXPECT_EQ(small_versioned[0], 0);
        EXPECT_EQ(small_versioned.substr(1), small_legacy);
    }

    /// The legacy round trip drops the wide set but keeps the main one.
    TestSet restored = deserialize(legacy, /*use_legacy_format=*/ true);
    EXPECT_EQ(restored.wideSetSize(), 0u);
    EXPECT_EQ(restored.size(), set.size());
}

TEST(UniquesHashSet, HugeCardinalityViaWideSet)
{
    /// Simulate the state left by 10^11 distinct elements: the wide set thinned out to the degree 23
    /// keeps about 10^11 / 2^23 values; the main set is saturated and unusable.
    constexpr UInt64 cardinality = 100000000000ULL;
    constexpr UInt8 wide_skip_degree = 23;
    constexpr size_t num_wide = cardinality >> wide_skip_degree;

    /// (There are only 65535 nonzero multiples of 2^16 among the 32-bit values, so 65000 is close
    /// to the saturation of the main sample.)
    auto main_values = randomSampledValues<UInt32>(65000, 16, 1);
    auto wide_values = randomSampledValues<UInt64>(num_wide, wide_skip_degree, 2);

    TestSet set = deserialize(craftState(16, main_values, false, wide_skip_degree, wide_values), false);

    /// Before the wide set was introduced, the estimate overflowed to values around 1.8 * 10^19 here.
    EXPECT_NEAR(static_cast<double>(set.size()), static_cast<double>(cardinality), 0.001 * cardinality);
}

TEST(UniquesHashSet, LegacyEstimateSaturatesInsteadOfOverflow)
{
    /// A fully saturated legacy state: at the thinning degree of 16 every possible sampled
    /// 32-bit hash (all multiples of 2^16) is present. The corrected estimate used to be
    /// computed as round(2^32 * log(2^32 / 0)) = inf, and casting it to an integer
    /// was undefined behavior that produced values like 18446743978444128518.
    std::vector<UInt32> values(65536);
    for (size_t i = 1; i < values.size(); ++i)
        values[i] = static_cast<UInt32>(i << 16);
    values[0] = 0;

    TestSet set = deserialize(craftState(16, values, /*use_legacy_format=*/ true), /*use_legacy_format=*/ true);

    /// The saturated estimate is 2^32 * ln(2^32), about 9.5 * 10^10.
    size_t estimate = set.size();
    EXPECT_GE(estimate, 90000000000ULL);
    EXPECT_LE(estimate, 100000000000ULL);
}

TEST(UniquesHashSet, WideSetsMergeAndThinOut)
{
    constexpr UInt8 wide_skip_degree = 20;
    auto main_values = randomSampledValues<UInt32>(60000, 16, 3);

    /// Each state alone is below the cap of the wide set; their union is above it,
    /// so the merge has to thin the wide set out.
    auto a_wide = randomSampledValues<UInt64>(12000, wide_skip_degree, 4);
    auto b_wide = randomSampledValues<UInt64>(12000, wide_skip_degree, 5);

    TestSet a = deserialize(craftState(16, main_values, false, wide_skip_degree, a_wide), false);
    TestSet b = deserialize(craftState(16, main_values, false, wide_skip_degree, b_wide), false);

    double a_estimate = static_cast<double>(a.size());
    EXPECT_NEAR(a_estimate, 12000.0 * exp2(wide_skip_degree), 0.001 * a_estimate);

    a.merge(b);

    /// Random 64-bit values practically do not overlap, so the union holds about 24000 values.
    double expected = 24000.0 * exp2(wide_skip_degree);
    EXPECT_NEAR(static_cast<double>(a.size()), expected, 0.03 * expected);

    /// The other merge direction gives the same estimate.
    TestSet a2 = deserialize(craftState(16, main_values, false, wide_skip_degree, a_wide), false);
    TestSet b2 = deserialize(craftState(16, main_values, false, wide_skip_degree, b_wide), false);
    b2.merge(a2);
    EXPECT_EQ(b2.size(), a.size());
}

TEST(UniquesHashSet, MergingLegacyStateDropsTheWideSet)
{
    constexpr UInt8 wide_skip_degree = 20;
    auto main_values = randomSampledValues<UInt32>(50000, 16, 6);
    auto wide_values = randomSampledValues<UInt64>(10000, wide_skip_degree, 7);

    TestSet with_wide = deserialize(craftState(16, main_values, false, wide_skip_degree, wide_values), false);
    TestSet legacy = deserialize(craftState(16, randomSampledValues<UInt32>(30000, 16, 8), true), true);

    EXPECT_TRUE(legacy.isWideIncomplete());
    EXPECT_FALSE(with_wide.isWideIncomplete());

    /// A legacy state carries no wide set: its elements are known by 32 bits of the hash only,
    /// so they cannot contribute to the wide set of the merged state. An estimate from a wide set
    /// that covers only a part of the elements would lose the rest, so the merged state drops
    /// the wide set altogether and falls back to the 32-bit estimator.
    with_wide.merge(legacy);
    EXPECT_TRUE(with_wide.isWideIncomplete());
    EXPECT_EQ(with_wide.wideSetSize(), 0u);

    /// And it stays incomplete: the elements consumed later are not enough to make the sample whole.
    for (UInt64 i = 0; i < 1000000; ++i)
        with_wide.insert(i);
    EXPECT_EQ(with_wide.wideSetSize(), 0u);

    /// The mark travels with the state: a version-1 state whose wide sample is incomplete
    /// says so in bit 1 of its flags byte, so a reader does not start estimating from a wide set
    /// that it would build after the incomplete elements were already consumed.
    std::string serialized = serialize(with_wide, /*use_legacy_format=*/ false);
    EXPECT_EQ(serialized[0], 2);
    EXPECT_TRUE(deserialize(serialized, /*use_legacy_format=*/ false).isWideIncomplete());

    /// An empty legacy state contributes nothing, so it does not spoil the wide set.
    TestSet clean = deserialize(craftState(16, main_values, false, wide_skip_degree, wide_values), false);
    clean.merge(deserialize(craftState(0, {}, true), true));
    EXPECT_FALSE(clean.isWideIncomplete());
    EXPECT_EQ(clean.wideSetSize(), wide_values.size());

    /// Nor does a later merge with a clean wide state resurrect the sample: an incomplete state
    /// stays without a wide set even when the other side's thinning is coarser than the initial
    /// one (this used to allocate an empty wide set that nothing could fill, making the state
    /// serialize with both flag bits - which the format defines as mutually exclusive).
    with_wide.merge(clean);
    EXPECT_TRUE(with_wide.isWideIncomplete());
    EXPECT_EQ(with_wide.wideSetSize(), 0u);
    serialized = serialize(with_wide, /*use_legacy_format=*/ false);
    EXPECT_EQ(serialized[0], 2);
}

TEST(UniquesHashSet, MergingLegacyStateDoesNotLoseItsElements)
{
    /** The rolling-upgrade case: one shard is new and reports a version-1 state for about
      * three billion distinct values, the other is old and reports a version-0 state for another
      * disjoint three billion. The merged estimate has to account for both: taking it from
      * the wide set - which only ever saw the new shard's half - would report three billion.
      * The 32-bit sample saw both halves (that is what the random overlap of the crafted
      * values models), so it is the one to use.
      */
    constexpr UInt8 wide_skip_degree = 18;
    constexpr size_t sampled_main_values = 32950;   /// 3e9 distinct values sampled at 2 ^ 16.
    constexpr size_t sampled_wide_values = 11444;   /// The same 3e9 sampled at 2 ^ 18.

    TestSet versioned = deserialize(
        craftState(
            16, randomSampledValues<UInt32>(sampled_main_values, 16, 11), false,
            wide_skip_degree, randomSampledValues<UInt64>(sampled_wide_values, wide_skip_degree, 12)),
        false);

    TestSet legacy = deserialize(craftState(16, randomSampledValues<UInt32>(sampled_main_values, 16, 13), true), true);

    /// Above the switch threshold the version-1 state alone is estimated from the wide set.
    EXPECT_GE(versioned.wideSetSize(), 8192u);
    EXPECT_GT(versioned.size(), 2500000000ULL);
    EXPECT_LT(versioned.size(), 3500000000ULL);

    versioned.merge(legacy);

    EXPECT_GT(versioned.size(), 5000000000ULL);
    EXPECT_LT(versioned.size(), 7000000000ULL);
}

TEST(UniquesHashSet, InsertPathFeedsTheWideSet)
{
    /// Find values whose 64-bit hashes pass the wide thinning.
    std::vector<UInt64> passing;
    for (UInt64 candidate = 0; passing.size() < 5; ++candidate)
        if ((DefaultHash<UInt64>()(candidate) & ((1 << 18) - 1)) == 0)
            passing.push_back(candidate);

    TestSet set;
    for (UInt64 i = 0; i < 100; ++i)
        set.insert(i);
    for (UInt64 candidate : passing)
    {
        set.insert(candidate);
        set.insert(candidate);   /// The wide set deduplicates.
    }

    EXPECT_GE(set.wideSetSize(), 5u);

    std::string serialized = serialize(set, false);
    EXPECT_EQ(serialized[0], 1);
    TestSet restored = deserialize(serialized, false);
    EXPECT_EQ(restored.wideSetSize(), set.wideSetSize());
    EXPECT_EQ(restored.size(), set.size());
}

TEST(UniquesHashSet, RejectsCorruptedStates)
{
    {
        /// Unknown flags.
        WriteBufferFromOwnString out;
        writeBinaryLittleEndian(static_cast<UInt8>(4), out);
        std::string data = out.str();
        ReadBufferFromString in(data);
        TestSet set;
        EXPECT_THROW(set.read(in, /*use_legacy_format=*/ false), Poco::Exception);
    }
    {
        /// The two defined flag bits are mutually exclusive: a state either carries a wide set
        /// or says that its wide sample is incomplete, never both.
        WriteBufferFromOwnString out;
        writeBinaryLittleEndian(static_cast<UInt8>(3), out);
        std::string data = out.str();
        ReadBufferFromString in(data);
        TestSet set;
        EXPECT_THROW(set.read(in, /*use_legacy_format=*/ false), Poco::Exception);
    }
    {
        /// Too large wide set skip degree.
        std::string data = craftState(0, {}, false, 64, {1ULL << 40});
        ReadBufferFromString in(data);
        TestSet set;
        EXPECT_THROW(set.read(in, /*use_legacy_format=*/ false), Poco::Exception);
    }
    {
        /// Too large wide set size.
        std::vector<UInt64> too_many(20000, 0);
        for (size_t i = 0; i < too_many.size(); ++i)
            too_many[i] = (i + 1) << 20;
        std::string data = craftState(0, {}, false, 20, too_many);
        ReadBufferFromString in(data);
        TestSet set;
        EXPECT_THROW(set.read(in, /*use_legacy_format=*/ false), Poco::Exception);
    }
}
