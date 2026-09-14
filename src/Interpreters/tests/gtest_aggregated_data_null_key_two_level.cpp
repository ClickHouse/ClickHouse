#include <gtest/gtest.h>

#include <Common/HashTable/HashTableTraits.h>
#include <Interpreters/AggregatedData.h>

using namespace DB;

/// Size hints reach the types the trait accepts. The nullable two-level variants build on a bare
/// `TwoLevelHashTable`, which the trait does not cover, so `AggregatedDataVariants::init` creates them
/// without a hint; a specialization that matched them would change that.
static_assert(HasConstructorOfNumberOfElements<AggregatedDataWithUInt64KeyTwoLevel>::value);
static_assert(HasConstructorOfNumberOfElements<AggregatedDataWithUInt64KeyVoidTwoLevel>::value);
static_assert(!HasConstructorOfNumberOfElements<AggregatedDataWithNullableUInt64KeyVoidTwoLevel>::value);

TEST(AggregationDataWithNullKeyTwoLevel, ConvertingConstructorKeepsTheNullKeyGroup)
{
    /// `convertToTwoLevel` builds the two-level variant from the single-level one through this constructor.
    constexpr UInt64 num_keys = 1000;
    char null_group_state = 0;

    AggregatedDataWithNullableUInt64KeyVoid single_level;
    single_level.hasNullKeyData() = true;
    single_level.getNullKeyData() = &null_group_state;
    for (UInt64 key = 1; key <= num_keys; ++key)
        single_level.insert(key);

    AggregatedDataWithNullableUInt64KeyVoidTwoLevel two_level(single_level);

    ASSERT_TRUE(two_level.hasNullKeyData());
    ASSERT_EQ(two_level.getNullKeyData(), &null_group_state);
    ASSERT_EQ(two_level.size(), single_level.size());
    for (UInt64 key = 1; key <= num_keys; ++key)
        ASSERT_NE(two_level.find(key), nullptr) << "key " << key;
    ASSERT_EQ(two_level.find(num_keys + 1), nullptr);
}
