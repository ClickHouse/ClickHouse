#pragma once
#include <AggregateFunctions/IAggregateFunction_fwd.h>

#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <Common/HashTable/TwoLevelStringHashMap.h>
namespace DB
{
/** Different data structures that can be used for aggregation
  * For efficiency, the aggregation data itself is put into the pool.
  * Data and pool ownership (states of aggregate functions)
  *  is acquired later - in `convertToBlocks` function, by the ColumnAggregateFunction object.
  *
  * Most data structures exist in two versions: normal and two-level (TwoLevel).
  * A two-level hash table works a little slower with a small number of different keys,
  *  but with a large number of different keys scales better, because it allows
  *  parallelize some operations (merging, post-processing) in a natural way.
  *
  * To ensure efficient work over a wide range of conditions,
  *  first single-level hash tables are used,
  *  and when the number of different keys is large enough,
  *  they are converted to two-level ones.
  *
  * PS. There are many different approaches to the effective implementation of parallel and distributed aggregation,
  *  best suited for different cases, and this approach is just one of them, chosen for a combination of reasons.
  */

using AggregatedDataWithoutKey = AggregateDataPtr;

using AggregatedDataWithUInt8Key = FixedImplicitZeroHashMapWithCalculatedSize<UInt8, AggregateDataPtr>;
using AggregatedDataWithUInt16Key = FixedImplicitZeroHashMap<UInt16, AggregateDataPtr>;

using AggregatedDataWithUInt32Key = HashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64Key = HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;

using AggregatedDataWithShortStringKey = StringHashMap<AggregateDataPtr>;

using AggregatedDataWithStringKey = HashMapWithSavedHash<StringRef, AggregateDataPtr>;

using AggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using AggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

using AggregatedDataWithUInt32KeyTwoLevel = TwoLevelHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyTwoLevel = TwoLevelHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;

using AggregatedDataWithShortStringKeyTwoLevel = TwoLevelStringHashMap<AggregateDataPtr>;

using AggregatedDataWithStringKeyTwoLevel = TwoLevelHashMapWithSavedHash<StringRef, AggregateDataPtr>;

using AggregatedDataWithKeys128TwoLevel = TwoLevelHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using AggregatedDataWithKeys256TwoLevel = TwoLevelHashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

/** Variants with better hash function, using more than 32 bits for hash.
  * Using for merging phase of external aggregation, where number of keys may be far greater than 4 billion,
  *  but we keep in memory and merge only sub-partition of them simultaneously.
  * TODO We need to switch for better hash function not only for external aggregation,
  *  but also for huge aggregation results on machines with terabytes of RAM.
  */

using AggregatedDataWithUInt64KeyHash64 = HashMap<UInt64, AggregateDataPtr, DefaultHash<UInt64>>;
using AggregatedDataWithStringKeyHash64 = HashMapWithSavedHash<StringRef, AggregateDataPtr, StringRefHash64>;
using AggregatedDataWithKeys128Hash64 = HashMap<UInt128, AggregateDataPtr, UInt128Hash>;
using AggregatedDataWithKeys256Hash64 = HashMap<UInt256, AggregateDataPtr, UInt256Hash>;

template <typename Base>
struct AggregationDataWithNullKey : public Base
{
    using Base::Base;

    bool & hasNullKeyData() { return has_null_key_data; }
    AggregateDataPtr & getNullKeyData() { return null_key_data; }
    bool hasNullKeyData() const { return has_null_key_data; }
    const AggregateDataPtr & getNullKeyData() const { return null_key_data; }
    size_t size() const { return Base::size() + (has_null_key_data ? 1 : 0); }
    bool empty() const { return Base::empty() && !has_null_key_data; }
    void clear()
    {
        Base::clear();
        has_null_key_data = false;
    }
    void clearAndShrink()
    {
        Base::clearAndShrink();
        has_null_key_data = false;
    }

private:
    bool has_null_key_data = false;
    AggregateDataPtr null_key_data = nullptr;
};

template <typename Base>
struct AggregationDataWithNullKeyTwoLevel : public Base
{
    using Base::Base;
    using Base::impls;

    AggregationDataWithNullKeyTwoLevel() = default;

    template <typename Other>
    explicit AggregationDataWithNullKeyTwoLevel(const Other & other) : Base(other)
    {
        impls[0].hasNullKeyData() = other.hasNullKeyData();
        impls[0].getNullKeyData() = other.getNullKeyData();
    }

    bool & hasNullKeyData() { return impls[0].hasNullKeyData(); }
    AggregateDataPtr & getNullKeyData() { return impls[0].getNullKeyData(); }
    bool hasNullKeyData() const { return impls[0].hasNullKeyData(); }
    const AggregateDataPtr & getNullKeyData() const { return impls[0].getNullKeyData(); }
};

template <typename ... Types>
using HashTableWithNullKey = AggregationDataWithNullKey<HashMapTable<Types ...>>;
template <typename ... Types>
using StringHashTableWithNullKey = AggregationDataWithNullKey<StringHashMap<Types ...>>;

using AggregatedDataWithNullableUInt8Key = AggregationDataWithNullKey<AggregatedDataWithUInt8Key>;
using AggregatedDataWithNullableUInt16Key = AggregationDataWithNullKey<AggregatedDataWithUInt16Key>;
using AggregatedDataWithNullableUInt32Key = AggregationDataWithNullKey<AggregatedDataWithUInt32Key>;


using AggregatedDataWithNullableUInt64Key = AggregationDataWithNullKey<AggregatedDataWithUInt64Key>;
using AggregatedDataWithNullableStringKey = AggregationDataWithNullKey<AggregatedDataWithStringKey>;
using AggregatedDataWithNullableShortStringKey = AggregationDataWithNullKey<AggregatedDataWithShortStringKey>;


using AggregatedDataWithNullableUInt32KeyTwoLevel = AggregationDataWithNullKeyTwoLevel<
    TwoLevelHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>,
                    TwoLevelHashTableGrower<>, HashTableAllocator, HashTableWithNullKey>>;
using AggregatedDataWithNullableUInt64KeyTwoLevel = AggregationDataWithNullKeyTwoLevel<
        TwoLevelHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>,
        TwoLevelHashTableGrower<>, HashTableAllocator, HashTableWithNullKey>>;

using AggregatedDataWithNullableShortStringKeyTwoLevel = AggregationDataWithNullKeyTwoLevel<
        TwoLevelStringHashMap<AggregateDataPtr, HashTableAllocator, StringHashTableWithNullKey>>;

using AggregatedDataWithNullableStringKeyTwoLevel = AggregationDataWithNullKeyTwoLevel<
        TwoLevelHashMapWithSavedHash<StringRef, AggregateDataPtr, DefaultHash<StringRef>,
        TwoLevelHashTableGrower<>, HashTableAllocator, HashTableWithNullKey>>;
}
