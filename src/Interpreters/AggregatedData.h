#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>

#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashSet.h>
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

/** For `GROUP BY` without aggregate functions (effectively `DISTINCT`) there is no per-key state to
  * store, so the "data" is a plain set rather than a map: we reuse the existing `HashSet` family instead
  * of a map with a dead `AggregateDataPtr` slot. The set cells report `VoidMapped`, and the
  * `AggregationMethod`s present that as `Mapped = void` (see `AggregationMethod.h`), which puts
  * `ColumnsHashing` in set mode. The key methods (`OneNumber`/`KeysFixed`/`Serialized`), two-level
  * conversion and the better-hash external-merge upgrade all work exactly as for the map variants.
  */
using AggregatedDataWithUInt32KeyVoid = HashSet<UInt32, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyVoid = HashSet<UInt64, HashCRC32<UInt64>>;

using AggregatedDataWithShortStringKey = StringHashMap<AggregateDataPtr>;

using AggregatedDataWithStringKey = HashMapWithSavedHash<std::string_view, AggregateDataPtr>;
/// Void (set) string-key data for the serialized / prealloc_serialized methods. The saved-hash set cell
/// avoids recomputing the hash of the Arena-held serialized key on every probe.
using AggregatedDataWithStringKeyVoid = HashSetWithSavedHash<std::string_view, DefaultHash<std::string_view>>;

using AggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using AggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

using AggregatedDataWithKeys128Void = HashSet<UInt128, UInt128HashCRC32>;
using AggregatedDataWithKeys256Void = HashSet<UInt256, UInt256HashCRC32>;

using AggregatedDataWithUInt32KeyTwoLevel = TwoLevelHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyTwoLevel = TwoLevelHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;

using AggregatedDataWithUInt32KeyVoidTwoLevel = TwoLevelHashSet<UInt32, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyVoidTwoLevel = TwoLevelHashSet<UInt64, HashCRC32<UInt64>>;

using AggregatedDataWithShortStringKeyTwoLevel = TwoLevelStringHashMap<AggregateDataPtr>;

using AggregatedDataWithStringKeyTwoLevel = TwoLevelHashMapWithSavedHash<std::string_view, AggregateDataPtr>;
using AggregatedDataWithStringKeyVoidTwoLevel = TwoLevelHashSetWithSavedHash<std::string_view, DefaultHash<std::string_view>>;

using AggregatedDataWithKeys128TwoLevel = TwoLevelHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using AggregatedDataWithKeys256TwoLevel = TwoLevelHashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

using AggregatedDataWithKeys128VoidTwoLevel = TwoLevelHashSet<UInt128, UInt128HashCRC32>;
using AggregatedDataWithKeys256VoidTwoLevel = TwoLevelHashSet<UInt256, UInt256HashCRC32>;

/** Variants with better hash function, using more than 32 bits for hash.
  * Using for merging phase of external aggregation, where number of keys may be far greater than 4 billion,
  *  but we keep in memory and merge only sub-partition of them simultaneously.
  * TODO We need to switch for better hash function not only for external aggregation,
  *  but also for huge aggregation results on machines with terabytes of RAM.
  */

using AggregatedDataWithUInt64KeyHash64 = HashMap<UInt64, AggregateDataPtr, DefaultHash<UInt64>>;
using AggregatedDataWithStringKeyHash64 = HashMapWithSavedHash<std::string_view, AggregateDataPtr, StringViewHash64>;
using AggregatedDataWithKeys128Hash64 = HashMap<UInt128, AggregateDataPtr, UInt128Hash>;
using AggregatedDataWithKeys256Hash64 = HashMap<UInt256, AggregateDataPtr, UInt256Hash>;

/// Void (set) counterparts of the better-hash data above, used as the external-aggregation merge target
/// for the void `GROUP BY` methods: the merged key count can exceed 4 billion, so - exactly like the regular
/// key64/keys128/keys256 -> *_hash64 upgrade in `Aggregator::mergeBlocks` - the merge must use a full-width
/// hash instead of `HashCRC32` to avoid collision-driven blowup.
using AggregatedDataWithUInt64KeyVoidHash64 = HashSet<UInt64, DefaultHash<UInt64>>;
using AggregatedDataWithKeys128VoidHash64 = HashSet<UInt128, UInt128Hash>;
using AggregatedDataWithKeys256VoidHash64 = HashSet<UInt256, UInt256Hash>;
using AggregatedDataWithStringKeyVoidHash64 = HashSetWithSavedHash<std::string_view, StringViewHash64>;

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
using HashSetWithNullKey = AggregationDataWithNullKey<HashSetTable<Types ...>>;
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
        TwoLevelHashMapWithSavedHash<std::string_view, AggregateDataPtr, DefaultHash<std::string_view>,
        TwoLevelHashTableGrower<>, HashTableAllocator, HashTableWithNullKey>>;

/// Void (set) nullable single-number data for `GROUP BY` without aggregate functions. The null group's
/// presence is tracked by `AggregationDataWithNullKey` (one bool + one dead ptr, O(1)); only the non-null
/// keys live in the set.
using AggregatedDataWithNullableUInt32KeyVoid = AggregationDataWithNullKey<AggregatedDataWithUInt32KeyVoid>;
using AggregatedDataWithNullableUInt64KeyVoid = AggregationDataWithNullKey<AggregatedDataWithUInt64KeyVoid>;
/// The per-bucket impl of a nullable two-level set: a `HashSet` bucket that also tracks the null key.
/// `TwoLevelHashTable`'s `ImplTable` parameter is a type (unlike `TwoLevelHashMapTable`'s template-template),
/// so it has to be instantiated explicitly here.
template <typename Key, typename Hash>
using TwoLevelHashSetWithNullKeyImpl
    = HashSetWithNullKey<Key, HashTableCell<Key, Hash>, Hash, TwoLevelHashTableGrower<>, HashTableAllocator>;

using AggregatedDataWithNullableUInt32KeyVoidTwoLevel = AggregationDataWithNullKeyTwoLevel<
    TwoLevelHashTable<UInt32, HashTableCell<UInt32, HashCRC32<UInt32>>, HashCRC32<UInt32>,
                      TwoLevelHashTableGrower<>, HashTableAllocator, TwoLevelHashSetWithNullKeyImpl<UInt32, HashCRC32<UInt32>>>>;
using AggregatedDataWithNullableUInt64KeyVoidTwoLevel = AggregationDataWithNullKeyTwoLevel<
    TwoLevelHashTable<UInt64, HashTableCell<UInt64, HashCRC32<UInt64>>, HashCRC32<UInt64>,
                      TwoLevelHashTableGrower<>, HashTableAllocator, TwoLevelHashSetWithNullKeyImpl<UInt64, HashCRC32<UInt64>>>>;
}
