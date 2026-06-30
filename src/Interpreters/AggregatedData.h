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

/** Cell for the special case when there are no aggregate functions (`GROUP BY` without aggregates,
  * i.e. effectively `DISTINCT`). The Aggregator stores an `AggregateDataPtr` per key only to mark a
  * cell as occupied (a `0x1` sentinel) and to satisfy generic code that binds `AggregateDataPtr &` to
  * `getMapped`. We don't actually need per-key storage, so this cell keeps only the key (halving the
  * cell footprint for `UInt64`: 8 bytes instead of 16) and routes `getMapped` to a per-thread dummy.
  * The dummy is `thread_local` so the parallel two-level merge - where `dst` and `src` both resolve to
  * the same dummy within a worker thread - is free of data races; with zero aggregate functions nothing
  * meaningful is ever read from it.
  */
template <typename Key, typename Hash, typename TState = HashTableNoState>
struct AggregatedDataVoidCell
{
    using State = TState;

    using key_type = Key;
    using value_type = Key;
    using mapped_type = AggregateDataPtr;
    using Mapped = AggregateDataPtr;

    Key key;

    AggregatedDataVoidCell() {} /// NOLINT
    AggregatedDataVoidCell(const Key & key_, const State &) : key(key_) {}

    static AggregateDataPtr & dummyMapped()
    {
        static thread_local AggregateDataPtr dummy = nullptr;
        return dummy;
    }

    const Key & getKey() const { return key; }
    AggregateDataPtr & getMapped() { return dummyMapped(); }
    const AggregateDataPtr & getMapped() const { return dummyMapped(); }
    const value_type & getValue() const { return key; }

    static const Key & getKey(const value_type & value) { return value; } /// NOLINT(bugprone-return-const-ref-from-parameter)

    bool keyEquals(const Key & key_) const { return bitEquals(key, key_); }
    bool keyEquals(const Key & key_, size_t /*hash_*/) const { return bitEquals(key, key_); }
    bool keyEquals(const Key & key_, size_t /*hash_*/, const State & /*state*/) const { return bitEquals(key, key_); }

    void setHash(size_t /*hash_value*/) {}
    size_t getHash(const Hash & hash) const { return hash(key); }

    bool isZero(const State & state) const { return isZero(key, state); }
    static bool isZero(const Key & key_, const State & /*state*/) { return ZeroTraits::check(key_); }
    void setZero() { ZeroTraits::set(key); }
    static constexpr bool need_zero_value_storage = true;

    void setMapped(const value_type & /*value*/) {}

    void write(DB::WriteBuffer & wb) const         { DB::writeBinaryLittleEndian(key, wb); }
    void writeText(DB::WriteBuffer & wb) const     { DB::writeDoubleQuoted(key, wb); }
    void read(DB::ReadBuffer & rb)                 { DB::readBinaryLittleEndian(key, rb); }
    void readText(DB::ReadBuffer & rb)             { DB::readDoubleQuoted(key, rb); }
};

/// Void-mapped single-level maps (no per-key `AggregateDataPtr`). Selected for `GROUP BY` without
/// aggregate functions. The UInt32 and UInt64 maps back several `AggregationMethod` instantiations:
/// the UInt64 map is shared by `key32`/`key64` (`AggregationMethodOneNumber`) and `keys64`
/// (`AggregationMethodKeysFixed`); the UInt32 map backs `keys32`; the wide maps back `keys128`/`keys256`.
using AggregatedDataWithUInt32KeyVoid = HashMapTable<UInt32, AggregatedDataVoidCell<UInt32, HashCRC32<UInt32>>, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyVoid = HashMapTable<UInt64, AggregatedDataVoidCell<UInt64, HashCRC32<UInt64>>, HashCRC32<UInt64>>;

using AggregatedDataWithShortStringKey = StringHashMap<AggregateDataPtr>;

using AggregatedDataWithStringKey = HashMapWithSavedHash<std::string_view, AggregateDataPtr>;

using AggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using AggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

using AggregatedDataWithKeys128Void = HashMapTable<UInt128, AggregatedDataVoidCell<UInt128, UInt128HashCRC32>, UInt128HashCRC32>;
using AggregatedDataWithKeys256Void = HashMapTable<UInt256, AggregatedDataVoidCell<UInt256, UInt256HashCRC32>, UInt256HashCRC32>;

using AggregatedDataWithUInt32KeyTwoLevel = TwoLevelHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyTwoLevel = TwoLevelHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;

using AggregatedDataWithUInt32KeyVoidTwoLevel = TwoLevelHashMapTable<UInt32, AggregatedDataVoidCell<UInt32, HashCRC32<UInt32>>, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyVoidTwoLevel = TwoLevelHashMapTable<UInt64, AggregatedDataVoidCell<UInt64, HashCRC32<UInt64>>, HashCRC32<UInt64>>;

using AggregatedDataWithShortStringKeyTwoLevel = TwoLevelStringHashMap<AggregateDataPtr>;

using AggregatedDataWithStringKeyTwoLevel = TwoLevelHashMapWithSavedHash<std::string_view, AggregateDataPtr>;

using AggregatedDataWithKeys128TwoLevel = TwoLevelHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using AggregatedDataWithKeys256TwoLevel = TwoLevelHashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

using AggregatedDataWithKeys128VoidTwoLevel = TwoLevelHashMapTable<UInt128, AggregatedDataVoidCell<UInt128, UInt128HashCRC32>, UInt128HashCRC32>;
using AggregatedDataWithKeys256VoidTwoLevel = TwoLevelHashMapTable<UInt256, AggregatedDataVoidCell<UInt256, UInt256HashCRC32>, UInt256HashCRC32>;

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

/// Void-mapped counterparts of the better-hash maps above, used as the external-aggregation merge target
/// for the void `GROUP BY` methods: the merged key count can exceed 4 billion, so - exactly like the regular
/// key64/keys128/keys256 -> *_hash64 upgrade in `Aggregator::mergeBlocks` - the merge must use a full-width
/// hash instead of `HashCRC32` to avoid collision-driven blowup, while still keeping the key-only cell.
using AggregatedDataWithUInt64KeyVoidHash64 = HashMapTable<UInt64, AggregatedDataVoidCell<UInt64, DefaultHash<UInt64>>, DefaultHash<UInt64>>;
using AggregatedDataWithKeys128VoidHash64 = HashMapTable<UInt128, AggregatedDataVoidCell<UInt128, UInt128Hash>, UInt128Hash>;
using AggregatedDataWithKeys256VoidHash64 = HashMapTable<UInt256, AggregatedDataVoidCell<UInt256, UInt256Hash>, UInt256Hash>;

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
        TwoLevelHashMapWithSavedHash<std::string_view, AggregateDataPtr, DefaultHash<std::string_view>,
        TwoLevelHashTableGrower<>, HashTableAllocator, HashTableWithNullKey>>;
}
