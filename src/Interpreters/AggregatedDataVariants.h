#pragma once
#include <Core/Block_fwd.h>
#include <Core/Names.h>
#include <Interpreters/AggregatedData.h>
#include <Interpreters/AggregationMethod.h>
#include <Common/PODArray.h>

#include <atomic>
#include <memory>
#include <optional>
#include <boost/noncopyable.hpp>


class MemoryTracker;

namespace DB
{

namespace ColumnsHashing
{
struct HashMethodContextSettings;
}

class Arena;
class Aggregator;

/// Whether an aggregation table iterates its cells, which an index of its cells needs (see
/// `AggregatedDataVariants::CellIndex`): the string table with its sub-maps does not.
template <typename Table>
concept IterableCells = requires (Table & table) { table.begin(); };

struct AggregatedDataVariants : private boost::noncopyable
{
    /** Working with states of aggregate functions in the pool is arranged in the following (inconvenient) way:
      * - when aggregating, states are created in the pool using IAggregateFunction::create (inside - `placement new` of arbitrary structure);
      * - they must then be destroyed using IAggregateFunction::destroy (inside - calling the destructor of arbitrary structure);
      * - if aggregation is complete, then, in the Aggregator::convertToBlocks function, pointers to the states of aggregate functions
      *   are written to ColumnAggregateFunction; ColumnAggregateFunction "acquires ownership" of them, that is - calls `destroy` in its destructor.
      * - if during the aggregation, before call to Aggregator::convertToBlocks, an exception was thrown,
      *   then the states of aggregate functions must still be destroyed,
      *   otherwise, for complex states (eg, AggregateFunctionUniq), there will be memory leaks;
      * - in this case, to destroy states, the destructor calls Aggregator::destroyAggregateStates method,
      *   but only if the variable aggregator (see below) is not nullptr;
      * - that is, until you transfer ownership of the aggregate function states in the ColumnAggregateFunction, set the variable `aggregator`,
      *   so that when an exception occurs, the states are correctly destroyed.
      *
      * PS. This can be corrected by making a pool that knows about which states of aggregate functions and in which order are put in it, and knows how to destroy them.
      * But this can hardly be done simply because it is planned to put variable-length strings into the same pool.
      * In this case, the pool will not be able to know with what offsets objects are stored.
      */
    const Aggregator * aggregator = nullptr;

    size_t keys_size{};  /// Number of keys. NOTE do we need this field?
    Sizes key_sizes;     /// Dimensions of keys, if keys of fixed length

    /// Pools for states of aggregate functions. Ownership will be later transferred to ColumnAggregateFunction.
    using ArenaPtr = std::shared_ptr<Arena>;
    using Arenas = std::vector<ArenaPtr>;
    Arenas aggregates_pools;
    Arena * aggregates_pool{};    /// The pool that is currently used for allocation.

    /// Accounts for the whole state of this table, including memory that aggregate
    /// states allocate outside the arenas. Attached by the aggregator on the first block.
    std::unique_ptr<MemoryTracker> memory_tracker;

    /** Specialization for the case when there are no keys, and for keys not fitted into max_rows_to_group_by.
      */
    AggregatedDataWithoutKey without_key = nullptr;

    /// Stats of a cache for consecutive keys optimization.
    /// Stats can be used to disable the cache in case of a lot of misses.
    ColumnsHashing::LastElementCacheStats consecutive_keys_cache_stats;

    // Disable consecutive key optimization for Uint8/16, because they use a FixedHashMap
    // and the lookup there is almost free, so we don't need to cache the last lookup result
    std::unique_ptr<AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key, false>>           key8;
    std::unique_ptr<AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key, false>>         key16;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64Key>>         key32;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>>         key64;
    /// Void-mapped variants for `GROUP BY` of fixed-width keys with no aggregate functions (effectively
    /// `DISTINCT`): the cells store only the key, with no dead `AggregateDataPtr` slot. For the
    /// `AggregationMethodOneNumber` variants consecutive_keys_optimization is disabled (the cache stores a
    /// `value.second` that the void cell, value_type == Key, does not have, and unique-key workloads gain
    /// nothing from it); `AggregationMethodKeysFixed` already disables it by default.
    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyVoid, false>> key32_void;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyVoid, false>> key64_void;
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithShortStringKey>>               key_string;
    std::unique_ptr<AggregationMethodPackedString<AggregatedDataWithPackedStringKey>>        key_packed_string;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKey>>          key_fixed_string;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt16Key, false, false, false>>  keys16;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32Key>>                   keys32;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64Key>>                   keys64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128>>                   keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256>>                   keys256;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32KeyVoid>>             keys32_void;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyVoid>>             keys64_void;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128Void>>               keys128_void;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256Void>>               keys256_void;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKey>>                          serialized;
    std::unique_ptr<AggregationMethodNullableSerialized<AggregatedDataWithStringKey>>                  nullable_serialized;
    std::unique_ptr<AggregationMethodPreallocSerialized<AggregatedDataWithStringKey>>                  prealloc_serialized;
    std::unique_ptr<AggregationMethodNullablePreallocSerialized<AggregatedDataWithStringKey>>          nullable_prealloc_serialized;
    /// Void-mapped serialized variants for `GROUP BY` without aggregate functions (key-only cells).
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyVoid>>                      serialized_void;
    std::unique_ptr<AggregationMethodNullableSerialized<AggregatedDataWithStringKeyVoid>>              nullable_serialized_void;
    std::unique_ptr<AggregationMethodPreallocSerialized<AggregatedDataWithStringKeyVoid>>              prealloc_serialized_void;
    std::unique_ptr<AggregationMethodNullablePreallocSerialized<AggregatedDataWithStringKeyVoid>>      nullable_prealloc_serialized_void;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyTwoLevel>> key32_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyTwoLevel>> key64_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyVoidTwoLevel, false>> key32_void_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyVoidTwoLevel, false>> key64_void_two_level;
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>>       key_string_two_level;
    std::unique_ptr<AggregationMethodPackedString<AggregatedDataWithPackedStringKeyTwoLevel>>       key_packed_string_two_level;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>>  key_fixed_string_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32KeyTwoLevel>>           keys32_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyTwoLevel>>           keys64_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel>>           keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel>>           keys256_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32KeyVoidTwoLevel>>     keys32_void_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyVoidTwoLevel>>     keys64_void_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128VoidTwoLevel>>       keys128_void_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256VoidTwoLevel>>       keys256_void_two_level;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel>>                  serialized_two_level;
    std::unique_ptr<AggregationMethodNullableSerialized<AggregatedDataWithStringKeyTwoLevel>>          nullable_serialized_two_level;
    std::unique_ptr<AggregationMethodPreallocSerialized<AggregatedDataWithStringKeyTwoLevel>>          prealloc_serialized_two_level;
    std::unique_ptr<AggregationMethodNullablePreallocSerialized<AggregatedDataWithStringKeyTwoLevel>>  nullable_prealloc_serialized_two_level;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyVoidTwoLevel>>              serialized_void_two_level;
    std::unique_ptr<AggregationMethodNullableSerialized<AggregatedDataWithStringKeyVoidTwoLevel>>      nullable_serialized_void_two_level;
    std::unique_ptr<AggregationMethodPreallocSerialized<AggregatedDataWithStringKeyVoidTwoLevel>>      prealloc_serialized_void_two_level;
    std::unique_ptr<AggregationMethodNullablePreallocSerialized<AggregatedDataWithStringKeyVoidTwoLevel>> nullable_prealloc_serialized_void_two_level;

    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyHash64>>   key64_hash64;
    std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKeyHash64>>              key_string_hash64;
    std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKeyHash64>>         key_fixed_string_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128Hash64>>             keys128_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256Hash64>>             keys256_hash64;
    /// Better-hash merge targets for the void methods (see AggregatedData.h). consecutive_keys_optimization
    /// is disabled for the OneNumber one, matching key64_void.
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyVoidHash64, false>> key64_void_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128VoidHash64>>         keys128_void_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256VoidHash64>>         keys256_void_hash64;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyHash64>>                  serialized_hash64;
    std::unique_ptr<AggregationMethodNullableSerialized<AggregatedDataWithStringKeyHash64>>          nullable_serialized_hash64;
    std::unique_ptr<AggregationMethodPreallocSerialized<AggregatedDataWithStringKeyHash64>>          prealloc_serialized_hash64;
    std::unique_ptr<AggregationMethodNullablePreallocSerialized<AggregatedDataWithStringKeyHash64>>  nullable_prealloc_serialized_hash64;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyVoidHash64>>              serialized_void_hash64;
    std::unique_ptr<AggregationMethodNullableSerialized<AggregatedDataWithStringKeyVoidHash64>>      nullable_serialized_void_hash64;
    std::unique_ptr<AggregationMethodPreallocSerialized<AggregatedDataWithStringKeyVoidHash64>>      prealloc_serialized_void_hash64;
    std::unique_ptr<AggregationMethodNullablePreallocSerialized<AggregatedDataWithStringKeyVoidHash64>> nullable_prealloc_serialized_void_hash64;
    /// The nullable fixed-width set methods need the better hash for the same reason as the rest: their
    /// tables are the ones the external merge re-aggregates. The packed forms reuse the plain void hash64
    /// data with `has_nullable_keys`, a nullable packed key carrying its null map inside the key.
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyVoidHash64, false, true>> nullable_key64_void_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128VoidHash64, true>>  nullable_keys128_void_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256VoidHash64, true>>  nullable_keys256_void_hash64;
    /// The nullable fixed-width keys need the better hash for the same reason as their non-nullable
    /// counterparts. The packed forms reuse the plain `*Hash64` data: a nullable packed key carries its null
    /// map inside the key, so only `has_nullable_keys` differs.
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyHash64, true, true>> nullable_key64_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128Hash64, true>>       nullable_keys128_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256Hash64, true>>       nullable_keys256_hash64;

    /// Support for nullable keys.
    std::unique_ptr<AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false, true>>         nullable_key8;
    std::unique_ptr<AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key, false, true>>         nullable_key16;
    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32Key, true, true>>         nullable_key32;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key, true, true>>         nullable_key64;
    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32KeyTwoLevel, true, true>>         nullable_key32_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyTwoLevel, true, true>>         nullable_key64_two_level;
    /// Void-mapped nullable single-number variants (GROUP BY without aggregates). consecutive_keys_optimization
    /// disabled (void cells have no value.second cache), nullable=true.
    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32KeyVoid, false, true>>         nullable_key32_void;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyVoid, false, true>>         nullable_key64_void;
    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32KeyVoidTwoLevel, false, true>> nullable_key32_void_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyVoidTwoLevel, false, true>> nullable_key64_void_two_level;

    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithNullableShortStringKey, true>> nullable_key_string;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithNullableShortStringKey, true>> nullable_key_fixed_string;
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithNullableShortStringKeyTwoLevel, true>> nullable_key_string_two_level;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithNullableShortStringKeyTwoLevel, true>> nullable_key_fixed_string_two_level;

    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128, true>>             nullable_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256, true>>             nullable_keys256;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel, true>>     nullable_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel, true>>     nullable_keys256_two_level;
    /// Void-mapped nullable packed-keys variants (null bitmap packed into the key, so the existing void maps
    /// are reused with has_nullable_keys=true).
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128Void, true>>         nullable_keys128_void;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256Void, true>>         nullable_keys256_void;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128VoidTwoLevel, true>> nullable_keys128_void_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256VoidTwoLevel, true>> nullable_keys256_void_two_level;

    /// Support for low cardinality.
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false>>> low_cardinality_key8;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key, false>>> low_cardinality_key16;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt64Key>>> low_cardinality_key32;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>>> low_cardinality_key64;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<AggregatedDataWithNullableStringKey>>> low_cardinality_key_string;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<AggregatedDataWithNullableStringKey>>> low_cardinality_key_fixed_string;

    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt64KeyTwoLevel>>> low_cardinality_key32_two_level;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyTwoLevel>>> low_cardinality_key64_two_level;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<AggregatedDataWithNullableStringKeyTwoLevel>>> low_cardinality_key_string_two_level;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<AggregatedDataWithNullableStringKeyTwoLevel>>> low_cardinality_key_fixed_string_two_level;

    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128, false, true>>      low_cardinality_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256, false, true>>      low_cardinality_keys256;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel, false, true>> low_cardinality_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel, false, true>> low_cardinality_keys256_two_level;

    std::unique_ptr<AggregationMethodSingleLowCardinalityDictionaryIndex<AggregatedDataWithUInt64Key>>
        low_cardinality_single_dictionary;
    std::unique_ptr<AggregationMethodSingleLowCardinalityDictionaryIndex<AggregatedDataWithUInt64KeyTwoLevel>>
        low_cardinality_single_dictionary_two_level;

    /// In this and similar macros, the option without_key is not considered.
    #define APPLY_FOR_AGGREGATED_VARIANTS(M) \
        M(key8,                       false) \
        M(key16,                      false) \
        M(key32,                      false) \
        M(key64,                      false) \
        M(key32_void,                 false) \
        M(key64_void,                 false) \
        M(keys32_void,                false) \
        M(keys64_void,                false) \
        M(keys128_void,               false) \
        M(keys256_void,               false) \
        M(key32_void_two_level,       true) \
        M(key64_void_two_level,       true) \
        M(keys32_void_two_level,      true) \
        M(keys64_void_two_level,      true) \
        M(keys128_void_two_level,     true) \
        M(keys256_void_two_level,     true) \
        M(key_string,                 false) \
        M(key_packed_string,          false) \
        M(key_fixed_string,           false) \
        M(keys16,                    false) \
        M(keys32,                    false) \
        M(keys64,                    false) \
        M(keys128,                    false) \
        M(keys256,                    false) \
        M(serialized,                   false) \
        M(nullable_serialized,          false) \
        M(prealloc_serialized,          false) \
        M(nullable_prealloc_serialized, false) \
        M(serialized_void,                   false) \
        M(nullable_serialized_void,          false) \
        M(prealloc_serialized_void,          false) \
        M(nullable_prealloc_serialized_void, false) \
        M(serialized_void_two_level,                   true) \
        M(nullable_serialized_void_two_level,          true) \
        M(prealloc_serialized_void_two_level,          true) \
        M(nullable_prealloc_serialized_void_two_level, true) \
        M(serialized_void_hash64,                   false) \
        M(nullable_serialized_void_hash64,          false) \
        M(prealloc_serialized_void_hash64,          false) \
        M(nullable_prealloc_serialized_void_hash64, false) \
        M(nullable_key64_void_hash64,   false) \
        M(nullable_keys128_void_hash64, false) \
        M(nullable_keys256_void_hash64, false) \
        M(key32_two_level,            true) \
        M(key64_two_level,            true) \
        M(key_string_two_level,       true) \
        M(key_packed_string_two_level, true) \
        M(key_fixed_string_two_level, true) \
        M(keys32_two_level,          true) \
        M(keys64_two_level,          true) \
        M(keys128_two_level,          true) \
        M(keys256_two_level,          true) \
        M(serialized_two_level,                   true) \
        M(nullable_serialized_two_level,          true) \
        M(prealloc_serialized_two_level,          true) \
        M(nullable_prealloc_serialized_two_level, true) \
        M(key64_hash64,               false) \
        M(key64_void_hash64,          false) \
        M(keys128_void_hash64,        false) \
        M(keys256_void_hash64,        false) \
        M(key_string_hash64,          false) \
        M(key_fixed_string_hash64,    false) \
        M(keys128_hash64,             false) \
        M(keys256_hash64,             false) \
        M(serialized_hash64,                   false) \
        M(nullable_serialized_hash64,          false) \
        M(prealloc_serialized_hash64,          false) \
        M(nullable_prealloc_serialized_hash64, false) \
        M(nullable_key64_hash64,      false) \
        M(nullable_keys128_hash64,    false) \
        M(nullable_keys256_hash64,    false) \
        M(nullable_key8,             false) \
        M(nullable_key16,             false) \
        M(nullable_key32,             false) \
        M(nullable_key64,             false) \
        M(nullable_key32_two_level,   true) \
        M(nullable_key64_two_level,   true) \
        M(nullable_key32_void,             false) \
        M(nullable_key64_void,             false) \
        M(nullable_key32_void_two_level,   true) \
        M(nullable_key64_void_two_level,   true) \
        M(nullable_key_string,        false) \
        M(nullable_key_fixed_string,  false) \
        M(nullable_key_string_two_level, true) \
        M(nullable_key_fixed_string_two_level, true) \
        M(nullable_keys128,           false) \
        M(nullable_keys256,           false) \
        M(nullable_keys128_two_level, true) \
        M(nullable_keys256_two_level, true) \
        M(nullable_keys128_void,           false) \
        M(nullable_keys256_void,           false) \
        M(nullable_keys128_void_two_level, true) \
        M(nullable_keys256_void_two_level, true) \
        M(low_cardinality_key8, false) \
        M(low_cardinality_key16, false) \
        M(low_cardinality_key32, false) \
        M(low_cardinality_key64, false) \
        M(low_cardinality_keys128, false) \
        M(low_cardinality_keys256, false) \
        M(low_cardinality_key_string, false) \
        M(low_cardinality_key_fixed_string, false) \
        M(low_cardinality_key32_two_level, true) \
        M(low_cardinality_key64_two_level, true) \
        M(low_cardinality_keys128_two_level, true) \
        M(low_cardinality_keys256_two_level, true) \
        M(low_cardinality_key_string_two_level, true) \
        M(low_cardinality_key_fixed_string_two_level, true) \
        M(low_cardinality_single_dictionary, false) \
        M(low_cardinality_single_dictionary_two_level, true) \

    #define APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M) \
        M(key32)            \
        M(key64)            \
        M(key32_void)       \
        M(key64_void)       \
        M(keys32_void)      \
        M(keys64_void)      \
        M(keys128_void)     \
        M(keys256_void)     \
        M(key_string)       \
        M(key_packed_string) \
        M(key_fixed_string) \
        M(keys32)           \
        M(keys64)           \
        M(keys128)          \
        M(keys256)          \
        M(serialized)       \
        M(nullable_serialized) \
        M(prealloc_serialized) \
        M(nullable_prealloc_serialized) \
        M(serialized_void)       \
        M(nullable_serialized_void) \
        M(prealloc_serialized_void) \
        M(nullable_prealloc_serialized_void) \
        M(nullable_key32) \
        M(nullable_key64) \
        M(nullable_key32_void) \
        M(nullable_key64_void) \
        M(nullable_key_string) \
        M(nullable_key_fixed_string) \
        M(nullable_keys128) \
        M(nullable_keys256) \
        M(nullable_keys128_void) \
        M(nullable_keys256_void) \
        M(low_cardinality_key32) \
        M(low_cardinality_key64) \
        M(low_cardinality_keys128) \
        M(low_cardinality_keys256) \
        M(low_cardinality_key_string) \
        M(low_cardinality_key_fixed_string) \
        M(low_cardinality_single_dictionary) \

    /// NOLINTNEXTLINE
    #define APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M) \
        M(key8)             \
        M(key16)            \
        M(nullable_key8) \
        M(nullable_key16) \
        M(keys16)           \
        M(key64_hash64)     \
        M(key64_void_hash64) \
        M(keys128_void_hash64) \
        M(keys256_void_hash64) \
        M(key_string_hash64)\
        M(key_fixed_string_hash64) \
        M(keys128_hash64)   \
        M(keys256_hash64)   \
        M(serialized_hash64) \
        M(nullable_serialized_hash64) \
        M(prealloc_serialized_hash64) \
        M(nullable_prealloc_serialized_hash64) \
        M(serialized_void_hash64) \
        M(nullable_serialized_void_hash64) \
        M(prealloc_serialized_void_hash64) \
        M(nullable_prealloc_serialized_void_hash64) \
        M(nullable_key64_void_hash64) \
        M(nullable_keys128_void_hash64) \
        M(nullable_keys256_void_hash64) \
        M(nullable_key64_hash64) \
        M(nullable_keys128_hash64) \
        M(nullable_keys256_hash64) \
        M(low_cardinality_key8) \
        M(low_cardinality_key16) \

    /// NOLINTNEXTLINE
    #define APPLY_FOR_VARIANTS_SINGLE_LEVEL(M) \
        APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M) \
        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M) \

    /// NOLINTNEXTLINE
    #define APPLY_FOR_VARIANTS_TWO_LEVEL(M) \
        M(key32_two_level)            \
        M(key64_two_level)            \
        M(key32_void_two_level)       \
        M(key64_void_two_level)       \
        M(keys32_void_two_level)      \
        M(keys64_void_two_level)      \
        M(keys128_void_two_level)     \
        M(keys256_void_two_level)     \
        M(key_string_two_level)       \
        M(key_packed_string_two_level) \
        M(key_fixed_string_two_level) \
        M(keys32_two_level)           \
        M(keys64_two_level)           \
        M(keys128_two_level)          \
        M(keys256_two_level)          \
        M(serialized_two_level)       \
        M(nullable_serialized_two_level)       \
        M(prealloc_serialized_two_level)       \
        M(nullable_prealloc_serialized_two_level)       \
        M(serialized_void_two_level)       \
        M(nullable_serialized_void_two_level)       \
        M(prealloc_serialized_void_two_level)       \
        M(nullable_prealloc_serialized_void_two_level)       \
        M(nullable_key32_two_level) \
        M(nullable_key64_two_level) \
        M(nullable_key32_void_two_level) \
        M(nullable_key64_void_two_level) \
        M(nullable_key_string_two_level) \
        M(nullable_key_fixed_string_two_level) \
        M(nullable_keys128_two_level) \
        M(nullable_keys256_two_level) \
        M(nullable_keys128_void_two_level) \
        M(nullable_keys256_void_two_level) \
        M(low_cardinality_key32_two_level) \
        M(low_cardinality_key64_two_level) \
        M(low_cardinality_keys128_two_level) \
        M(low_cardinality_keys256_two_level) \
        M(low_cardinality_key_string_two_level) \
        M(low_cardinality_key_fixed_string_two_level) \
        M(low_cardinality_single_dictionary_two_level) \

    #define APPLY_FOR_LOW_CARDINALITY_VARIANTS(M) \
        M(low_cardinality_key8) \
        M(low_cardinality_key16) \
        M(low_cardinality_key32) \
        M(low_cardinality_key64) \
        M(low_cardinality_keys128) \
        M(low_cardinality_keys256) \
        M(low_cardinality_key_string) \
        M(low_cardinality_key_fixed_string) \
        M(low_cardinality_key32_two_level) \
        M(low_cardinality_key64_two_level) \
        M(low_cardinality_keys128_two_level) \
        M(low_cardinality_keys256_two_level) \
        M(low_cardinality_key_string_two_level) \
        M(low_cardinality_key_fixed_string_two_level) \
        M(low_cardinality_single_dictionary) \
        M(low_cardinality_single_dictionary_two_level)

    enum class Type : uint8_t
    {
        EMPTY = 0,
        without_key,

    #define M(NAME, IS_TWO_LEVEL) NAME,
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    };
    Type type = Type::EMPTY;
    bool top_k_heap_ever_rejected = false;

    /// The table admits no key outside the set `kept_keys_seed` holds, under
    /// `group_by_overflow_mode = ANY`. Only such a table may be flushed to a temporary file
    /// while the shared cutoff holds (see `ManyAggregatedData::SharedKeptKeys`): everything it
    /// contains is a kept key, and `Aggregator` re-seeds it with the kept keys after the flush
    /// so the remaining rows of those keys keep being aggregated.
    bool restricted_to_kept_keys = false;

    /// Keys with empty aggregate states, in the mergeable block layout; the `Aggregator` puts
    /// them back into the table after an external-aggregation spill has emptied it. With the
    /// shared cutoff of the trivial `GROUP BY ... LIMIT` optimization this is the frozen set
    /// shared by all the streams (`ManyAggregatedData::SharedKeptKeys::seed`), captured before
    /// the table is rebuilt around it, and set together with `restricted_to_kept_keys`; with a
    /// plain `max_rows_to_group_by` cap it is refreshed by every flush from what that flush
    /// wrote, which is every key the stream has admitted so far.
    ConstBlockPtr kept_keys_seed;

    /// The table is being rebuilt around the kept keys, or re-seeded with them after a flush.
    /// Those merges only re-insert data the table already held, so an external-aggregation spill
    /// in the middle of them must be skipped: it would empty the table while the rest of the
    /// rebuild is merged into it under `no_more_keys`, dropping everything accumulated so far.
    bool kept_keys_rebuild_in_progress = false;

    AggregatedDataVariants();
    ~AggregatedDataVariants();
    bool empty() const { return type == Type::EMPTY; }
    /// Whether any rows were aggregated: `empty` only says no method was ever initialized,
    /// while a table that was flushed to disk keeps its type with zero rows.
    /// `empty` is not the negation of `size` here, so the two checks are not redundant and
    /// `readability-container-size-empty` does not apply.
    /// NOLINTNEXTLINE(readability-container-size-empty)
    bool hasData() const { return !empty() && size() != 0; }
    void invalidate() { type = Type::EMPTY; }
    /// Tears the variants down after the aggregate states changed owner (`aggregator` must
    /// already be null): destroys the active method and with it the hash-table buffer, drops
    /// the arenas and the overflow row, and leaves the variants empty, with no arena. Calling
    /// this while the variants still own live nontrivial states would leak them undestroyed.
    void resetAfterStateOwnershipTransfer();
    void init(Type type_, std::optional<size_t> size_hint = std::nullopt);
    /// Number of rows (different keys).
    size_t size() const;
    size_t sizeWithoutOverflowRow() const;
    /// Memory held by the variants: the arenas (keys and states) plus the active method's
    /// hash-table buffer, which dominates for inline states over fixed keys.
    size_t allocatedBytes() const;
    /// Bytes of the active method's hash-table buffer: its cells, which hold the keys (references to
    /// the bytes of string keys) and the states' pointers, or the states themselves where they are
    /// stored inline.
    size_t hashTableBufferBytes() const;
    /// Bytes that the keys and states took in the arenas. Unlike the arenas' allocated bytes,
    /// which grow by whole chunks of doubling size, they follow every allocation.
    size_t usedArenaBytes() const;
    /// Tracked size of the state; without a tracker (merge-only aggregation) only arenas and
    /// hash table are known.
    size_t memoryUsage() const;

    /// The adaptive bucket-parallel merge's arena slots, one per bucket, populated on the merge
    /// destination (see `Aggregator::createAdaptiveMergeDestination`). A slot holds the arena of
    /// the bucket's unit being merged, takes a fresh one for every unit and is reset when the
    /// bucket retires (see `Aggregator::mergeAndConvertAdaptiveBucket`), so a converted unit's
    /// drained and merged states free early. Deliberately outside `aggregates_pools`: the output
    /// conversion captures that list wholesale into every bucket's non-final and -State columns,
    /// while each conversion captures only the arena of its unit. States adopted from the
    /// producers' mixed arenas stay in `aggregates_pools` and live until the variants die.
    Arenas adaptive_merge_bucket_arenas;

    /// Keys consumed and produced by the two-level bucket merges that have completed
    /// (`Aggregator::mergeBucketImpl`). The buckets are uniform hash samples of one key set,
    /// so the measured result/input ratio predicts the merged size of the remaining buckets.
    std::atomic<UInt64> merged_buckets_input_keys{0};
    std::atomic<UInt64> merged_buckets_result_keys{0};
    const char * getMethodName() const;
    bool isTwoLevel() const;
    bool isConvertibleToTwoLevel() const;
    static bool isConvertibleToTwoLevel(Type type);
    void convertToTwoLevel();
    /// Whether the cells of the single-level table can be indexed: the table of every method with a
    /// two-level counterpart, except the string table (see `IterableCells`).
    bool canIndexCells() const;
    /// Builds the cell index of the single-level table (see `CellIndex`): its cells grouped by
    /// `partition_of(hash)`, so that a merge reads the cells of any run of consecutive partitions without
    /// walking the table, which spares its conversion to two-level. The caller guarantees that the table can
    /// be indexed (see `canIndexCells`) and that `partition_of` returns a partition below `num_partitions`.
    /// Counts the table in `AggregationIndexedTables`, as `convertToTwoLevel` counts a conversion.
    template <typename PartitionOf>
    void indexCells(size_t num_partitions, PartitionOf && partition_of);
    /// Whether `indexCells` indexed the table's cells and nothing cleared the index since (see `CellIndex`).
    bool hasCellIndex() const { return cell_index.has_value(); }
    /// The partitions of the cell index. The caller guarantees that the table is indexed.
    size_t numCellIndexPartitions() const { return cell_index->offsets.size() - 1; }
    /// The cells of the partitions from `first_partition` to `end_partition` of the cell index. The caller
    /// guarantees that the table is indexed.
    size_t indexedCellCount(size_t first_partition, size_t end_partition) const
    {
        return cell_index->offsets[end_partition] - cell_index->offsets[first_partition];
    }
    /// Calls `callback(key, mapped, hash)` for each cell of the partitions from `first_partition` to
    /// `end_partition` of the cell index, partition by partition and in the order of the table within a
    /// partition. `mapped` is the cell's slot of the states, or a `VoidMapped` placeholder for a set method.
    /// `Method` is the method of the indexed table or of its two-level counterpart: both hold the same cells.
    /// The caller guarantees that the table is indexed.
    template <typename Method, typename Callback>
    void forEachIndexedCell(size_t first_partition, size_t end_partition, Callback && callback);
    /// Drops the cell index before an insertion into the table, which may move the cells it points to.
    void dropCellIndex() { cell_index.reset(); }
    /// Replaces an empty two-level table with an empty single-level table of the same method: the reverse of
    /// `convertToTwoLevel` for a table whose data was written to disk.
    void resetToSingleLevel();
    bool isLowCardinality() const;
    bool isSingleLowCardinalityDictionary() const;

    /// Serialized hash methods; `Aggregator` passes non-nullable `LowCardinality` key columns
    /// to them without materializing full columns first.
    bool isSerialized() const;
    static ColumnsHashing::HashMethodContextPtr createCache(Type type, const ColumnsHashing::HashMethodContextSettings & settings);
    bool topKHeapEverRejected() const;
    /// Whether the active method's top-K heap is inactive for the block about to be processed: it has
    /// already frozen, or `Aggregator::executeImpl` will certainly freeze it before processing row 0 of
    /// that block (see `TopKAggregationHeapBase::willBeInactive`). Conservative: when the shared-boundary
    /// exchange may still keep the heap running, the heap is reported as active.
    bool topKHeapInactive() const;

    /** Select the aggregation method based on the number and types of keys. */
    static Type chooseMethod(const Block & header, const Names & keys, Sizes & out_key_sizes);

private:
    /// The cells of the single-level table grouped by the partitions that `indexCells` assigns to their
    /// hashes: a merge reads the cells of any run of consecutive partitions without walking the whole table
    /// (see `forEachIndexedCell`). The index keeps only the partition numbers, and whoever chose the
    /// partitions maps them back: the adaptive aggregation indexes by the partitions of its layout, which
    /// nest in the buckets of the two-level counterpart, so its merge finds the cells of a bucket by
    /// `AdaptivePartitionLayout::firstPartition`. The index points into the table's buffer, which an
    /// insertion may move, so it serves only while the table takes no more keys: the serial merge, which
    /// inserts into the first table, drops the index of that table first (see `dropCellIndex`), and `init`,
    /// `convertToTwoLevel` and `resetAfterStateOwnershipTransfer`, which replace the table, clear it.
    struct CellIndex
    {
        /// The cells of partition `p` and their hashes are at the positions from `offsets[p]` to
        /// `offsets[p + 1]`.
        PaddedPODArray<void *> cells;
        PaddedPODArray<UInt64> hashes;
        PaddedPODArray<UInt64> offsets;
    };
    std::optional<CellIndex> cell_index;

    /// Indexes the cells of `table` by `partition_of` of their hashes with a counting sort: the cells of a
    /// partition take a contiguous range of positions, in the order of the table. Each hash is computed once:
    /// a table that does not save the hashes computes one for every call of `getHash`, which reads the bytes
    /// of a long string key. The table of a `Nullable` key keeps the null key outside its cells, though
    /// `size` counts it, so the index takes the cells that the table iterates; the merges take the null key
    /// from the table.
    template <typename Table, typename PartitionOf>
    static void indexTableCells(Table & table, size_t num_partitions, PartitionOf && partition_of, CellIndex & index);

    /// Stores the index that `indexCells` built, and counts it.
    void setCellIndex(CellIndex && index);

    [[noreturn]] void throwCannotIndexCells() const;
};

template <typename PartitionOf>
void AggregatedDataVariants::indexCells(size_t num_partitions, PartitionOf && partition_of)
{
    auto index_table = [&]<typename TwoLevelTable>(std::type_identity<TwoLevelTable>, auto & table)
    {
        using Table = std::decay_t<decltype(table)>;
        if constexpr (IterableCells<Table>)
        {
            /// The merges read the cells of a bucket of either kind of table as the cells of the two-level
            /// table.
            static_assert(std::is_same_v<typename Table::cell_type, typename TwoLevelTable::cell_type>);
            CellIndex index;
            indexTableCells(table, num_partitions, partition_of, index);
            setCellIndex(std::move(index));
        }
        else
        {
            throwCannotIndexCells();
        }
    };

    switch (type)
    {
    #define M(NAME) \
        case Type::NAME: \
            index_table(std::type_identity<decltype(NAME ## _two_level)::element_type::Data>{}, (NAME)->data); \
            break;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    #undef M
        default:
            throwCannotIndexCells();
    }
}

template <typename Method, typename Callback>
void AggregatedDataVariants::forEachIndexedCell(size_t first_partition, size_t end_partition, Callback && callback)
{
    using Table = typename Method::Data;
    if constexpr (IterableCells<Table>)
    {
        using Cell = typename Table::cell_type;
        const CellIndex & index = *cell_index;
        for (size_t i = index.offsets[first_partition]; i < index.offsets[end_partition]; ++i)
        {
            Cell & cell = *static_cast<Cell *>(index.cells[i]);
            callback(Cell::getKey(cell.getValue()), cell.getMapped(), index.hashes[i]);
        }
    }
    else
    {
        /// The string table cannot be indexed (see `canIndexCells`), so no indexed table gets here.
        throwCannotIndexCells();
    }
}

template <typename Table, typename PartitionOf>
void AggregatedDataVariants::indexTableCells(Table & table, size_t num_partitions, PartitionOf && partition_of, CellIndex & index)
{
    PaddedPODArray<void *> table_cells(table.size());
    PaddedPODArray<UInt64> table_hashes(table.size());
    PaddedPODArray<UInt64> next;
    next.resize_fill(num_partitions, 0);
    size_t num_cells = 0;
    for (auto it = table.begin(); it != table.end(); ++it, ++num_cells)
    {
        table_cells[num_cells] = &*it;
        table_hashes[num_cells] = it.getHash();
        const size_t partition = partition_of(table_hashes[num_cells]);
        chassert(partition < num_partitions);
        ++next[partition];
    }

    index.offsets.resize(num_partitions + 1);
    UInt64 position = 0;
    for (size_t partition = 0; partition < num_partitions; ++partition)
    {
        index.offsets[partition] = position;
        position += std::exchange(next[partition], position);
    }
    index.offsets[num_partitions] = position;

    index.cells.resize(num_cells);
    index.hashes.resize(num_cells);
    for (size_t cell = 0; cell < num_cells; ++cell)
    {
        const UInt64 i = next[partition_of(table_hashes[cell])]++;
        index.cells[i] = table_cells[cell];
        index.hashes[i] = table_hashes[cell];
    }
}

using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;
using ManyAggregatedDataVariants = std::vector<AggregatedDataVariantsPtr>;
using ManyAggregatedDataVariantsPtr = std::shared_ptr<ManyAggregatedDataVariants>;

/// The two-level twin of a convertible variant type; other types map to themselves.
inline AggregatedDataVariants::Type convertToTwoLevelTypeIfPossible(AggregatedDataVariants::Type type)
{
    using Type = AggregatedDataVariants::Type;
    switch (type)
    {
#define M(NAME) \
    case Type::NAME: \
        return Type::NAME##_two_level;
        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
        default:
            return type;
    }
    UNREACHABLE();
}

}
