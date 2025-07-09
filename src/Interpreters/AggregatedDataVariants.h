#pragma once
#include <Common/ColumnsHashing.h>
#include <Interpreters/AggregatedData.h>
#include <Interpreters/AggregationMethod.h>

#include <memory>
#include <boost/noncopyable.hpp>


namespace DB
{
class Arena;
class Aggregator;

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
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithShortStringKey>>               key_string;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKey>>          key_fixed_string;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt16Key, false, false, false>>  keys16;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32Key>>                   keys32;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64Key>>                   keys64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128>>                   keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256>>                   keys256;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKey>>                          serialized;
    std::unique_ptr<AggregationMethodNullableSerialized<AggregatedDataWithStringKey>>                  nullable_serialized;
    std::unique_ptr<AggregationMethodPreallocSerialized<AggregatedDataWithStringKey>>                  prealloc_serialized;
    std::unique_ptr<AggregationMethodNullablePreallocSerialized<AggregatedDataWithStringKey>>          nullable_prealloc_serialized;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyTwoLevel>> key32_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyTwoLevel>> key64_two_level;
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>>       key_string_two_level;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>>  key_fixed_string_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32KeyTwoLevel>>           keys32_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyTwoLevel>>           keys64_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel>>           keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel>>           keys256_two_level;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel>>                  serialized_two_level;
    std::unique_ptr<AggregationMethodNullableSerialized<AggregatedDataWithStringKeyTwoLevel>>          nullable_serialized_two_level;
    std::unique_ptr<AggregationMethodPreallocSerialized<AggregatedDataWithStringKeyTwoLevel>>          prealloc_serialized_two_level;
    std::unique_ptr<AggregationMethodNullablePreallocSerialized<AggregatedDataWithStringKeyTwoLevel>>  nullable_prealloc_serialized_two_level;

    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyHash64>>   key64_hash64;
    std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKeyHash64>>              key_string_hash64;
    std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKeyHash64>>         key_fixed_string_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128Hash64>>             keys128_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256Hash64>>             keys256_hash64;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyHash64>>                  serialized_hash64;
    std::unique_ptr<AggregationMethodNullableSerialized<AggregatedDataWithStringKeyHash64>>          nullable_serialized_hash64;
    std::unique_ptr<AggregationMethodPreallocSerialized<AggregatedDataWithStringKeyHash64>>          prealloc_serialized_hash64;
    std::unique_ptr<AggregationMethodNullablePreallocSerialized<AggregatedDataWithStringKeyHash64>>  nullable_prealloc_serialized_hash64;

    /// Support for nullable keys.
    std::unique_ptr<AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false, true>>         nullable_key8;
    std::unique_ptr<AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key, false, true>>         nullable_key16;
    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32Key, true, true>>         nullable_key32;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key, true, true>>         nullable_key64;
    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32KeyTwoLevel, true, true>>         nullable_key32_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyTwoLevel, true, true>>         nullable_key64_two_level;

    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithNullableShortStringKey, true>> nullable_key_string;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithNullableShortStringKey, true>> nullable_key_fixed_string;
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithNullableShortStringKeyTwoLevel, true>> nullable_key_string_two_level;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithNullableShortStringKeyTwoLevel, true>> nullable_key_fixed_string_two_level;

    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128, true>>             nullable_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256, true>>             nullable_keys256;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel, true>>     nullable_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel, true>>     nullable_keys256_two_level;

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

    /// In this and similar macros, the option without_key is not considered.
    #define APPLY_FOR_AGGREGATED_VARIANTS(M) \
        M(key8,                       false) \
        M(key16,                      false) \
        M(key32,                      false) \
        M(key64,                      false) \
        M(key_string,                 false) \
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
        M(key32_two_level,            true) \
        M(key64_two_level,            true) \
        M(key_string_two_level,       true) \
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
        M(key_string_hash64,          false) \
        M(key_fixed_string_hash64,    false) \
        M(keys128_hash64,             false) \
        M(keys256_hash64,             false) \
        M(serialized_hash64,                   false) \
        M(nullable_serialized_hash64,          false) \
        M(prealloc_serialized_hash64,          false) \
        M(nullable_prealloc_serialized_hash64, false) \
        M(nullable_key8,             false) \
        M(nullable_key16,             false) \
        M(nullable_key32,             false) \
        M(nullable_key64,             false) \
        M(nullable_key32_two_level,   true) \
        M(nullable_key64_two_level,   true) \
        M(nullable_key_string,        false) \
        M(nullable_key_fixed_string,  false) \
        M(nullable_key_string_two_level, true) \
        M(nullable_key_fixed_string_two_level, true) \
        M(nullable_keys128,           false) \
        M(nullable_keys256,           false) \
        M(nullable_keys128_two_level, true) \
        M(nullable_keys256_two_level, true) \
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

    #define APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M) \
        M(key32)            \
        M(key64)            \
        M(key_string)       \
        M(key_fixed_string) \
        M(keys32)           \
        M(keys64)           \
        M(keys128)          \
        M(keys256)          \
        M(serialized)       \
        M(nullable_serialized) \
        M(prealloc_serialized) \
        M(nullable_prealloc_serialized) \
        M(nullable_key32) \
        M(nullable_key64) \
        M(nullable_key_string) \
        M(nullable_key_fixed_string) \
        M(nullable_keys128) \
        M(nullable_keys256) \
        M(low_cardinality_key32) \
        M(low_cardinality_key64) \
        M(low_cardinality_keys128) \
        M(low_cardinality_keys256) \
        M(low_cardinality_key_string) \
        M(low_cardinality_key_fixed_string) \

    /// NOLINTNEXTLINE
    #define APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M) \
        M(key8)             \
        M(key16)            \
        M(nullable_key8) \
        M(nullable_key16) \
        M(keys16)           \
        M(key64_hash64)     \
        M(key_string_hash64)\
        M(key_fixed_string_hash64) \
        M(keys128_hash64)   \
        M(keys256_hash64)   \
        M(serialized_hash64) \
        M(nullable_serialized_hash64) \
        M(prealloc_serialized_hash64) \
        M(nullable_prealloc_serialized_hash64) \
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
        M(key_string_two_level)       \
        M(key_fixed_string_two_level) \
        M(keys32_two_level)           \
        M(keys64_two_level)           \
        M(keys128_two_level)          \
        M(keys256_two_level)          \
        M(serialized_two_level)       \
        M(nullable_serialized_two_level)       \
        M(prealloc_serialized_two_level)       \
        M(nullable_prealloc_serialized_two_level)       \
        M(nullable_key32_two_level) \
        M(nullable_key64_two_level) \
        M(nullable_key_string_two_level) \
        M(nullable_key_fixed_string_two_level) \
        M(nullable_keys128_two_level) \
        M(nullable_keys256_two_level) \
        M(low_cardinality_key32_two_level) \
        M(low_cardinality_key64_two_level) \
        M(low_cardinality_keys128_two_level) \
        M(low_cardinality_keys256_two_level) \
        M(low_cardinality_key_string_two_level) \
        M(low_cardinality_key_fixed_string_two_level) \

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
        M(low_cardinality_key_fixed_string_two_level)

    enum class Type : uint8_t
    {
        EMPTY = 0,
        without_key,

    #define M(NAME, IS_TWO_LEVEL) NAME,
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    };
    Type type = Type::EMPTY;
    AggregatedDataVariants();
    ~AggregatedDataVariants();
    bool empty() const { return type == Type::EMPTY; }
    void invalidate() { type = Type::EMPTY; }
    void init(Type type_, std::optional<size_t> size_hint = std::nullopt);
    /// Number of rows (different keys).
    size_t size() const;
    size_t sizeWithoutOverflowRow() const;
    const char * getMethodName() const;
    bool isTwoLevel() const;
    bool isConvertibleToTwoLevel() const;
    void convertToTwoLevel();
    bool isLowCardinality() const;
    static ColumnsHashing::HashMethodContextPtr createCache(Type type, const ColumnsHashing::HashMethodContext::Settings & settings);

};

using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;
using ManyAggregatedDataVariants = std::vector<AggregatedDataVariantsPtr>;
using ManyAggregatedDataVariantsPtr = std::shared_ptr<ManyAggregatedDataVariants>;
}
