#pragma once

#include <mutex>
#include <memory>
#include <functional>

#include <base/StringRef.h>
#include <Common/Arena.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelStringHashMap.h>

#include <Common/ThreadPool.h>
#include <Common/ColumnsHashing.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>

#include <QueryPipeline/SizeLimits.h>

#include <Disks/SingleDiskVolume.h>

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/JIT/compileFunction.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>

/// proton: starts
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Parsers/ASTFunction.h>
#include <Common/HashTable/Hash.h>

#include <numeric>
/// proton: ends

/// This is a copy of `Aggregator.h` and adjust for streaming windows aggregation, we shall keep this file and its implementation file's
/// layouts are as identical as the origins to easy future diff / merge
namespace DB
{
class CompiledAggregateFunctionsHolder;
class NativeWriter;

namespace Streaming
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

enum class ConvertAction : uint8_t
{
    UNKNOWN = 0,
    DISTRIBUTED_MERGE = 1,
    WRITE_TO_TEMP_FS = 2,
    // CHECKPOINT = 3,
    STREAMING_EMIT = 4,
    INTERNAL_MERGE = 5
};

/// using TimeBucketAggregatedDataWithUInt16Key = TimeBucketHashMap<FixedImplicitZeroHashMap<UInt16, AggregateDataPtr>>;
/// using TimeBucketAggregatedDataWithUInt32Key = TimeBucketHashMap<HashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>>;
/// using TimeBucketAggregatedDataWithUInt64Key = TimeBucketHashMap<HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>>;
/// using TimeBucketAggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
/// using TimeBucketAggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

/// Single key
// using TimeBucketAggregatedDataWithUInt16KeyTwoLevel = TimeBucketHashMap<UInt16, AggregateDataPtr, HashCRC32<UInt16>>;
// using TimeBucketAggregatedDataWithUInt32KeyTwoLevel = TimeBucketHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
// using TimeBucketAggregatedDataWithUInt64KeyTwoLevel = TimeBucketHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
// using TimeBucketAggregatedDataWithStringKeyTwoLevel = TimeBucketHashMapWithSavedHash<StringRef, AggregateDataPtr>;

/// Multiple keys
// using TimeBucketAggregatedDataWithKeys128TwoLevel = TimeBucketHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
// using TimeBucketAggregatedDataWithKeys256TwoLevel = TimeBucketHashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

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
    Arenas aggregates_pools;
    Arena * aggregates_pool{};    /// The pool that is currently used for allocation.

    /** Specialization for the case when there are no keys, and for keys not fitted into max_rows_to_group_by.
      */
    AggregatedDataWithoutKey without_key = nullptr;

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
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKey>>                serialized;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyTwoLevel>> key32_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyTwoLevel>> key64_two_level;
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>>       key_string_two_level;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>>  key_fixed_string_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32KeyTwoLevel>>           keys32_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyTwoLevel>>           keys64_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel>>           keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel>>           keys256_two_level;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel>>        serialized_two_level;

    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyHash64>>   key64_hash64;
    std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKeyHash64>>              key_string_hash64;
    std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKeyHash64>>         key_fixed_string_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128Hash64>>             keys128_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256Hash64>>             keys256_hash64;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyHash64>>          serialized_hash64;

    /// Support for nullable keys.
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

    /// proton: starts
    /// Single key
    // std::unique_ptr<AggregationMethodOneNumber<UInt16, TimeBucketAggregatedDataWithUInt64KeyTwoLevel>> time_bucket_key16_two_level;
    // std::unique_ptr<AggregationMethodOneNumber<UInt32, TimeBucketAggregatedDataWithUInt64KeyTwoLevel>> time_bucket_key32_two_level;
    // std::unique_ptr<AggregationMethodOneNumber<UInt64, TimeBucketAggregatedDataWithUInt64KeyTwoLevel>> time_bucket_key64_two_level;

    /// Multiple keys
    // std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithUInt32KeyTwoLevel>>  time_bucket_keys32_two_level;
    // std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithUInt64KeyTwoLevel>>  time_bucket_keys64_two_level;
    // std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys128TwoLevel>>    time_bucket_keys128_two_level;
    // std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys256TwoLevel>>    time_bucket_keys256_two_level;

    /// Nullable
    // std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys128TwoLevel, true>>  time_bucket_nullable_keys128_two_level;
    // std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys256TwoLevel, true>>  time_bucket_nullable_keys256_two_level;

    /// Low cardinality
//    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, StreamingAggregatedDataWithNullableUInt64KeyTwoLevel>>> streaming_low_cardinality_key32_two_level;
//    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, StreamingAggregatedDataWithNullableUInt64KeyTwoLevel>>> streaming_low_cardinality_key64_two_level;
//    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<StreamingAggregatedDataWithNullableStringKeyTwoLevel>>> streaming_low_cardinality_key_string_two_level;
//    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<StreamingAggregatedDataWithNullableStringKeyTwoLevel>>> streaming_low_cardinality_key_fixed_string_two_level;

    // std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys128TwoLevel, false, true>> time_bucket_low_cardinality_keys128_two_level;
    // std::unique_ptr<AggregationMethodKeysFixed<TimeBucketAggregatedDataWithKeys256TwoLevel, false, true>> time_bucket_low_cardinality_keys256_two_level;

    /// Fallback
    // std::unique_ptr<AggregationMethodSerialized<TimeBucketAggregatedDataWithStringKeyTwoLevel>>  time_bucket_serialized_two_level;
    /// proton: ends

    /// In this and similar macros, the option without_key is not considered.
    #define APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M) \
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
        M(serialized,                 false) \
        M(key32_two_level,            true) \
        M(key64_two_level,            true) \
        M(key_string_two_level,       true) \
        M(key_fixed_string_two_level, true) \
        M(keys32_two_level,          true) \
        M(keys64_two_level,          true) \
        M(keys128_two_level,          true) \
        M(keys256_two_level,          true) \
        M(serialized_two_level,       true) \
        M(key64_hash64,               false) \
        M(key_string_hash64,          false) \
        M(key_fixed_string_hash64,    false) \
        M(keys128_hash64,             false) \
        M(keys256_hash64,             false) \
        M(serialized_hash64,          false) \
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
        /* proton: starts */ \
        /* time bucket two level */ \
        /* M(time_bucket_key16_two_level, true) */ \
        /* M(time_bucket_key32_two_level, true) */ \
        /* M(time_bucket_key64_two_level, true) */ \
        /* M(time_bucket_keys32_two_level, true) */ \
        /* M(time_bucket_keys64_two_level, true) */ \
        /* M(time_bucket_keys128_two_level, true) */ \
        /* M(time_bucket_keys256_two_level, true) */ \
        /* M(time_bucket_nullable_keys128_two_level, true) */ \
        /* M(time_bucket_nullable_keys256_two_level, true) */ \
        /* M(time_bucket_low_cardinality_keys128_two_level, true) */ \
        /* M(time_bucket_low_cardinality_keys256_two_level, true) */ \
        /* M(time_bucket_serialized_two_level, true) */ \
        /* proton: ends. */

    enum class Type
    {
        EMPTY = 0,
        without_key,

    #define M(NAME, IS_TWO_LEVEL) NAME,
        APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
    #undef M
    };
    Type type = Type::EMPTY;

    explicit AggregatedDataVariants() : aggregates_pools(1, std::make_shared<Arena>()), aggregates_pool(aggregates_pools.back().get()) {}
    bool empty() const { return type == Type::EMPTY; }
    void invalidate() { type = Type::EMPTY; }

    ~AggregatedDataVariants();

    #define APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M) \
        M(key32_two_level) \
        M(key64_two_level) \
        M(key_string_two_level) \
        M(key_fixed_string_two_level) \
        M(keys32_two_level) \
        M(keys64_two_level) \
        M(keys128_two_level) \
        M(keys256_two_level) \
        M(serialized_two_level) \
        M(nullable_keys128_two_level) \
        M(nullable_keys256_two_level) \
        M(low_cardinality_key32_two_level) \
        M(low_cardinality_key64_two_level) \
        M(low_cardinality_keys128_two_level) \
        M(low_cardinality_keys256_two_level) \
        M(low_cardinality_key_string_two_level) \
        M(low_cardinality_key_fixed_string_two_level) \

    // #define APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M) \
    //     M(time_bucket_key16_two_level) \
    //     M(time_bucket_key32_two_level) \
    //     M(time_bucket_key64_two_level) \
    //     M(time_bucket_keys32_two_level) \
    //     M(time_bucket_keys64_two_level) \
    //     M(time_bucket_keys128_two_level) \
    //     M(time_bucket_keys256_two_level) \
    //     M(time_bucket_nullable_keys128_two_level) \
    //     M(time_bucket_nullable_keys256_two_level) \
    //     M(time_bucket_low_cardinality_keys128_two_level) \
    //     M(time_bucket_low_cardinality_keys256_two_level) \
    //     M(time_bucket_serialized_two_level)

    #define APPLY_FOR_VARIANTS_ALL_TWO_LEVEL(M) \
        APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M)
        // APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)

    void init(Type type_)
    {
        switch (type_)
        {
            case Type::EMPTY:       break;
            case Type::without_key: break;

        #define M(NAME, IS_TWO_LEVEL) \
            case Type::NAME: NAME = std::make_unique<decltype(NAME)::element_type>(); break;
            APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
        #undef M
        }

        type = type_;

        /// proton: start. Setup window key size since we will need use the size to extract the window key value
        /// and sort the window key in a sorted map for recycle
        // switch (type)
        // {
        // #define M(NAME) \
        //     case Type::NAME: NAME->data.setWinKeySize(key_sizes[0]); break;
        //     APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
        // #undef M

        //     default:
        //         /// Enable arena recycling only for streaming window
        //         /// Disable it for global streaming aggregation
        //         aggregates_pool->enableRecycle(false);
        //         break;
        // }
        /// proton: ends;
    }

    /// Number of rows (different keys).
    size_t size() const
    {
        switch (type)
        {
            case Type::EMPTY:       return 0;
            case Type::without_key: return 1;

        #define M(NAME, IS_TWO_LEVEL) \
            case Type::NAME: return NAME->data.size() + (without_key != nullptr);
            APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
        #undef M
        }

        __builtin_unreachable();
    }

    /// The size without taking into account the row in which data is written for the calculation of TOTALS.
    size_t sizeWithoutOverflowRow() const
    {
        switch (type)
        {
            case Type::EMPTY:       return 0;
            case Type::without_key: return 1;

            #define M(NAME, IS_TWO_LEVEL) \
            case Type::NAME: return NAME->data.size();
            APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
            #undef M
        }

        __builtin_unreachable();
    }

    const char * getMethodName() const
    {
        switch (type)
        {
            case Type::EMPTY:       return "EMPTY";
            case Type::without_key: return "without_key";

        #define M(NAME, IS_TWO_LEVEL) \
            case Type::NAME: return #NAME;
            APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
        #undef M
        }

        __builtin_unreachable();
    }

    bool isTwoLevel() const
    {
        return isStaticBucketTwoLevel();
    }

    bool isStaticBucketTwoLevel() const
    {
        switch (type)
        {
        #define M(NAME) \
            case Type::NAME: return true;
                APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M)
        #undef M
            default: return false;
        }
    }

    // bool isTimeBucketTwoLevel() const
    // {
    //     switch (type)
    //     {
    //     #define M(NAME) \
    //         case Type::NAME: return true;
    //             // APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
    //     #undef M
    //         default: return false;
    //     }
    // }

    #define APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M) \
        M(key32)            \
        M(key64)            \
        M(key_string)       \
        M(key_fixed_string) \
        M(keys32)           \
        M(keys64)           \
        M(keys128)          \
        M(keys256)          \
        M(serialized)       \
        M(nullable_keys128) \
        M(nullable_keys256) \
        M(low_cardinality_key32) \
        M(low_cardinality_key64) \
        M(low_cardinality_keys128) \
        M(low_cardinality_keys256) \
        M(low_cardinality_key_string) \
        M(low_cardinality_key_fixed_string) \

    #define APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M) \
        M(key8)             \
        M(key16)            \
        M(keys16)           \
        M(key64_hash64)     \
        M(key_string_hash64)\
        M(key_fixed_string_hash64) \
        M(keys128_hash64)   \
        M(keys256_hash64)   \
        M(serialized_hash64) \
        M(low_cardinality_key8) \
        M(low_cardinality_key16) \

    #define APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M) \
        APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M) \
        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M) \

    bool isConvertibleToTwoLevel() const
    {
        switch (type)
        {
        #define M(NAME) \
            case Type::NAME: return true;

            APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M)

        #undef M
            default:
                return false;
        }
    }

    void convertToTwoLevel();

    /// proton: starts
    #define APPLY_FOR_LOW_CARDINALITY_VARIANTS_STREAMING(M) \
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
        // M(time_bucket_low_cardinality_keys128_two_level) \
        // M(time_bucket_low_cardinality_keys256_two_level) \

    /// proton ends
    bool isLowCardinality() const
    {
        switch (type)
        {
        #define M(NAME) \
            case Type::NAME: return true;

            APPLY_FOR_LOW_CARDINALITY_VARIANTS_STREAMING(M)
        #undef M
            default:
                return false;
        }
    }

    static HashMethodContextPtr createCache(Type type, const HashMethodContext::Settings & settings)
    {
        switch (type)
        {
            case Type::without_key: return nullptr;

            #define M(NAME, IS_TWO_LEVEL) \
            case Type::NAME: \
            { \
                using TPtr ## NAME = decltype(AggregatedDataVariants::NAME); \
                using T ## NAME = typename TPtr ## NAME ::element_type; \
                return T ## NAME ::State::createContext(settings); \
            }

            APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
            #undef M

            default:
                throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
        }
    }
};

using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;
using ManyAggregatedDataVariants = std::vector<AggregatedDataVariantsPtr>;
using ManyAggregatedDataVariantsPtr = std::shared_ptr<ManyAggregatedDataVariants>;

/** How are "total" values calculated with WITH TOTALS?
  * (For more details, see TotalsHavingTransform.)
  *
  * In the absence of group_by_overflow_mode = 'any', the data is aggregated as usual, but the states of the aggregate functions are not finalized.
  * Later, the aggregate function states for all rows (passed through HAVING) are merged into one - this will be TOTALS.
  *
  * If there is group_by_overflow_mode = 'any', the data is aggregated as usual, except for the keys that did not fit in max_rows_to_group_by.
  * For these keys, the data is aggregated into one additional row - see below under the names `overflow_row`, `overflows`...
  * Later, the aggregate function states for all rows (passed through HAVING) are merged into one,
  *  also overflow_row is added or not added (depending on the totals_mode setting) also - this will be TOTALS.
  */


/** Aggregates the source of the blocks.
  */
class Aggregator final
{
public:
    struct Params
    {
        /// Data structure of source blocks.
        Block src_header;
        /// Data structure of intermediate blocks before merge.
        Block intermediate_header;

        /// What to count.
        const Names keys;
        const AggregateDescriptions aggregates;
        const size_t keys_size;
        const size_t aggregates_size;

        /// The settings of approximate calculation of GROUP BY.
        const bool overflow_row;    /// Do we need to put into AggregatedDataVariants::without_key aggregates for keys that are not in max_rows_to_group_by.
        const size_t max_rows_to_group_by;
        const OverflowMode group_by_overflow_mode;

        /// Two-level aggregation settings (used for a large number of keys).
        /** With how many keys or the size of the aggregation state in bytes,
          *  two-level aggregation begins to be used. Enough to reach of at least one of the thresholds.
          * 0 - the corresponding threshold is not specified.
          */
        size_t group_by_two_level_threshold;
        size_t group_by_two_level_threshold_bytes;

        /// Settings to flush temporary data to the filesystem (external aggregation).
        const size_t max_bytes_before_external_group_by;        /// 0 - do not use external aggregation.

        /// Return empty result when aggregating without keys on empty set.
        bool empty_result_for_aggregation_by_empty_set;

        VolumePtr tmp_volume;

        /// Settings is used to determine cache size. No threads are created.
        size_t max_threads;

        const size_t min_free_disk_space;

        bool compile_aggregate_expressions;
        size_t min_count_to_compile_aggregate_expression;

        /// proton: starts
        /// `keep_state` tell Aggregator if it needs to hold state in-memory for streaming
        /// processing. In normal case, it is true. However for global over global aggregation
        /// etc cases, we don't want the outer global aggregation to accumulate states in-memory.
        /// Actually things are complex when we support `EMIT CHANGELOG`, in which case `keep_state`
        /// shall always be `true` as `EMIT CHANGELOG` is expected to retract the previous state.
        /// There is another case we will set keep_state to false : when we cancel
        bool keep_state = true;
        /// How many streaming windows to keep from recycling
        // size_t streaming_window_count = 0;

        /// GroupBy tells if the first group column is either WINDOW_START or WINDOW_END or
        /// anything else
        enum class GroupBy
        {
            // WINDOW_START,
            // WINDOW_END,
            // USER_DEFINED,
            OTHER,
        };
        GroupBy group_by = GroupBy::OTHER;

        // ssize_t delta_col_pos;

        // size_t window_keys_num;

        // WindowParamsPtr window_params;
        /// proton: ends

        /// proton: starts
        Params(
            const Block & src_header_,
            const Names & keys_, const AggregateDescriptions & aggregates_,
            bool overflow_row_, size_t max_rows_to_group_by_, OverflowMode group_by_overflow_mode_,
            size_t group_by_two_level_threshold_, size_t group_by_two_level_threshold_bytes_,
            size_t max_bytes_before_external_group_by_,
            bool empty_result_for_aggregation_by_empty_set_,
            VolumePtr tmp_volume_, size_t max_threads_,
            size_t min_free_disk_space_,
            bool compile_aggregate_expressions_,
            size_t min_count_to_compile_aggregate_expression_,
            const Block & intermediate_header_ = {},
            bool keep_state_ = true,
            GroupBy streaming_group_by_ = GroupBy::OTHER)
        : src_header(src_header_),
            intermediate_header(intermediate_header_),
            keys(keys_), aggregates(aggregates_), keys_size(keys.size()), aggregates_size(aggregates.size()),
            overflow_row(overflow_row_), max_rows_to_group_by(max_rows_to_group_by_), group_by_overflow_mode(group_by_overflow_mode_),
            group_by_two_level_threshold(group_by_two_level_threshold_), group_by_two_level_threshold_bytes(group_by_two_level_threshold_bytes_),
            max_bytes_before_external_group_by(max_bytes_before_external_group_by_),
            empty_result_for_aggregation_by_empty_set(empty_result_for_aggregation_by_empty_set_),
            tmp_volume(tmp_volume_), max_threads(max_threads_),
            min_free_disk_space(min_free_disk_space_),
            compile_aggregate_expressions(compile_aggregate_expressions_),
            min_count_to_compile_aggregate_expression(min_count_to_compile_aggregate_expression_),
            keep_state(keep_state_),
            group_by(streaming_group_by_)
        {
        }
        /// proton: ends

        /// Only parameters that matter during merge.
        Params(const Block & intermediate_header_,
            const Names & keys_, const AggregateDescriptions & aggregates_, bool overflow_row_, size_t max_threads_)
            : Params(Block(), keys_, aggregates_, overflow_row_, 0, OverflowMode::THROW, 0, 0, 0, false, nullptr, max_threads_, 0, false, 0)
        {
            intermediate_header = intermediate_header_;
        }

        static Block getHeader(
            const Block & src_header,
            const Block & intermediate_header,
            const Names & keys,
            const AggregateDescriptions & aggregates,
            bool final);

        Block getHeader(bool final) const { return getHeader(src_header, intermediate_header, keys, aggregates, final); }

        /// Returns keys and aggregated for EXPLAIN query
        void explain(WriteBuffer & out, size_t indent) const;
        void explain(JSONBuilder::JSONMap & map) const;
    };

    explicit Aggregator(const Params & params_);

    using AggregateColumns = std::vector<ColumnRawPtrs>;
    using AggregateColumnsData = std::vector<ColumnAggregateFunction::Container *>;
    using AggregateColumnsConstData = std::vector<const ColumnAggregateFunction::Container *>;
    using AggregateFunctionsPlainPtrs = std::vector<const IAggregateFunction *>;

    /// Process one block. Return {should_abort, need_finalization} pair
    /// should_abort: if the processing should be aborted (with group_by_overflow_mode = 'break') return true, otherwise false.
    /// need_finalization : only for UDA aggregation. If there is no UDA, always false
    std::pair<bool, bool> executeOnBlock(const Block & block,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys) const;

    std::pair<bool, bool> executeOnBlock(Columns columns,
        size_t row_begin, size_t row_end,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns, AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys) const;

    bool mergeOnBlock(Block block, AggregatedDataVariants & result, bool & no_more_keys) const;

    /** Convert the aggregation data structure into a block.
      * If overflow_row = true, then aggregates for rows that are not included in max_rows_to_group_by are put in the first block.
      *
      * If final = false, then ColumnAggregateFunction is created as the aggregation columns with the state of the calculations,
      *  which can then be combined with other states (for distributed query processing or checkpoint).
      * If final = true, then columns with ready values are created as aggregate columns.
      *
      * For streaming processing, the internal aggregate state may be pruned or kept depending on different scenarios
      * 1. During checkpointing, never prune the aggregate states, `keep_state = true` in this case
      * 2. In `EMIT changelog` case, never prune the states. Examples
      *    a. SELECT count(), avg(i), sum(k) FROM my_stream EMIT changelog;
      *    b. SELECT count(), avg(i), sum(k) FROM (
      *         SELECT avg(i) AS i, sum(k) AS k FROM my_stream GROUP BY device_id) EMIT changelog;
      * 3. In `non emit changelog` and `non checkpoint` scenario
      *    i. For first level global aggregation, never prune the aggregate states. Examples
      *       a. SELECT count(), avg(i), sum(k) FROM my_stream; <-- first level global aggr
      *       b. SELECT count(), avg(i), sum(k) FROM ( <-- first level global aggr
      *            SELECT window_start, avg(i) AS i, sum(k) AS k FROM tumble(my_stream, 5s) GROUP BY window_start);
      *    ii. For non-first level global aggregation, always prune the states. Examples
      *       a. SELECT count(), avg(i), sum(k) FROM ( <-- second level global aggr, need prune its state at this level
      *            SELECT avg(i) AS i, sum(k) AS k FROM my_stream GROUP BY device_id <-- first level global aggr, don't prune states
      *          );
      */
    BlocksList convertToBlocks(AggregatedDataVariants & data_variants, bool final, ConvertAction action, size_t max_threads) const;

    BlocksList convertToBlocksFinal(AggregatedDataVariants & data_variants, ConvertAction action, size_t max_threads) const
    {
        return convertToBlocks(data_variants, true, action, max_threads);
    }

    BlocksList convertToBlocksIntermediate(AggregatedDataVariants & data_variants, ConvertAction action, size_t max_threads) const
    {
        return convertToBlocks(data_variants, false, action, max_threads);
    }

    Block convertOneBucketToBlockFinal(AggregatedDataVariants & data_variants, ConvertAction action, size_t bucket) const;
    Block convertOneBucketToBlockIntermediate(AggregatedDataVariants & data_variants, ConvertAction action, size_t bucket) const;

    ManyAggregatedDataVariantsPtr prepareVariantsToMerge(ManyAggregatedDataVariants & data_variants) const;

    using BucketToBlocks = std::map<Int32, BlocksList>;
    /// Merge partially aggregated blocks separated to buckets into one data structure.
    void mergeBlocks(BucketToBlocks bucket_to_blocks, AggregatedDataVariants & result, size_t max_threads);

    /// Merge several partially aggregated blocks into one.
    /// Precondition: for all blocks block.info.is_overflows flag must be the same.
    /// (either all blocks are from overflow data or none blocks are).
    /// The resulting block has the same value of is_overflows flag.
    Block mergeBlocks(BlocksList & blocks, bool final, ConvertAction action);

    /** Split block with partially-aggregated data to many blocks, as if two-level method of aggregation was used.
      * This is needed to simplify merging of that data with other results, that are already two-level.
      */
    std::vector<Block> convertBlockToTwoLevel(const Block & block) const;

    void initStatesForWithoutKeyOrOverflow(AggregatedDataVariants & data_variants) const;

    /// For external aggregation.
    void writeToTemporaryFile(AggregatedDataVariants & data_variants, const String & tmp_path) const;
    void writeToTemporaryFile(AggregatedDataVariants & data_variants) const;

    bool hasTemporaryFiles() const { return !temporary_files.empty(); }

    struct TemporaryFiles
    {
        std::vector<std::unique_ptr<Poco::TemporaryFile>> files;
        size_t sum_size_uncompressed = 0;
        size_t sum_size_compressed = 0;
        mutable std::mutex mutex;

        bool empty() const
        {
            std::lock_guard lock(mutex);
            return files.empty();
        }
    };

    const TemporaryFiles & getTemporaryFiles() const { return temporary_files; }

    /// Get data structure of the result.
    Block getHeader(bool final) const;

    /// proton: starts
    Params & getParams() { return params; }
    /// proton: ends

private:

    friend struct AggregatedDataVariants;
    friend class ConvertingAggregatedToChunksTransform;
    friend class ConvertingAggregatedToChunksSource;
    friend class AggregatingInOrderTransform;

    /// proton: starts
    friend class StreamingConvertingAggregatedToChunksTransform;
    friend class StreamingConvertingAggregatedToChunksSource;
    friend class AggregatingTransform;
    friend class GlobalAggregatingTransform;
    // friend class WindowAggregatingTransform;
    // friend class WindowAggregatingTransformWithSubstream;
    // friend class TumbleAggregatingTransform;
    // friend class TumbleAggregatingTransformWithSubstream;
    // friend class HopAggregatingTransform;
    // friend class HopAggregatingTransformWithSubstream;
    // friend class SessionAggregatingTransform;
    // friend class SessionAggregatingTransformWithSubstream;
    // friend class UserDefinedEmitStrategyAggregatingTransform;

    // mutable std::optional<VersionType> version;
    /// proton: ends

    /// Positions of aggregation key columns in the header.
    const ColumnNumbers keys_positions;
    Params params;

    AggregatedDataVariants::Type method_chosen;
    Sizes key_sizes;

    HashMethodContextPtr aggregation_state_cache;

    AggregateFunctionsPlainPtrs aggregate_functions;

    /** This array serves two purposes.
      *
      * Function arguments are collected side by side, and they do not need to be collected from different places. Also the array is made zero-terminated.
      * The inner loop (for the case without_key) is almost twice as compact; performance gain of about 30%.
      */
    struct AggregateFunctionInstruction
    {
        const IAggregateFunction * that{};
        size_t state_offset{};
        const IColumn ** arguments{};
        const IAggregateFunction * batch_that{};
        const IColumn ** batch_arguments{};
        /// proton : starts
        // const IColumn * delta_column{};
        /// proton : ends
        const UInt64 * offsets{};
    };

    using AggregateFunctionInstructions = std::vector<AggregateFunctionInstruction>;
    using NestedColumnsHolder = std::vector<std::vector<const IColumn *>>;

    Sizes offsets_of_aggregate_states;    /// The offset to the n-th aggregate function in a row of aggregate functions.
    size_t total_size_of_aggregate_states = 0;    /// The total size of the row from the aggregate functions.

    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;

    bool all_aggregates_has_trivial_destructor = false;

    /// How many RAM were used to process the query before processing the first block.
    Int64 memory_usage_before_aggregation = 0;

    Poco::Logger * log;

    /// For external aggregation.
    mutable TemporaryFiles temporary_files;

// #if USE_EMBEDDED_COMPILER
#if 0
    std::shared_ptr<CompiledAggregateFunctionsHolder> compiled_aggregate_functions_holder;
#endif

    std::vector<bool> is_aggregate_function_compiled;

    /** Try to compile aggregate functions.
      */
    void compileAggregateFunctionsIfNeeded();

    /** Select the aggregation method based on the number and types of keys. */
    AggregatedDataVariants::Type chooseAggregationMethod();

    /// proton: starts
    // AggregatedDataVariants::Type chooseAggregationMethodTimeBucketTwoLevel(
    //     const DataTypes & types_removed_nullable, bool has_nullable_key,
    //     bool has_low_cardinality, size_t num_fixed_contiguous_keys, size_t keys_bytes) const;
    /// proton: ends

    /** Create states of aggregate functions for one key.
      */
    template <bool skip_compiled_aggregate_functions = false>
    void createAggregateStates(AggregateDataPtr & aggregate_data) const;

    /** Call `destroy` methods for states of aggregate functions.
      * Used in the exception handler for aggregation, since RAII in this case is not applicable.
      */
    void destroyAllAggregateStates(AggregatedDataVariants & result) const;

    bool executeImpl(
        AggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys,
        AggregateDataPtr overflow_row = nullptr) const;

    /// Process one data block, aggregate the data into a hash table.
    template <typename Method>
    bool executeImpl(
        Method & method,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys,
        AggregateDataPtr overflow_row) const;

    /// Specialization for a particular value no_more_keys.
    template <bool no_more_keys, bool use_compiled_functions, typename Method>
    bool executeImplBatch(
        Method & method,
        typename Method::State & state,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        AggregateDataPtr overflow_row) const;

    /// For case when there are no keys (all aggregate into one row). For UDA with own strategy, return 'true' means the UDA should emit after execution
    template <bool use_compiled_functions>
    bool executeWithoutKeyImpl(
        AggregatedDataWithoutKey & res,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        Arena * arena) const;

    static void executeOnIntervalWithoutKeyImpl(
        AggregatedDataWithoutKey & res,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        Arena * arena/* ,
        const IColumn * delta_col */);

    template <typename Method>
    void writeToTemporaryFileImpl(
        AggregatedDataVariants & data_variants,
        Method & method,
        NativeWriter & out) const;

    /// Merge NULL key data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataNullKey(
            Table & table_dst,
            Table & table_src,
            Arena * arena,
            bool clear_states) const;

    /// Merge data from hash table `src` into `dst`.
    using EmptyKeyHandler = void *;
    template <typename Method, bool use_compiled_functions, typename Table, typename KeyHandler = EmptyKeyHandler>
    void mergeDataImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena,
        bool clear_states,
        KeyHandler && key_handler = nullptr) const;

    /// Merge data from hash table `src` into `dst`, but only for keys that already exist in dst. In other cases, merge the data into `overflows`.
    template <typename Method, typename Table>
    void mergeDataNoMoreKeysImpl(
        Table & table_dst,
        AggregatedDataWithoutKey & overflows,
        Table & table_src,
        Arena * arena,
        bool clear_states) const;

    /// Same, but ignores the rest of the keys.
    template <typename Method, typename Table>
    void mergeDataOnlyExistingKeysImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena,
        bool clear_states) const;

    void mergeWithoutKeyDataImpl(
        ManyAggregatedDataVariants & non_empty_data) const;

    template <typename Method>
    void mergeSingleLevelDataImpl(
        ManyAggregatedDataVariants & non_empty_data, ConvertAction action) const;

    template <typename Method, typename Table>
    void convertToBlockImpl(
        Method & method,
        Table & data,
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        Arena * arena,
        bool final,
        ConvertAction action) const;

    template <typename Mapped>
    void insertAggregatesIntoColumns(
        Mapped & mapped,
        MutableColumns & final_aggregate_columns,
        Arena * arena) const;

    template <typename Method, bool use_compiled_functions, typename Table>
    void convertToBlockImplFinal(
        Method & method,
        Table & data,
        std::vector<IColumn *> key_columns,
        MutableColumns & final_aggregate_columns,
        Arena * arena,
        ConvertAction action) const;

    template <typename Method, typename Table>
    void convertToBlockImplNotFinal(
        Method & method,
        Table & data,
        std::vector<IColumn *>  key_columns,
        AggregateColumnsData & aggregate_columns) const;

    template <typename Filler>
    Block prepareBlockAndFill(
        AggregatedDataVariants & data_variants,
        bool final,
        ConvertAction action,
        size_t rows,
        Filler && filler) const;

    template <typename Method>
    Block convertOneBucketToBlock(
        AggregatedDataVariants & data_variants,
        Method & method,
        Arena * arena,
        bool final,
        ConvertAction action,
        size_t bucket) const;

    Block mergeAndConvertOneBucketToBlock(
        ManyAggregatedDataVariants & variants,
        Arena * arena,
        bool final,
        ConvertAction action,
        size_t bucket,
        std::atomic<bool> * is_cancelled = nullptr) const;

    /// proton: starts.
    // template <typename Method>
    // void spliceBucketsImpl(
    //     AggregatedDataVariants & data_dest,
    //     AggregatedDataVariants & data_src,
    //     bool final,
    //     ConvertAction action,
    //     const std::vector<Int64> & gcd_buckets,
    //     Arena * arena) const;

    /// Used for hop window function, merge multiple gcd windows (buckets) to a hop window
    /// For examples:
    ///   gcd_bucket1 - [00:00, 00:02)
    ///                            =>  result block - [00:00, 00:04)
    ///   gcd_bucket2 - [00:02, 00:04)
    // Block spliceAndConvertBucketsToBlock(
    //     AggregatedDataVariants & variants, bool final, ConvertAction action, const std::vector<Int64> & gcd_buckets) const;

    // void mergeBuckets(
    //     ManyAggregatedDataVariants & variants, Arena * arena, bool final, ConvertAction action, const std::vector<Int64> & buckets) const;
    /// proton: ends.

    Block prepareBlockAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows, ConvertAction action) const;
    Block prepareBlockAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final, ConvertAction action) const;
    BlocksList prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, size_t max_threads, ConvertAction action) const;

    template <typename Method>
    BlocksList prepareBlocksAndFillTwoLevelImpl(
        AggregatedDataVariants & data_variants,
        Method & method,
        bool final,
        ConvertAction action,
        ThreadPool * thread_pool) const;

    template <bool no_more_keys, typename Method, typename Table>
    void mergeStreamsImplCase(
        Block & block,
        Arena * aggregates_pool,
        Method & method,
        Table & data,
        AggregateDataPtr overflow_row) const;

    template <typename Method, typename Table>
    void mergeStreamsImpl(
        Block & block,
        Arena * aggregates_pool,
        Method & method,
        Table & data,
        AggregateDataPtr overflow_row,
        bool no_more_keys) const;

    void mergeWithoutKeyStreamsImpl(
        Block & block,
        AggregatedDataVariants & result) const;

    template <typename Method>
    void mergeBucketImpl(
        ManyAggregatedDataVariants & data, bool final, ConvertAction action, size_t bucket, Arena * arena, std::atomic<bool> * is_cancelled = nullptr) const;

    template <typename Method>
    void convertBlockToTwoLevelImpl(
        Method & method,
        Arena * pool,
        ColumnRawPtrs & key_columns,
        const Block & source,
        std::vector<Block> & destinations) const;

    template <typename Method, typename Table>
    void destroyImpl(Table & table) const;

    void destroyWithoutKey(
        AggregatedDataVariants & result) const;


    /** Checks constraints on the maximum number of keys for aggregation.
      * If it is exceeded, then, depending on the group_by_overflow_mode, either
      * - throws an exception;
      * - returns false, which means that execution must be aborted;
      * - sets the variable no_more_keys to true.
      */
    bool checkLimits(size_t result_size, bool & no_more_keys) const;

    void prepareAggregateInstructions(
        Columns columns,
        AggregateColumns & aggregate_columns,
        Columns & materialized_columns,
        AggregateFunctionInstructions & instructions,
        NestedColumnsHolder & nested_columns_holder) const;

    void addSingleKeyToAggregateColumns(
        const AggregatedDataVariants & data_variants,
        MutableColumns & aggregate_columns) const;

    void addArenasToAggregateColumns(
        const AggregatedDataVariants & data_variants,
        MutableColumns & aggregate_columns) const;

    void createStatesAndFillKeyColumnsWithSingleKey(
        AggregatedDataVariants & data_variants,
        Columns & key_columns, size_t key_row,
        MutableColumns & final_key_columns) const;

    /// proton: starts
    void setupAggregatesPoolTimestamps(size_t row_begin, size_t row_end, const ColumnRawPtrs & key_columns, Arena * aggregates_pool) const;
    void removeBucketsBefore(AggregatedDataVariants & result, Int64 max_bucket) const;
    std::vector<Int64> bucketsBefore(const AggregatedDataVariants & result, Int64 max_bucket) const;

    inline bool shouldClearStates(ConvertAction action, bool final_) const;

    // VersionType getVersionFromRevision(UInt64 revision) const;
    // VersionType getVersion() const;

// public:
    // void checkpoint(const AggregatedDataVariants & data_variants, WriteBuffer & wb);
    // void recover(AggregatedDataVariants & data_variants, ReadBuffer & rb);
    // void recoverStates(AggregatedDataVariants & data_variants, BlocksList & blocks);
    // void recoverStatesWithoutKey(AggregatedDataVariants & data_variants, BlocksList & blocks);
    // void recoverStatesSingleLevel(AggregatedDataVariants & data_variants, BlocksList & blocks);
    // void recoverStatesTwoLevel(AggregatedDataVariants & data_variants, BlocksList & blocks);

    // template <typename Method>
    // void doRecoverStates(Method & method, Arena * aggregates_pool, Block & block);
    /// proton: ends
};


/** Get the aggregation variant by its type. */
template <typename Method> Method & getDataVariant(AggregatedDataVariants & variants);

#define M(NAME, IS_TWO_LEVEL) \
    template <> inline decltype(AggregatedDataVariants::NAME)::element_type & getDataVariant<decltype(AggregatedDataVariants::NAME)::element_type>(AggregatedDataVariants & variants) { return *variants.NAME; }

APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)

#undef M
}
}
