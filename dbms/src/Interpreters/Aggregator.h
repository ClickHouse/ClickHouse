#pragma once

#include <mutex>
#include <memory>
#include <functional>

#include <Poco/TemporaryFile.h>

#include <common/logger_useful.h>

#include <common/StringRef.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <common/ThreadPool.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/SizeLimits.h>

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/Compiler.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

class IBlockOutputStream;


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

using AggregatedDataWithUInt8Key = HashMap<UInt64, AggregateDataPtr, TrivialHash, HashTableFixedGrower<8>>;
using AggregatedDataWithUInt16Key = HashMap<UInt64, AggregateDataPtr, TrivialHash, HashTableFixedGrower<16>>;

using AggregatedDataWithUInt64Key = HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
using AggregatedDataWithStringKey = HashMapWithSavedHash<StringRef, AggregateDataPtr>;
using AggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using AggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;
using AggregatedDataHashed = HashMap<UInt128, std::pair<StringRef*, AggregateDataPtr>, UInt128TrivialHash>;

using AggregatedDataWithUInt64KeyTwoLevel = TwoLevelHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
using AggregatedDataWithStringKeyTwoLevel = TwoLevelHashMapWithSavedHash<StringRef, AggregateDataPtr>;
using AggregatedDataWithKeys128TwoLevel = TwoLevelHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using AggregatedDataWithKeys256TwoLevel = TwoLevelHashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;
using AggregatedDataHashedTwoLevel = TwoLevelHashMap<UInt128, std::pair<StringRef*, AggregateDataPtr>, UInt128TrivialHash>;

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


/// For the case where there is one numeric key.
template <typename FieldType, typename TData>    /// UInt8/16/32/64 for any type with corresponding bit width.
struct AggregationMethodOneNumber
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using iterator = typename Data::iterator;
    using const_iterator = typename Data::const_iterator;

    Data data;

    AggregationMethodOneNumber() {}

    template <typename Other>
    AggregationMethodOneNumber(const Other & other) : data(other.data) {}

    /// To use one `Method` in different threads, use different `State`.
    struct State
    {
        const FieldType * vec;

        /** Called at the start of each block processing.
          * Sets the variables needed for the other methods called in inner loops.
          */
        void init(ColumnRawPtrs & key_columns)
        {
            vec = &static_cast<const ColumnVector<FieldType> *>(key_columns[0])->getData()[0];
        }

        /// Get the key from the key columns for insertion into the hash table.
        Key getKey(
            const ColumnRawPtrs & /*key_columns*/,
            size_t /*keys_size*/,         /// Number of key columns.
            size_t i,                     /// From which row of the block, get the key.
            const Sizes & /*key_sizes*/,  /// If the keys of a fixed length - their lengths. It is not used in aggregation methods for variable length keys.
            StringRefs & /*keys*/,        /// Here references to key data in columns can be written. They can be used in the future.
            Arena & /*pool*/) const
        {
            return unionCastToUInt64(vec[i]);
        }
    };

    /// From the value in the hash table, get AggregateDataPtr.
    static AggregateDataPtr & getAggregateData(Mapped & value)                { return value; }
    static const AggregateDataPtr & getAggregateData(const Mapped & value)    { return value; }

    /** Place additional data, if necessary, in case a new key was inserted into the hash table.
      */
    static void onNewKey(typename Data::value_type & /*value*/, size_t /*keys_size*/, StringRefs & /*keys*/, Arena & /*pool*/)
    {
    }

    /** The action to be taken if the key is not new. For example, roll back the memory allocation in the pool.
      */
    static void onExistingKey(const Key & /*key*/, StringRefs & /*keys*/, Arena & /*pool*/) {}

    /** Do not use optimization for consecutive keys.
      */
    static const bool no_consecutive_keys_optimization = false;

    /** Insert the key from the hash table into columns.
      */
    static void insertKeyIntoColumns(const typename Data::value_type & value, MutableColumns & key_columns, size_t /*keys_size*/, const Sizes & /*key_sizes*/)
    {
        static_cast<ColumnVector<FieldType> *>(key_columns[0].get())->insertData(reinterpret_cast<const char *>(&value.first), sizeof(value.first));
    }
};


/// For the case where there is one string key.
template <typename TData>
struct AggregationMethodString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using iterator = typename Data::iterator;
    using const_iterator = typename Data::const_iterator;

    Data data;

    AggregationMethodString() {}

    template <typename Other>
    AggregationMethodString(const Other & other) : data(other.data) {}

    struct State
    {
        const ColumnString::Offsets * offsets;
        const ColumnString::Chars_t * chars;

        void init(ColumnRawPtrs & key_columns)
        {
            const IColumn & column = *key_columns[0];
            const ColumnString & column_string = static_cast<const ColumnString &>(column);
            offsets = &column_string.getOffsets();
            chars = &column_string.getChars();
        }

        Key getKey(
            const ColumnRawPtrs & /*key_columns*/,
            size_t /*keys_size*/,
            size_t i,
            const Sizes & /*key_sizes*/,
            StringRefs & /*keys*/,
            Arena & /*pool*/) const
        {
            return StringRef(
                &(*chars)[i == 0 ? 0 : (*offsets)[i - 1]],
                (i == 0 ? (*offsets)[i] : ((*offsets)[i] - (*offsets)[i - 1])) - 1);
        }
    };

    static AggregateDataPtr & getAggregateData(Mapped & value)                { return value; }
    static const AggregateDataPtr & getAggregateData(const Mapped & value)    { return value; }

    static void onNewKey(typename Data::value_type & value, size_t /*keys_size*/, StringRefs & /*keys*/, Arena & pool)
    {
        value.first.data = pool.insert(value.first.data, value.first.size);
    }

    static void onExistingKey(const Key & /*key*/, StringRefs & /*keys*/, Arena & /*pool*/) {}

    static const bool no_consecutive_keys_optimization = false;

    static void insertKeyIntoColumns(const typename Data::value_type & value, MutableColumns & key_columns, size_t, const Sizes &)
    {
        key_columns[0]->insertData(value.first.data, value.first.size);
    }
};


/// For the case where there is one fixed-length string key.
template <typename TData>
struct AggregationMethodFixedString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using iterator = typename Data::iterator;
    using const_iterator = typename Data::const_iterator;

    Data data;

    AggregationMethodFixedString() {}

    template <typename Other>
    AggregationMethodFixedString(const Other & other) : data(other.data) {}

    struct State
    {
        size_t n;
        const ColumnFixedString::Chars_t * chars;

        void init(ColumnRawPtrs & key_columns)
        {
            const IColumn & column = *key_columns[0];
            const ColumnFixedString & column_string = static_cast<const ColumnFixedString &>(column);
            n = column_string.getN();
            chars = &column_string.getChars();
        }

        Key getKey(
            const ColumnRawPtrs &,
            size_t,
            size_t i,
            const Sizes &,
            StringRefs &,
            Arena &) const
        {
            return StringRef(&(*chars)[i * n], n);
        }
    };

    static AggregateDataPtr & getAggregateData(Mapped & value)                { return value; }
    static const AggregateDataPtr & getAggregateData(const Mapped & value)    { return value; }

    static void onNewKey(typename Data::value_type & value, size_t, StringRefs &, Arena & pool)
    {
        value.first.data = pool.insert(value.first.data, value.first.size);
    }

    static void onExistingKey(const Key &, StringRefs &, Arena &) {}

    static const bool no_consecutive_keys_optimization = false;

    static void insertKeyIntoColumns(const typename Data::value_type & value, MutableColumns & key_columns, size_t, const Sizes &)
    {
        key_columns[0]->insertData(value.first.data, value.first.size);
    }
};

namespace aggregator_impl
{

/// This class is designed to provide the functionality that is required for
/// supporting nullable keys in AggregationMethodKeysFixed. If there are
/// no nullable keys, this class is merely implemented as an empty shell.
template <typename Key, bool has_nullable_keys>
class BaseStateKeysFixed;

/// Case where nullable keys are supported.
template <typename Key>
class BaseStateKeysFixed<Key, true>
{
protected:
    void init(const ColumnRawPtrs & key_columns)
    {
        null_maps.reserve(key_columns.size());
        actual_columns.reserve(key_columns.size());

        for (const auto & col : key_columns)
        {
            if (col->isColumnNullable())
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(*col);
                actual_columns.push_back(&nullable_col.getNestedColumn());
                null_maps.push_back(&nullable_col.getNullMapColumn());
            }
            else
            {
                actual_columns.push_back(col);
                null_maps.push_back(nullptr);
            }
        }
    }

    /// Return the columns which actually contain the values of the keys.
    /// For a given key column, if it is nullable, we return its nested
    /// column. Otherwise we return the key column itself.
    inline const ColumnRawPtrs & getActualColumns() const
    {
        return actual_columns;
    }

    /// Create a bitmap that indicates whether, for a particular row,
    /// a key column bears a null value or not.
    KeysNullMap<Key> createBitmap(size_t row) const
    {
        KeysNullMap<Key> bitmap{};

        for (size_t k = 0; k < null_maps.size(); ++k)
        {
            if (null_maps[k] != nullptr)
            {
                const auto & null_map = static_cast<const ColumnUInt8 &>(*null_maps[k]).getData();
                if (null_map[row] == 1)
                {
                    size_t bucket = k / 8;
                    size_t offset = k % 8;
                    bitmap[bucket] |= UInt8(1) << offset;
                }
            }
        }

        return bitmap;
    }

private:
    ColumnRawPtrs actual_columns;
    ColumnRawPtrs null_maps;
};

/// Case where nullable keys are not supported.
template <typename Key>
class BaseStateKeysFixed<Key, false>
{
protected:
    void init(const ColumnRawPtrs &)
    {
        throw Exception{"Internal error: calling init() for non-nullable"
            " keys is forbidden", ErrorCodes::LOGICAL_ERROR};
    }

    const ColumnRawPtrs & getActualColumns() const
    {
        throw Exception{"Internal error: calling getActualColumns() for non-nullable"
            " keys is forbidden", ErrorCodes::LOGICAL_ERROR};
    }

    KeysNullMap<Key> createBitmap(size_t) const
    {
        throw Exception{"Internal error: calling createBitmap() for non-nullable keys"
            " is forbidden", ErrorCodes::LOGICAL_ERROR};
    }
};

}

/// For the case where all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename TData, bool has_nullable_keys_ = false>
struct AggregationMethodKeysFixed
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using iterator = typename Data::iterator;
    using const_iterator = typename Data::const_iterator;
    static constexpr bool has_nullable_keys = has_nullable_keys_;

    Data data;

    AggregationMethodKeysFixed() {}

    template <typename Other>
    AggregationMethodKeysFixed(const Other & other) : data(other.data) {}

    class State final : private aggregator_impl::BaseStateKeysFixed<Key, has_nullable_keys>
    {
    public:
        using Base = aggregator_impl::BaseStateKeysFixed<Key, has_nullable_keys>;

        void init(ColumnRawPtrs & key_columns)
        {
            if (has_nullable_keys)
                Base::init(key_columns);
        }

        Key getKey(
            const ColumnRawPtrs & key_columns,
            size_t keys_size,
            size_t i,
            const Sizes & key_sizes,
            StringRefs &,
            Arena &) const
        {
            if (has_nullable_keys)
            {
                auto bitmap = Base::createBitmap(i);
                return packFixed<Key>(i, keys_size, Base::getActualColumns(), key_sizes, bitmap);
            }
            else
                return packFixed<Key>(i, keys_size, key_columns, key_sizes);
        }
    };

    static AggregateDataPtr & getAggregateData(Mapped & value)                { return value; }
    static const AggregateDataPtr & getAggregateData(const Mapped & value)    { return value; }

    static void onNewKey(typename Data::value_type &, size_t, StringRefs &, Arena &)
    {
    }

    static void onExistingKey(const Key &, StringRefs &, Arena &) {}

    static const bool no_consecutive_keys_optimization = false;

    static void insertKeyIntoColumns(const typename Data::value_type & value, MutableColumns & key_columns, size_t keys_size, const Sizes & key_sizes)
    {
        static constexpr auto bitmap_size = has_nullable_keys ? std::tuple_size<KeysNullMap<Key>>::value : 0;
        /// In any hash key value, column values to be read start just after the bitmap, if it exists.
        size_t pos = bitmap_size;

        for (size_t i = 0; i < keys_size; ++i)
        {
            IColumn * observed_column;
            ColumnUInt8 * null_map;

            /// If we have a nullable column, get its nested column and its null map.
            if (has_nullable_keys && key_columns[i]->isColumnNullable())
            {
                ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*key_columns[i]);
                observed_column = &nullable_col.getNestedColumn();
                null_map = static_cast<ColumnUInt8 *>(&nullable_col.getNullMapColumn());
            }
            else
            {
                observed_column = key_columns[i].get();
                null_map = nullptr;
            }

            bool is_null;
            if (has_nullable_keys && key_columns[i]->isColumnNullable())
            {
                /// The current column is nullable. Check if the value of the
                /// corresponding key is nullable. Update the null map accordingly.
                size_t bucket = i / 8;
                size_t offset = i % 8;
                UInt8 val = (reinterpret_cast<const UInt8 *>(&value.first)[bucket] >> offset) & 1;
                null_map->insert(val);
                is_null = val == 1;
            }
            else
                is_null = false;

            if (has_nullable_keys && is_null)
                observed_column->insertDefault();
            else
            {
                size_t size = key_sizes[i];
                observed_column->insertData(reinterpret_cast<const char *>(&value.first) + pos, size);
                pos += size;
            }
        }
    }
};


/// Aggregates by key concatenation. (In this case, strings containing zeros in the middle can stick together.)
template <typename TData>
struct AggregationMethodConcat
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using iterator = typename Data::iterator;
    using const_iterator = typename Data::const_iterator;

    Data data;

    AggregationMethodConcat() {}

    template <typename Other>
    AggregationMethodConcat(const Other & other) : data(other.data) {}

    struct State
    {
        void init(ColumnRawPtrs &)
        {
        }

        Key getKey(
            const ColumnRawPtrs & key_columns,
            size_t keys_size,
            size_t i,
            const Sizes &,
            StringRefs & keys,
            Arena & pool) const
        {
            return extractKeysAndPlaceInPoolContiguous(i, keys_size, key_columns, keys, pool);
        }
    };

    static AggregateDataPtr & getAggregateData(Mapped & value)                { return value; }
    static const AggregateDataPtr & getAggregateData(const Mapped & value)    { return value; }

    static void onNewKey(typename Data::value_type &, size_t, StringRefs &, Arena &)
    {
    }

    static void onExistingKey(const Key & key, StringRefs & keys, Arena & pool)
    {
        pool.rollback(key.size + keys.size() * sizeof(keys[0]));
    }

    /// If the key already was, then it is removed from the pool (overwritten), and the next key can not be compared with it.
    static const bool no_consecutive_keys_optimization = true;

    static void insertKeyIntoColumns(const typename Data::value_type & value, MutableColumns & key_columns, size_t keys_size, const Sizes & key_sizes)
    {
        insertKeyIntoColumnsImpl(value, key_columns, keys_size, key_sizes);
    }

private:
    /// Insert the values of the specified keys into the corresponding columns.
    static void insertKeyIntoColumnsImpl(const typename Data::value_type & value, MutableColumns & key_columns, size_t keys_size, const Sizes &)
    {
        /// See function extractKeysAndPlaceInPoolContiguous.
        const StringRef * key_refs = reinterpret_cast<const StringRef *>(value.first.data + value.first.size);

        if (unlikely(0 == value.first.size))
        {
            /** Fix if all keys are empty arrays. For them, a zero-length StringRef is written to the hash table, but with a non-zero pointer.
                * But when inserted into a hash table, this StringRef occurs equal to another key of zero length,
                *  whose data pointer can be any garbage and can not be used.
                */
            for (size_t i = 0; i < keys_size; ++i)
                key_columns[i]->insertDefault();
        }
        else
        {
            for (size_t i = 0; i < keys_size; ++i)
                key_columns[i]->insertDataWithTerminatingZero(key_refs[i].data, key_refs[i].size);
        }
    }
};


/** Aggregates by concatenating serialized key values.
  * Similar to AggregationMethodConcat, but it is suitable, for example, for arrays of strings or multiple arrays.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename TData>
struct AggregationMethodSerialized
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using iterator = typename Data::iterator;
    using const_iterator = typename Data::const_iterator;

    Data data;

    AggregationMethodSerialized() {}

    template <typename Other>
    AggregationMethodSerialized(const Other & other) : data(other.data) {}

    struct State
    {
        void init(ColumnRawPtrs &)
        {
        }

        Key getKey(
            const ColumnRawPtrs & key_columns,
            size_t keys_size,
            size_t i,
            const Sizes &,
            StringRefs &,
            Arena & pool) const
        {
            return serializeKeysToPoolContiguous(i, keys_size, key_columns, pool);
        }
    };

    static AggregateDataPtr & getAggregateData(Mapped & value)                { return value; }
    static const AggregateDataPtr & getAggregateData(const Mapped & value)    { return value; }

    static void onNewKey(typename Data::value_type &, size_t, StringRefs &, Arena &)
    {
    }

    static void onExistingKey(const Key & key, StringRefs &, Arena & pool)
    {
        pool.rollback(key.size);
    }

    /// If the key already was, it is removed from the pool (overwritten), and the next key can not be compared with it.
    static const bool no_consecutive_keys_optimization = true;

    static void insertKeyIntoColumns(const typename Data::value_type & value, MutableColumns & key_columns, size_t keys_size, const Sizes &)
    {
        auto pos = value.first.data;
        for (size_t i = 0; i < keys_size; ++i)
            pos = key_columns[i]->deserializeAndInsertFromArena(pos);
    }
};


/// For other cases. Aggregates by 128-bit hash from the key.
template <typename TData>
struct AggregationMethodHashed
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using iterator = typename Data::iterator;
    using const_iterator = typename Data::const_iterator;

    Data data;

    AggregationMethodHashed() {}

    template <typename Other>
    AggregationMethodHashed(const Other & other) : data(other.data) {}

    struct State
    {
        void init(ColumnRawPtrs &)
        {
        }

        Key getKey(
            const ColumnRawPtrs & key_columns,
            size_t keys_size,
            size_t i,
            const Sizes &,
            StringRefs & keys,
            Arena &) const
        {
            return hash128(i, keys_size, key_columns, keys);
        }
    };

    static AggregateDataPtr & getAggregateData(Mapped & value)                { return value.second; }
    static const AggregateDataPtr & getAggregateData(const Mapped & value)    { return value.second; }

    static void onNewKey(typename Data::value_type & value, size_t keys_size, StringRefs & keys, Arena & pool)
    {
        value.second.first = placeKeysInPool(keys_size, keys, pool);
    }

    static void onExistingKey(const Key &, StringRefs &, Arena &) {}

    static const bool no_consecutive_keys_optimization = false;

    static void insertKeyIntoColumns(const typename Data::value_type & value, MutableColumns & key_columns, size_t keys_size, const Sizes &)
    {
        for (size_t i = 0; i < keys_size; ++i)
            key_columns[i]->insertDataWithTerminatingZero(value.second.first[i].data, value.second.first[i].size);
    }
};


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
    Aggregator * aggregator = nullptr;

    size_t keys_size;    /// Number of keys. NOTE do we need this field?
    Sizes key_sizes;     /// Dimensions of keys, if keys of fixed length

    /// Pools for states of aggregate functions. Ownership will be later transferred to ColumnAggregateFunction.
    Arenas aggregates_pools;
    Arena * aggregates_pool;    /// The pool that is currently used for allocation.

    /** Specialization for the case when there are no keys, and for keys not fitted into max_rows_to_group_by.
      */
    AggregatedDataWithoutKey without_key = nullptr;

    std::unique_ptr<AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key>>           key8;
    std::unique_ptr<AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key>>         key16;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64Key>>         key32;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>>         key64;
    std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKey>>                    key_string;
    std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKey>>               key_fixed_string;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128>>                   keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256>>                   keys256;
    std::unique_ptr<AggregationMethodHashed<AggregatedDataHashed>>                           hashed;
    std::unique_ptr<AggregationMethodConcat<AggregatedDataWithStringKey>>                    concat;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKey>>                serialized;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyTwoLevel>> key32_two_level;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyTwoLevel>> key64_two_level;
    std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKeyTwoLevel>>            key_string_two_level;
    std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKeyTwoLevel>>       key_fixed_string_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel>>           keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel>>           keys256_two_level;
    std::unique_ptr<AggregationMethodHashed<AggregatedDataHashedTwoLevel>>                   hashed_two_level;
    std::unique_ptr<AggregationMethodConcat<AggregatedDataWithStringKeyTwoLevel>>            concat_two_level;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel>>        serialized_two_level;

    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyHash64>>   key64_hash64;
    std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKeyHash64>>              key_string_hash64;
    std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKeyHash64>>         key_fixed_string_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128Hash64>>             keys128_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256Hash64>>             keys256_hash64;
    std::unique_ptr<AggregationMethodConcat<AggregatedDataWithStringKeyHash64>>              concat_hash64;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyHash64>>          serialized_hash64;

    /// Support for nullable keys.
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128, true>>             nullable_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256, true>>             nullable_keys256;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel, true>>     nullable_keys128_two_level;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel, true>>     nullable_keys256_two_level;

    /// In this and similar macros, the option without_key is not considered.
    #define APPLY_FOR_AGGREGATED_VARIANTS(M) \
        M(key8,                       false) \
        M(key16,                      false) \
        M(key32,                      false) \
        M(key64,                      false) \
        M(key_string,                 false) \
        M(key_fixed_string,           false) \
        M(keys128,                    false) \
        M(keys256,                    false) \
        M(hashed,                     false) \
        M(concat,                     false) \
        M(serialized,                 false) \
        M(key32_two_level,            true) \
        M(key64_two_level,            true) \
        M(key_string_two_level,       true) \
        M(key_fixed_string_two_level, true) \
        M(keys128_two_level,          true) \
        M(keys256_two_level,          true) \
        M(hashed_two_level,           true) \
        M(concat_two_level,           true) \
        M(serialized_two_level,       true) \
        M(key64_hash64,               false) \
        M(key_string_hash64,          false) \
        M(key_fixed_string_hash64,    false) \
        M(keys128_hash64,             false) \
        M(keys256_hash64,             false) \
        M(concat_hash64,              false) \
        M(serialized_hash64,          false) \
        M(nullable_keys128,           false) \
        M(nullable_keys256,           false) \
        M(nullable_keys128_two_level, true) \
        M(nullable_keys256_two_level, true) \

    enum class Type
    {
        EMPTY = 0,
        without_key,

    #define M(NAME, IS_TWO_LEVEL) NAME,
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    };
    Type type = Type::EMPTY;

    AggregatedDataVariants() : aggregates_pools(1, std::make_shared<Arena>()), aggregates_pool(aggregates_pools.back().get()) {}
    bool empty() const { return type == Type::EMPTY; }
    void invalidate() { type = Type::EMPTY; }

    ~AggregatedDataVariants();

    void init(Type type_)
    {
        switch (type_)
        {
            case Type::EMPTY:       break;
            case Type::without_key: break;

        #define M(NAME, IS_TWO_LEVEL) \
            case Type::NAME: NAME = std::make_unique<decltype(NAME)::element_type>(); break;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M

            default:
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }

        type = type_;
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
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M

            default:
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
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
            APPLY_FOR_AGGREGATED_VARIANTS(M)
            #undef M

            default:
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
    }

    const char * getMethodName() const
    {
        switch (type)
        {
            case Type::EMPTY:       return "EMPTY";
            case Type::without_key: return "without_key";

        #define M(NAME, IS_TWO_LEVEL) \
            case Type::NAME: return #NAME;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M

            default:
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
    }

    bool isTwoLevel() const
    {
        switch (type)
        {
            case Type::EMPTY:       return false;
            case Type::without_key: return false;

        #define M(NAME, IS_TWO_LEVEL) \
            case Type::NAME: return IS_TWO_LEVEL;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M

            default:
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
    }

    #define APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M) \
        M(key32)            \
        M(key64)            \
        M(key_string)       \
        M(key_fixed_string) \
        M(keys128)          \
        M(keys256)          \
        M(hashed)           \
        M(concat)           \
        M(serialized)       \
        M(nullable_keys128) \
        M(nullable_keys256) \

    #define APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M) \
        M(key8)             \
        M(key16)            \
        M(key64_hash64)     \
        M(key_string_hash64)\
        M(key_fixed_string_hash64) \
        M(keys128_hash64)   \
        M(keys256_hash64)   \
        M(concat_hash64)    \
        M(serialized_hash64) \

    #define APPLY_FOR_VARIANTS_SINGLE_LEVEL(M) \
        APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M) \
        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M) \

    bool isConvertibleToTwoLevel() const
    {
        switch (type)
        {
        #define M(NAME) \
            case Type::NAME: return true;

            APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

        #undef M
            default:
                return false;
        }
    }

    void convertToTwoLevel();

    #define APPLY_FOR_VARIANTS_TWO_LEVEL(M) \
        M(key32_two_level)            \
        M(key64_two_level)            \
        M(key_string_two_level)       \
        M(key_fixed_string_two_level) \
        M(keys128_two_level)          \
        M(keys256_two_level)          \
        M(hashed_two_level)           \
        M(concat_two_level)           \
        M(serialized_two_level)       \
        M(nullable_keys128_two_level) \
        M(nullable_keys256_two_level)
};

using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;
using ManyAggregatedDataVariants = std::vector<AggregatedDataVariantsPtr>;

/** How are "total" values calculated with WITH TOTALS?
  * (For more details, see TotalsHavingBlockInputStream.)
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
class Aggregator
{
public:
    struct Params
    {
        /// Data structure of source blocks.
        Block src_header;
        /// Data structure of intermediate blocks before merge.
        Block intermediate_header;

        /// What to count.
        ColumnNumbers keys;
        AggregateDescriptions aggregates;
        size_t keys_size;
        size_t aggregates_size;

        /// The settings of approximate calculation of GROUP BY.
        const bool overflow_row;    /// Do we need to put into AggregatedDataVariants::without_key aggregates for keys that are not in max_rows_to_group_by.
        const size_t max_rows_to_group_by;
        const OverflowMode group_by_overflow_mode;

        /// For dynamic compilation.
        Compiler * compiler;
        const UInt32 min_count_to_compile;

        /// Two-level aggregation settings (used for a large number of keys).
        /** With how many keys or the size of the aggregation state in bytes,
          *  two-level aggregation begins to be used. Enough to reach of at least one of the thresholds.
          * 0 - the corresponding threshold is not specified.
          */
        const size_t group_by_two_level_threshold;
        const size_t group_by_two_level_threshold_bytes;

        /// Settings to flush temporary data to the filesystem (external aggregation).
        const size_t max_bytes_before_external_group_by;        /// 0 - do not use external aggregation.

        /// Return empty result when aggregating without keys on empty set.
        bool empty_result_for_aggregation_by_empty_set;

        const std::string tmp_path;

        Params(
            const Block & src_header_,
            const ColumnNumbers & keys_, const AggregateDescriptions & aggregates_,
            bool overflow_row_, size_t max_rows_to_group_by_, OverflowMode group_by_overflow_mode_,
            Compiler * compiler_, UInt32 min_count_to_compile_,
            size_t group_by_two_level_threshold_, size_t group_by_two_level_threshold_bytes_,
            size_t max_bytes_before_external_group_by_,
            bool empty_result_for_aggregation_by_empty_set_,
            const std::string & tmp_path_)
            : src_header(src_header_),
            keys(keys_), aggregates(aggregates_), keys_size(keys.size()), aggregates_size(aggregates.size()),
            overflow_row(overflow_row_), max_rows_to_group_by(max_rows_to_group_by_), group_by_overflow_mode(group_by_overflow_mode_),
            compiler(compiler_), min_count_to_compile(min_count_to_compile_),
            group_by_two_level_threshold(group_by_two_level_threshold_), group_by_two_level_threshold_bytes(group_by_two_level_threshold_bytes_),
            max_bytes_before_external_group_by(max_bytes_before_external_group_by_),
            empty_result_for_aggregation_by_empty_set(empty_result_for_aggregation_by_empty_set_),
            tmp_path(tmp_path_)
        {
        }

        /// Only parameters that matter during merge.
        Params(const Block & intermediate_header_,
            const ColumnNumbers & keys_, const AggregateDescriptions & aggregates_, bool overflow_row_)
            : Params(Block(), keys_, aggregates_, overflow_row_, 0, OverflowMode::THROW, nullptr, 0, 0, 0, 0, false, "")
        {
            intermediate_header = intermediate_header_;
        }

        /// Calculate the column numbers in `keys` and `aggregates`.
        void calculateColumnNumbers(const Block & block);
    };

    Aggregator(const Params & params_);

    /// Aggregate the source. Get the result in the form of one of the data structures.
    void execute(const BlockInputStreamPtr & stream, AggregatedDataVariants & result);

    using AggregateColumns = std::vector<ColumnRawPtrs>;
    using AggregateColumnsData = std::vector<ColumnAggregateFunction::Container *>;
    using AggregateColumnsConstData = std::vector<const ColumnAggregateFunction::Container *>;
    using AggregateFunctionsPlainPtrs = std::vector<IAggregateFunction *>;

    /// Process one block. Return false if the processing should be aborted (with group_by_overflow_mode = 'break').
    bool executeOnBlock(const Block & block, AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns, AggregateColumns & aggregate_columns,    /// Passed to not create them anew for each block
        StringRefs & keys,                                        /// - pass the corresponding objects that are initially empty.
        bool & no_more_keys);

    /** Convert the aggregation data structure into a block.
      * If overflow_row = true, then aggregates for rows that are not included in max_rows_to_group_by are put in the first block.
      *
      * If final = false, then ColumnAggregateFunction is created as the aggregation columns with the state of the calculations,
      *  which can then be combined with other states (for distributed query processing).
      * If final = true, then columns with ready values are created as aggregate columns.
      */
    BlocksList convertToBlocks(AggregatedDataVariants & data_variants, bool final, size_t max_threads) const;

    /** Merge several aggregation data structures and output the result as a block stream.
      */
    std::unique_ptr<IBlockInputStream> mergeAndConvertToBlocks(ManyAggregatedDataVariants & data_variants, bool final, size_t max_threads) const;

    /** Merge the stream of partially aggregated blocks into one data structure.
      * (Pre-aggregate several blocks that represent the result of independent aggregations from remote servers.)
      */
    void mergeStream(const BlockInputStreamPtr & stream, AggregatedDataVariants & result, size_t max_threads);

    /// Merge several partially aggregated blocks into one.
    /// Precondition: for all blocks block.info.is_overflows flag must be the same.
    /// (either all blocks are from overflow data or none blocks are).
    /// The resulting block has the same value of is_overflows flag.
    Block mergeBlocks(BlocksList & blocks, bool final);

    /** Split block with partially-aggregated data to many blocks, as if two-level method of aggregation was used.
      * This is needed to simplify merging of that data with other results, that are already two-level.
      */
    std::vector<Block> convertBlockToTwoLevel(const Block & block);

    using CancellationHook = std::function<bool()>;

    /** Set a function that checks whether the current task can be aborted.
      */
    void setCancellationHook(const CancellationHook cancellation_hook);

    /// For external aggregation.
    void writeToTemporaryFile(AggregatedDataVariants & data_variants);

    bool hasTemporaryFiles() const { return !temporary_files.empty(); }

    struct TemporaryFiles
    {
        std::vector<std::unique_ptr<Poco::TemporaryFile>> files;
        size_t sum_size_uncompressed = 0;
        size_t sum_size_compressed = 0;
        mutable std::mutex mutex;

        bool empty() const
        {
            std::lock_guard<std::mutex> lock(mutex);
            return files.empty();
        }
    };

    const TemporaryFiles & getTemporaryFiles() const { return temporary_files; }

    /// Get data structure of the result.
    Block getHeader(bool final) const;

protected:
    friend struct AggregatedDataVariants;
    friend class MergingAndConvertingBlockInputStream;

    Params params;

    AggregatedDataVariants::Type method;
    Sizes key_sizes;

    AggregateFunctionsPlainPtrs aggregate_functions;

    /** This array serves two purposes.
      *
      * 1. Function arguments are collected side by side, and they do not need to be collected from different places. Also the array is made zero-terminated.
      * The inner loop (for the case without_key) is almost twice as compact; performance gain of about 30%.
      *
      * 2. Calling a function by pointer is better than a virtual call, because in the case of a virtual call,
      *  GCC 5.1.2 generates code that, at each iteration of the loop, reloads the function address from memory into the register
      *  (the offset value in the virtual function table).
      */
    struct AggregateFunctionInstruction
    {
        const IAggregateFunction * that;
        IAggregateFunction::AddFunc func;
        size_t state_offset;
        const IColumn ** arguments;
    };

    using AggregateFunctionInstructions = std::vector<AggregateFunctionInstruction>;

    Sizes offsets_of_aggregate_states;    /// The offset to the n-th aggregate function in a row of aggregate functions.
    size_t total_size_of_aggregate_states = 0;    /// The total size of the row from the aggregate functions.

    // add info to track alignment requirement
    // If there are states whose alignmentment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;

    bool all_aggregates_has_trivial_destructor = false;

    /// How many RAM were used to process the query before processing the first block.
    Int64 memory_usage_before_aggregation = 0;

    std::mutex mutex;

    Logger * log = &Logger::get("Aggregator");

    /** Dynamically compiled library for aggregation, if any.
      * The meaning of dynamic compilation is to specialize code
      *  for a specific list of aggregate functions.
      * This allows you to expand the loop to create and update states of aggregate functions,
      *  and also use inline-code instead of virtual calls.
      */
    struct CompiledData
    {
        SharedLibraryPtr compiled_aggregator;

        /// Obtained with dlsym. It is still necessary to make reinterpret_cast to the function pointer.
        void * compiled_method_ptr = nullptr;
        void * compiled_two_level_method_ptr = nullptr;
    };
    /// shared_ptr - to pass into a callback, that can survive Aggregator.
    std::shared_ptr<CompiledData> compiled_data { new CompiledData };

    bool compiled_if_possible = false;
    void compileIfPossible(AggregatedDataVariants::Type type);

    /// Returns true if you can abort the current task.
    CancellationHook isCancelled;

    /// For external aggregation.
    TemporaryFiles temporary_files;

    /** Select the aggregation method based on the number and types of keys. */
    AggregatedDataVariants::Type chooseAggregationMethod();

    /** Create states of aggregate functions for one key.
      */
    void createAggregateStates(AggregateDataPtr & aggregate_data) const;

    /** Call `destroy` methods for states of aggregate functions.
      * Used in the exception handler for aggregation, since RAII in this case is not applicable.
      */
    void destroyAllAggregateStates(AggregatedDataVariants & result);


    /// Process one data block, aggregate the data into a hash table.
    template <typename Method>
    void executeImpl(
        Method & method,
        Arena * aggregates_pool,
        size_t rows,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        const Sizes & key_sizes,
        StringRefs & keys,
        bool no_more_keys,
        AggregateDataPtr overflow_row) const;

    /// Specialization for a particular value no_more_keys.
    template <bool no_more_keys, typename Method>
    void executeImplCase(
        Method & method,
        typename Method::State & state,
        Arena * aggregates_pool,
        size_t rows,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        const Sizes & key_sizes,
        StringRefs & keys,
        AggregateDataPtr overflow_row) const;

    /// For case when there are no keys (all aggregate into one row).
    void executeWithoutKeyImpl(
        AggregatedDataWithoutKey & res,
        size_t rows,
        AggregateFunctionInstruction * aggregate_instructions,
        Arena * arena) const;

    template <typename Method>
    void writeToTemporaryFileImpl(
        AggregatedDataVariants & data_variants,
        Method & method,
        IBlockOutputStream & out);

public:
    /// Templates that are instantiated by dynamic code compilation - see SpecializedAggregator.h

    template <typename Method, typename AggregateFunctionsList>
    void executeSpecialized(
        Method & method,
        Arena * aggregates_pool,
        size_t rows,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns,
        const Sizes & key_sizes,
        StringRefs & keys,
        bool no_more_keys,
        AggregateDataPtr overflow_row) const;

    template <bool no_more_keys, typename Method, typename AggregateFunctionsList>
    void executeSpecializedCase(
        Method & method,
        typename Method::State & state,
        Arena * aggregates_pool,
        size_t rows,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns,
        const Sizes & key_sizes,
        StringRefs & keys,
        AggregateDataPtr overflow_row) const;

    template <typename AggregateFunctionsList>
    void executeSpecializedWithoutKey(
        AggregatedDataWithoutKey & res,
        size_t rows,
        AggregateColumns & aggregate_columns,
        Arena * arena) const;

protected:
    /// Merge data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena) const;

    /// Merge data from hash table `src` into `dst`, but only for keys that already exist in dst. In other cases, merge the data into `overflows`.
    template <typename Method, typename Table>
    void mergeDataNoMoreKeysImpl(
        Table & table_dst,
        AggregatedDataWithoutKey & overflows,
        Table & table_src,
        Arena * arena) const;

    /// Same, but ignores the rest of the keys.
    template <typename Method, typename Table>
    void mergeDataOnlyExistingKeysImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena) const;

    void mergeWithoutKeyDataImpl(
        ManyAggregatedDataVariants & non_empty_data) const;

    template <typename Method>
    void mergeSingleLevelDataImpl(
        ManyAggregatedDataVariants & non_empty_data) const;

    template <typename Method, typename Table>
    void convertToBlockImpl(
        Method & method,
        Table & data,
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        const Sizes & key_sizes,
        bool final) const;

    template <typename Method, typename Table>
    void convertToBlockImplFinal(
        Method & method,
        Table & data,
        MutableColumns & key_columns,
        MutableColumns & final_aggregate_columns,
        const Sizes & key_sizes) const;

    template <typename Method, typename Table>
    void convertToBlockImplNotFinal(
        Method & method,
        Table & data,
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        const Sizes & key_sizes) const;

    template <typename Filler>
    Block prepareBlockAndFill(
        AggregatedDataVariants & data_variants,
        bool final,
        size_t rows,
        Filler && filler) const;

    template <typename Method>
    Block convertOneBucketToBlock(
        AggregatedDataVariants & data_variants,
        Method & method,
        bool final,
        size_t bucket) const;

    Block prepareBlockAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const;
    Block prepareBlockAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const;
    BlocksList prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, ThreadPool * thread_pool) const;

    template <typename Method>
    BlocksList prepareBlocksAndFillTwoLevelImpl(
        AggregatedDataVariants & data_variants,
        Method & method,
        bool final,
        ThreadPool * thread_pool) const;

    template <bool no_more_keys, typename Method, typename Table>
    void mergeStreamsImplCase(
        Block & block,
        const Sizes & key_sizes,
        Arena * aggregates_pool,
        Method & method,
        Table & data,
        AggregateDataPtr overflow_row) const;

    template <typename Method, typename Table>
    void mergeStreamsImpl(
        Block & block,
        const Sizes & key_sizes,
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
        ManyAggregatedDataVariants & data, Int32 bucket, Arena * arena) const;

    template <typename Method>
    void convertBlockToTwoLevelImpl(
        Method & method,
        Arena * pool,
        ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        StringRefs & keys,
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
};


/** Get the aggregation variant by its type. */
template <typename Method> Method & getDataVariant(AggregatedDataVariants & variants);

#define M(NAME, IS_TWO_LEVEL) \
    template <> inline decltype(AggregatedDataVariants::NAME)::element_type & getDataVariant<decltype(AggregatedDataVariants::NAME)::element_type>(AggregatedDataVariants & variants) { return *variants.NAME; }

APPLY_FOR_AGGREGATED_VARIANTS(M)

#undef M


}
