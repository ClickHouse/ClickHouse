#pragma once

#include <optional>
#include <utility>

#include <Common/ColumnsHashing.h>
#include <DataTypes/IDataType.h>
#include <Common/assert_cast.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/ClearableHashSet.h>
#include <Common/HashTable/FixedClearableHashSet.h>
#include <Common/HashTable/FixedHashSet.h>
#include <Common/HashTable/FixedHashMap.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** Methods for different implementations of sets (used in right hand side of IN or for DISTINCT).
  * To use as template parameter.
  */

/// The method structs below are shared between presence-only sets (a `HashSet`, whose cell reports
/// `VoidMapped`) and counting multisets (a `HashMap`, with a real mapped type, used by INTERSECT
/// ALL / EXCEPT ALL). The mapped type is derived from the table rather than passed explicitly, so a
/// method stays "a set" when given a `HashSet` and becomes a counting multiset when given a `HashMap`.
template <typename TData>
using SetMethodMapped = std::conditional_t<std::is_same_v<typename TData::mapped_type, VoidMapped>, void, typename TData::mapped_type>;

/// The consecutive-keys cache returns the mapped value by copy, which would break in-place count
/// updates, so it is only used for plain (non-counting) sets.
template <typename TData, bool use_cache>
inline constexpr bool set_method_use_cache = use_cache && std::is_void_v<SetMethodMapped<TData>>;


/// For the case where there is one numeric key.
template <typename FieldType, typename TData, bool use_cache = true>    /// UInt8/16/32/64 for any types with corresponding bit width.
struct SetMethodOneNumber
{
    using Data = TData;
    using Key = typename Data::key_type;

    Data data;

    using State = ColumnsHashing::HashMethodOneNumber<typename Data::value_type,
        SetMethodMapped<Data>, FieldType, set_method_use_cache<Data, use_cache>>;

    /// Appends the numeric key to its destination column.
    static void insertKeyIntoColumns(const Key & key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        key_columns[0]->insertData(reinterpret_cast<const char *>(&key), sizeof(key));
    }
};

/// For the case where there is one string key.
/// Unlike aggregation, the consecutive-keys cache stays disabled for string keys: a set probe's
/// per-row work is just hash+lookup, which for strings is cheaper than the extra bytewise
/// comparison the cache check performs on every miss. Measured 2026-08 on 20M-row `IN` probes:
/// enabling the cache made unclustered inputs 40% slower for short `String` keys and 24% slower
/// for `FixedString(36)` keys, while clustered inputs (runs of equal consecutive keys) improved
/// only 8-16%. In aggregation the same check is relatively cheap because the per-row work there
/// (aggregate-state updates) is much heavier.
template <typename TData>
struct SetMethodString
{
    using Data = TData;
    using Key = typename Data::key_type;

    Data data;

    using State = ColumnsHashing::HashMethodString<typename Data::value_type, SetMethodMapped<Data>, true, false>;

    static void insertKeyIntoColumns(std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        key_columns[0]->insertData(key.data(), key.size());
    }
};

/// For the case when there is one fixed-length string key.
/// The consecutive-keys cache is disabled for the same reason as in `SetMethodString`.
template <typename TData>
struct SetMethodFixedString
{
    using Data = TData;
    using Key = typename Data::key_type;

    Data data;

    using State = ColumnsHashing::HashMethodFixedString<typename Data::value_type, SetMethodMapped<Data>, true, false>;

    static void insertKeyIntoColumns(std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        key_columns[0]->insertData(key.data(), key.size());
    }
};

namespace set_impl
{

/// This class is designed to provide the functionality that is required for
/// supporting nullable keys in SetMethodKeysFixed. If there are
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
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*col))
            {
                actual_columns.push_back(&nullable->getNestedColumn());
                null_maps.push_back(&nullable->getNullMapColumn());
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
    [[nodiscard]] const ColumnRawPtrs & getActualColumns() const
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
                const auto & null_map = assert_cast<const ColumnUInt8 &>(*null_maps[k]).getData();
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Internal error: calling init() for non-nullable keys is forbidden");
    }

    [[nodiscard]] const ColumnRawPtrs & getActualColumns() const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Internal error: calling getActualColumns() for non-nullable keys is forbidden");
    }

    KeysNullMap<Key> createBitmap(size_t) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Internal error: calling createBitmap() for non-nullable keys is forbidden");
    }
};

}

/// For the case when all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename TData, bool has_nullable_keys_ = false>
struct SetMethodKeysFixed
{
    using Data = TData;
    using Key = typename Data::key_type;
    static constexpr bool has_nullable_keys = has_nullable_keys_;

    Data data;

    using State = ColumnsHashing::HashMethodKeysFixed<typename Data::value_type, Key, SetMethodMapped<Data>,
        has_nullable_keys, false, set_method_use_cache<Data, true>>;

    /// `unpack_order` is the order in which the columns were packed into the key, if it differs from the
    /// original one (see `HashMethodKeysFixed::packedKeysOrder`).
    static void insertKeyIntoColumns(
        const Key & key, std::vector<IColumn *> & key_columns, const Sizes & key_sizes, const std::vector<size_t> * unpack_order)
    {
        unpackFixedKeyIntoColumns<has_nullable_keys>(key, unpack_order, key_columns, key_sizes);
    }
};

/// For other cases. 128 bit hash from the key.
template <typename TData>
struct SetMethodHashed
{
    using Data = TData;
    using Key = typename Data::key_type;

    Data data;

    using State = ColumnsHashing::HashMethodHashed<typename Data::value_type, SetMethodMapped<Data>, set_method_use_cache<Data, true>>;

    /// Appends the retained fingerprint to a `UInt128` comparison column.
    static void insertKeyIntoColumns(const Key & key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        key_columns[0]->insertData(reinterpret_cast<const char *>(&key), sizeof(key));
    }
};

/// Whether the keys of `Method` are the values of fixed-size key columns, packed into an integer.
template <typename Method>
inline constexpr bool set_method_has_fixed_size_keys = false;

template <typename FieldType, typename TData, bool use_cache>
inline constexpr bool set_method_has_fixed_size_keys<SetMethodOneNumber<FieldType, TData, use_cache>> = true;

template <typename TData, bool has_nullable_keys>
inline constexpr bool set_method_has_fixed_size_keys<SetMethodKeysFixed<TData, has_nullable_keys>> = true;

/// Whether the keys of `Method` are the values of one `String` key column or of one `FixedString` key column
/// too long to pack, which its table keeps as they are.
template <typename Method>
inline constexpr bool set_method_has_string_keys = false;

template <typename TData>
inline constexpr bool set_method_has_string_keys<SetMethodString<TData>> = true;

template <typename TData>
inline constexpr bool set_method_has_string_keys<SetMethodFixedString<TData>> = true;

/// Reads the keys of the table of `Method` into new columns of `key_types`, in batches, with
/// `Method::insertKeyIntoColumns`: the key columns of the set, or one `UInt128` column with the hashes that
/// the `hashed` method keeps. Packed keys can hold their columns in another order than the original one,
/// which is computed once for all keys (see `HashMethodKeysFixed::packedKeysOrder`). The table must stay
/// alive and unchanged while the reader reads it.
template <typename Method>
class SetMethodKeyReader
{
public:
    SetMethodKeyReader(const Method & method, DataTypes key_types_, Sizes key_sizes_)
        : key_types(std::move(key_types_))
        , key_sizes(std::move(key_sizes_))
        , position(method.data.begin())
        , end(method.data.end())
    {
        if constexpr (requires { Method::State::packedKeysOrder(key_sizes); })
            unpack_order = Method::State::packedKeysOrder(key_sizes);
    }

    [[nodiscard]] bool atEnd() const { return position == end; }

    /// Returns at most `max_rows` keys, stopping after a complete key reaches `max_bytes` of allocated column
    /// memory. The byte target is soft because a key or an allocation can exceed it; zero disables it.
    /// `max_rows` must be positive.
    MutableColumns read(size_t max_rows, size_t max_bytes)
    {
        chassert(max_rows > 0);

        MutableColumns columns;
        std::vector<IColumn *> raw_columns;
        columns.reserve(key_types.size());
        raw_columns.reserve(key_types.size());
        for (const auto & key_type : key_types)
        {
            columns.push_back(key_type->createColumn());
            raw_columns.push_back(columns.back().get());
        }

        size_t rows = 0;
        while (position != end && rows < max_rows)
        {
            if constexpr (requires { Method::State::packedKeysOrder(key_sizes); })
                Method::insertKeyIntoColumns(
                    position->getValue(), raw_columns, key_sizes, unpack_order ? &*unpack_order : nullptr);
            else
                Method::insertKeyIntoColumns(position->getValue(), raw_columns, key_sizes);

            ++position;
            ++rows;

            if (max_bytes)
            {
                size_t bytes = 0;
                for (const auto & column : columns)
                    bytes += column->allocatedBytes();
                if (bytes >= max_bytes)
                    break;
            }
        }

        return columns;
    }

private:
    const DataTypes key_types;
    const Sizes key_sizes;
    std::optional<Sizes> unpack_order;
    typename Method::Data::const_iterator position;
    const typename Method::Data::const_iterator end;
};

/** Different implementations of the set.
  */
struct NonClearableSet
{
    /*
     * As in Aggregator, using consecutive keys cache doesn't improve performance
     * for FixedHashTables.
     */
    std::unique_ptr<SetMethodOneNumber<UInt8, FixedHashSet<UInt8>, false /* use_cache */>>   key8;
    std::unique_ptr<SetMethodOneNumber<UInt16, FixedHashSet<UInt16>, false /* use_cache */>> key16;

    /** Also for the experiment was tested the ability to use SmallSet,
      *  as long as the number of elements in the set is small (and, if necessary, converted to a full-fledged HashSet).
      * But this experiment showed that there is an advantage only in rare cases.
      */
    std::unique_ptr<SetMethodOneNumber<UInt32, HashSet<UInt32, HashCRC32<UInt32>>>>          key32;
    std::unique_ptr<SetMethodOneNumber<UInt64, HashSet<UInt64, HashCRC32<UInt64>>>>          key64;
    std::unique_ptr<SetMethodString<HashSetWithSavedHash<std::string_view>>>                        key_string;
    std::unique_ptr<SetMethodFixedString<HashSetWithSavedHash<std::string_view>>>                   key_fixed_string;
    std::unique_ptr<SetMethodKeysFixed<HashSet<UInt32, HashCRC32<UInt32>>>>                  keys32;
    std::unique_ptr<SetMethodKeysFixed<HashSet<UInt64, HashCRC32<UInt64>>>>                  keys64;
    std::unique_ptr<SetMethodKeysFixed<HashSet<UInt128, UInt128HashCRC32>>>                  keys128;
    std::unique_ptr<SetMethodKeysFixed<HashSet<UInt256, UInt256HashCRC32>>>                  keys256;
    std::unique_ptr<SetMethodHashed<HashSet<UInt128, UInt128TrivialHash>>>                   hashed;

    /// Support for nullable keys (for DISTINCT implementation).
    std::unique_ptr<SetMethodKeysFixed<HashSet<UInt128, UInt128HashCRC32>, true>>            nullable_keys128;
    std::unique_ptr<SetMethodKeysFixed<HashSet<UInt256, UInt256HashCRC32>, true>>            nullable_keys256;
    /** Unlike Aggregator, `concat` method is not used here.
      * This is done because `hashed` method, although slower, but in this case, uses less RAM.
      *  since when you use it, the key values themselves are not stored.
      */
};

struct ClearableSet
{
    std::unique_ptr<SetMethodOneNumber<UInt8, FixedClearableHashSet<UInt8>, false /* use_cache */>>  key8;
    std::unique_ptr<SetMethodOneNumber<UInt16, FixedClearableHashSet<UInt16>, false /*use_cache */>> key16;

    std::unique_ptr<SetMethodOneNumber<UInt32, ClearableHashSet<UInt32, HashCRC32<UInt32>>>>         key32;
    std::unique_ptr<SetMethodOneNumber<UInt64, ClearableHashSet<UInt64, HashCRC32<UInt64>>>>         key64;
    std::unique_ptr<SetMethodString<ClearableHashSetWithSavedHash<std::string_view>>>                       key_string;
    std::unique_ptr<SetMethodFixedString<ClearableHashSetWithSavedHash<std::string_view>>>                  key_fixed_string;
    std::unique_ptr<SetMethodKeysFixed<ClearableHashSet<UInt32, HashCRC32<UInt32>>>>                 keys32;
    std::unique_ptr<SetMethodKeysFixed<ClearableHashSet<UInt64, HashCRC32<UInt64>>>>                 keys64;
    std::unique_ptr<SetMethodKeysFixed<ClearableHashSet<UInt128, UInt128HashCRC32>>>                 keys128;
    std::unique_ptr<SetMethodKeysFixed<ClearableHashSet<UInt256, UInt256HashCRC32>>>                 keys256;
    std::unique_ptr<SetMethodHashed<ClearableHashSet<UInt128, UInt128TrivialHash>>>                  hashed;

    /// Support for nullable keys (for DISTINCT implementation).
    std::unique_ptr<SetMethodKeysFixed<ClearableHashSet<UInt128, UInt128HashCRC32>, true>>           nullable_keys128;
    std::unique_ptr<SetMethodKeysFixed<ClearableHashSet<UInt256, UInt256HashCRC32>, true>>           nullable_keys256;
    /** Unlike Aggregator, `concat` method is not used here.
      * This is done because `hashed` method, although slower, but in this case, uses less RAM.
      *  since when you use it, the key values themselves are not stored.
      */
};

/// Like NonClearableSet, but each distinct key carries a UInt64 occurrence count (a multiset),
/// used by INTERSECT ALL / EXCEPT ALL. It mirrors NonClearableSet with HashMap-s in place of the
/// HashSet-s; the count mapped type and the disabled consecutive-keys cache are derived from the
/// table (see SetMethodMapped / set_method_use_cache).
struct CountingSet
{
    using Count = UInt64;

    std::unique_ptr<SetMethodOneNumber<UInt8,  FixedHashMap<UInt8,  Count>, false /* use_cache */>>  key8;
    std::unique_ptr<SetMethodOneNumber<UInt16, FixedHashMap<UInt16, Count>, false /* use_cache */>>  key16;

    std::unique_ptr<SetMethodOneNumber<UInt32, HashMap<UInt32, Count, HashCRC32<UInt32>>>>           key32;
    std::unique_ptr<SetMethodOneNumber<UInt64, HashMap<UInt64, Count, HashCRC32<UInt64>>>>           key64;
    std::unique_ptr<SetMethodString<HashMapWithSavedHash<std::string_view, Count>>>                  key_string;
    std::unique_ptr<SetMethodFixedString<HashMapWithSavedHash<std::string_view, Count>>>             key_fixed_string;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt32, Count, HashCRC32<UInt32>>>>                   keys32;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt64, Count, HashCRC32<UInt64>>>>                   keys64;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt128, Count, UInt128HashCRC32>>>                   keys128;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt256, Count, UInt256HashCRC32>>>                   keys256;
    std::unique_ptr<SetMethodHashed<HashMap<UInt128, Count, UInt128TrivialHash>>>                    hashed;

    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt128, Count, UInt128HashCRC32>, true>>             nullable_keys128;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt256, Count, UInt256HashCRC32>, true>>             nullable_keys256;
};

template <typename Variant>
struct SetVariantsTemplate: public Variant
{
    Arena string_pool;

    #define APPLY_FOR_SET_VARIANTS(M) \
        M(key8)                 \
        M(key16)                \
        M(key32)                \
        M(key64)                \
        M(key_string)           \
        M(key_fixed_string)     \
        M(keys32)               \
        M(keys64)               \
        M(keys128)              \
        M(keys256)              \
        M(nullable_keys128)     \
        M(nullable_keys256)     \
        M(hashed)

    #define M(NAME) using Variant::NAME;
        APPLY_FOR_SET_VARIANTS(M)
    #undef M

    enum class Type : uint8_t
    {
        EMPTY,

    #define M(NAME) NAME,
        APPLY_FOR_SET_VARIANTS(M)
    #undef M
    };

    Type type = Type::EMPTY;

    [[nodiscard]] bool empty() const { return type == Type::EMPTY; }

    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);

    void init(Type type_);

    /// Calls `func` with the method of the set, which must be initialized, and returns what `func` returns.
    /// The method is const for a const set.
    template <typename Self, typename Func>
    decltype(auto) callOnMethod(this Self & self, Func && func)
    {
        switch (self.type)
        {
            case Type::EMPTY:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The method of an uninitialized set is called");

        #define M(NAME) case Type::NAME: return std::forward<Func>(func)(std::forward_like<Self &>(*self.NAME));
            APPLY_FOR_SET_VARIANTS(M)
        #undef M
        }
        UNREACHABLE();
    }

    /// Estimates peak additional key-storage memory assuming every input row is new. Includes hash-table
    /// buffers and arena allocations for the range beginning at `start_row`. Requires an initialized
    /// set and materialized key columns matching the selected method. Saturates at the maximum of
    /// `size_t` when the bound is not representable.
    [[nodiscard]] size_t estimateGrowthMemory(const ColumnRawPtrs & key_columns, size_t start_row, size_t num_rows) const
        requires std::is_same_v<Variant, NonClearableSet>;

    /// Estimates the per-row packed-key buffer of the selected hash method, independently of table
    /// growth. Other methods pack or hash each row on demand and need no such buffer.
    /// Requires an initialized set and key sizes matching the selected method.
    [[nodiscard]] size_t estimatePreparedKeysMemory(size_t num_rows, const Sizes & key_sizes) const;

    [[nodiscard]] size_t getTotalRowCount() const;
    /// Counts the size in bytes of the Set buffer and the size of the `string_pool`
    [[nodiscard]] size_t getTotalByteCount() const;
};

using SetVariants = SetVariantsTemplate<NonClearableSet>;
using ClearableSetVariants = SetVariantsTemplate<ClearableSet>;
using CountingSetVariants = SetVariantsTemplate<CountingSet>;

}
