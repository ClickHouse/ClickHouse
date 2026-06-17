#pragma once

#include <Common/ColumnsHashing/HashMethod.h>
#include <Common/assert_cast.h>
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


/// For the case where there is one numeric key.
template <typename FieldType, typename TData, bool use_cache = true, typename Mapped = void>    /// UInt8/16/32/64 for any types with corresponding bit width.
struct SetMethodOneNumber
{
    using Data = TData;
    using Key = typename Data::key_type;

    Data data;

    using State = ColumnsHashing::HashMethodOneNumber<typename Data::value_type,
        Mapped, FieldType, use_cache>;
};

/// For the case where there is one string key.
template <typename TData, typename Mapped = void>
struct SetMethodString
{
    using Data = TData;
    using Key = typename Data::key_type;

    Data data;

    using State = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped, true, false>;
};

/// For the case when there is one fixed-length string key.
template <typename TData, typename Mapped = void>
struct SetMethodFixedString
{
    using Data = TData;
    using Key = typename Data::key_type;

    Data data;

    using State = ColumnsHashing::HashMethodFixedString<typename Data::value_type, Mapped, true, false>;
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
    const ColumnRawPtrs & getActualColumns() const
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

    const ColumnRawPtrs & getActualColumns() const
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
template <typename TData, bool has_nullable_keys_ = false, typename Mapped = void, bool use_cache = true>
struct SetMethodKeysFixed
{
    using Data = TData;
    using Key = typename Data::key_type;
    static constexpr bool has_nullable_keys = has_nullable_keys_;

    Data data;

    using State = ColumnsHashing::HashMethodKeysFixed<typename Data::value_type, Key, Mapped, has_nullable_keys, false, use_cache>;
};

/// For other cases. 128 bit hash from the key.
template <typename TData, typename Mapped = void, bool use_cache = true>
struct SetMethodHashed
{
    using Data = TData;
    using Key = typename Data::key_type;

    Data data;

    using State = ColumnsHashing::HashMethodHashed<typename Data::value_type, Mapped, use_cache>;
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

/// INTERSECT/EXCEPT ALL inputs are rarely tiny, and the default 256-cell start forces several
/// resizes just to absorb the first block. Start the counting tables larger (2^14 = 16384 cells).
static constexpr size_t COUNTING_SET_INITIAL_SIZE_DEGREE = 14;
using CountingSetGrower = HashTableGrowerWithPrecalculation<COUNTING_SET_INITIAL_SIZE_DEGREE>;

/// Like NonClearableSet, but each distinct key carries a UInt64 occurrence count
/// (a multiset). Used by INTERSECT ALL / EXCEPT ALL. `use_cache` is disabled so that
/// `emplaceKey(...).getMapped()` returns a reference to the table slot (the consecutive-keys
/// cache returns a reference to a by-value copy, which would break in-place increment).
struct CountingSet
{
    using Count = UInt64;

    std::unique_ptr<SetMethodOneNumber<UInt8,  FixedHashMap<UInt8,  Count>, false, Count>>  key8;
    std::unique_ptr<SetMethodOneNumber<UInt16, FixedHashMap<UInt16, Count>, false, Count>>  key16;

    std::unique_ptr<SetMethodOneNumber<UInt32, HashMap<UInt32, Count, HashCRC32<UInt32>, CountingSetGrower>, false, Count>> key32;
    std::unique_ptr<SetMethodOneNumber<UInt64, HashMap<UInt64, Count, HashCRC32<UInt64>, CountingSetGrower>, false, Count>> key64;
    std::unique_ptr<SetMethodString<HashMapWithSavedHash<std::string_view, Count, DefaultHash<std::string_view>, CountingSetGrower>, Count>>        key_string;
    std::unique_ptr<SetMethodFixedString<HashMapWithSavedHash<std::string_view, Count, DefaultHash<std::string_view>, CountingSetGrower>, Count>>   key_fixed_string;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt32, Count, HashCRC32<UInt32>, CountingSetGrower>, false, Count, false>>  keys32;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt64, Count, HashCRC32<UInt64>, CountingSetGrower>, false, Count, false>>  keys64;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt128, Count, UInt128HashCRC32, CountingSetGrower>, false, Count, false>>  keys128;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt256, Count, UInt256HashCRC32, CountingSetGrower>, false, Count, false>>  keys256;
    std::unique_ptr<SetMethodHashed<HashMap<UInt128, Count, UInt128TrivialHash, CountingSetGrower>, Count, false>>   hashed;

    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt128, Count, UInt128HashCRC32, CountingSetGrower>, true, Count, false>>   nullable_keys128;
    std::unique_ptr<SetMethodKeysFixed<HashMap<UInt256, Count, UInt256HashCRC32, CountingSetGrower>, true, Count, false>>   nullable_keys256;
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

    bool empty() const { return type == Type::EMPTY; }

    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);

    void init(Type type_);

    size_t getTotalRowCount() const;
    /// Counts the size in bytes of the Set buffer and the size of the `string_pool`
    size_t getTotalByteCount() const;
};

using SetVariants = SetVariantsTemplate<NonClearableSet>;
using ClearableSetVariants = SetVariantsTemplate<ClearableSet>;
using CountingSetVariants = SetVariantsTemplate<CountingSet>;

}
