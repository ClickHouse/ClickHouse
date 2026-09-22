#pragma once
#include <Interpreters/HashJoin/HashJoin.h>
#include <Common/ColumnsHashing.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <HashJoin::Type type, typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl;

/// Does the hash-table work once per dictionary index rather than once per row. Not aggregation's
/// `HashMethodSingleLowCardinalityColumn`, which is not const-correct on probe and carries no
/// `JoinUsedFlags` offset.
template <typename BaseMethod, typename Mapped, bool use_offset>
struct LowCardinalityKeyGetterForJoin
{
    using MappedNonConst = std::remove_const_t<Mapped>;
    static constexpr bool has_mapped = !std::is_same_v<Mapped, void>;
    using EmplaceResult = BaseMethod::EmplaceResult;
    using FindResult = BaseMethod::FindResult;

    /// Resolving a key costs a dictionary lookup, and prefetching would fight the cache below.
    static constexpr bool has_cheap_key_calculation = false;

    BaseMethod base;
    const IColumn * positions = nullptr;
    size_t size_of_index_type = 0;
    /// Dictionary positions outside it have no saved hash and are hashed from the key.
    std::span<const UInt64> saved_hash;
    ColumnPtr dictionary_holder;

    /// Pointers into the cells, not copies: the join result dereferences them lazily, and a copy
    /// would dangle - besides not working at all for move-only `AsofRowRefs`.
    PaddedPODArray<UInt8> visit_cache;       /// 0 = not visited, 1 = found, 2 = not found
    PaddedPODArray<Mapped *> mapped_cache;
    PaddedPODArray<size_t> offset_cache;

    static const IColumn * getBaseColumn(const IColumn * column)
    {
        if (const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(column))
            return low_cardinality_column->getDictionary().getNestedNotNullableColumn().get();
        return column;
    }

    LowCardinalityKeyGetterForJoin(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, const ColumnsHashing::HashMethodContextPtr &)
        : base({getBaseColumn(key_columns[0])}, key_sizes, nullptr)
    {
        /// A join accepts plain T against LowCardinality(T), so the probe key may have no
        /// dictionary; keys still match because the map stores values, not dictionary indices.
        const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(key_columns[0]);
        if (!low_cardinality_column)
            return;

        const auto & dictionary = low_cardinality_column->getDictionary();
        dictionary_holder = low_cardinality_column->getDictionaryPtr();
        saved_hash = dictionary.tryGetSavedHash();
        size_of_index_type = low_cardinality_column->getSizeOfIndexType();
        positions = low_cardinality_column->getIndexesPtr().get();

        const size_t dictionary_size = dictionary.getNestedNotNullableColumn()->size();
        visit_cache.assign(dictionary_size, static_cast<UInt8>(0));
        mapped_cache.assign(dictionary_size, static_cast<Mapped *>(nullptr));
        if constexpr (use_offset)
            offset_cache.assign(dictionary_size, static_cast<size_t>(0));
    }

    ALWAYS_INLINE bool isLowCardinality() const { return positions != nullptr; }

    ALWAYS_INLINE size_t getIndexAt(size_t row) const
    {
        switch (size_of_index_type)
        {
            case sizeof(UInt8):  return assert_cast<const ColumnUInt8 *>(positions)->getElement(row);
            case sizeof(UInt16): return assert_cast<const ColumnUInt16 *>(positions)->getElement(row);
            case sizeof(UInt32): return assert_cast<const ColumnUInt32 *>(positions)->getElement(row);
            case sizeof(UInt64): return assert_cast<const ColumnUInt64 *>(positions)->getElement(row);
            default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column.");
        }
    }

    ALWAYS_INLINE auto getKeyHolder(size_t row, Arena & pool) const
    {
        return base.getKeyHolder(isLowCardinality() ? getIndexAt(row) : row, pool);
    }

    template <typename Data>
    ALWAYS_INLINE size_t routingHashForRow(const Data & data, size_t row_, Arena & pool) const
    {
        if (!isLowCardinality())
        {
            auto key_holder = base.getKeyHolder(row_, pool);
            return data.hash(keyHolderGetKey(key_holder));
        }

        const size_t row = getIndexAt(row_);
        /// The saved hash is of the key value, which is what the map places by.
        if (row < saved_hash.size())
            return saved_hash[row];

        auto key_holder = base.getKeyHolder(row, pool);
        return data.hash(keyHolderGetKey(key_holder));
    }

    /// No per-index dedup: the row-ref list lives in the cell, so every row has to reach it.
    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplaceKey(Data & data, size_t row_, Arena & pool)
    {
        if (!isLowCardinality())
            return base.emplaceKey(data, row_, pool);

        const size_t row = getIndexAt(row_);

        auto key_holder = base.getKeyHolder(row, pool);

        typename Data::LookupResult it;
        bool inserted = false;
        data.emplace(key_holder, it, inserted, routingHashForRow(data, row_, pool));

        if constexpr (has_mapped)
        {
            auto & mapped = it->getMapped();
            if (inserted)
                new (&mapped) MappedNonConst();
            return EmplaceResult(mapped, mapped, inserted);
        }
        else
            return EmplaceResult(inserted);
    }

    template <typename Data>
    ALWAYS_INLINE FindResult findKey(Data & data, size_t row_, Arena & pool)
    {
        if (!isLowCardinality())
            return base.findKey(data, row_, pool);

        const size_t row = getIndexAt(row_);

        if (visit_cache[row] != 0)
        {
            size_t cached_offset = 0;
            if constexpr (use_offset)
                cached_offset = offset_cache[row];
            if constexpr (has_mapped)
                return FindResult(mapped_cache[row], visit_cache[row] == 1, cached_offset);
            else
                return FindResult(visit_cache[row] == 1, cached_offset);
        }

        auto key_holder = base.getKeyHolder(row, pool);
        const auto key = keyHolderGetKey(key_holder);

        auto it = row < saved_hash.size() ? data.find(key, saved_hash[row]) : data.find(key);

        const bool found = it;
        /// Only the used-flag paths ask; `freezeMapsForProbing` computed the prefix sums
        /// before any probe.
        size_t offset = 0;
        if constexpr (use_offset)
            offset = found ? data.offsetInternal(it) : 0;

        visit_cache[row] = found ? 1 : 2;
        if constexpr (use_offset)
            offset_cache[row] = offset;

        if constexpr (has_mapped)
        {
            Mapped * mapped = found ? &it->getMapped() : nullptr;
            mapped_cache[row] = mapped;
            return FindResult(mapped, found, offset);
        }
        else
            return FindResult(found, offset);
    }
};

template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::key8, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::key16, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::key32, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::key64, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::key_string, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::key_fixed_string, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::keys32, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt32, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::keys64, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt64, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::keys128, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::keys256, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::hashed, Value, Mapped, use_offset>
{
    using Type = ColumnsHashing::HashMethodHashed<Value, Mapped, false, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::low_cardinality_key_string, Value, Mapped, use_offset>
{
    using Type
        = LowCardinalityKeyGetterForJoin<ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>, Mapped, use_offset>;
};
template <typename Value, typename Mapped, bool use_offset>
struct KeyGetterForTypeImpl<HashJoin::Type::low_cardinality_key_fixed_string, Value, Mapped, use_offset>
{
    using Type
        = LowCardinalityKeyGetterForJoin<ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>, Mapped, use_offset>;
};
#define KEYGETTER_RANGE_IMPL(TYPE, FIELD_TYPE) \
    template <typename Value, typename Mapped, bool use_offset> \
    struct KeyGetterForTypeImpl<HashJoin::Type::TYPE, Value, Mapped, use_offset> \
    { \
        using Type = ColumnsHashing::HashMethodOneNumberInRange<Value, Mapped, FIELD_TYPE, false, use_offset>; \
    };
KEYGETTER_RANGE_IMPL(range8_key32, UInt32)
KEYGETTER_RANGE_IMPL(range16_key32, UInt32)
KEYGETTER_RANGE_IMPL(range17_key32, UInt32)
KEYGETTER_RANGE_IMPL(range18_key32, UInt32)
KEYGETTER_RANGE_IMPL(range8_key64, UInt64)
KEYGETTER_RANGE_IMPL(range16_key64, UInt64)
KEYGETTER_RANGE_IMPL(range17_key64, UInt64)
KEYGETTER_RANGE_IMPL(range18_key64, UInt64)
#undef KEYGETTER_RANGE_IMPL

#define KEYGETTER_TWO_LEVEL_IMPL(NAME) \
    template <typename Value, typename Mapped, bool use_offset> \
    struct KeyGetterForTypeImpl<HashJoin::Type::two_level_##NAME, Value, Mapped, use_offset> \
        : KeyGetterForTypeImpl<HashJoin::Type::NAME, Value, Mapped, use_offset> \
    { \
    };
APPLY_FOR_SINGLE_LEVEL_JOIN_VARIANTS(KEYGETTER_TWO_LEVEL_IMPL)
KEYGETTER_TWO_LEVEL_IMPL(key8)
KEYGETTER_TWO_LEVEL_IMPL(key16)
#undef KEYGETTER_TWO_LEVEL_IMPL

/// Set tables, which joins that never read a right-side row run on, have no mapped value in their
/// cells: they report the `VoidMapped` placeholder, while the column hashing methods spell the absence
/// of a mapped value as `void`.
template <typename Data>
struct JoinMappedType
{
    using Raw = typename std::remove_const_t<Data>::mapped_type;
    using Type = std::conditional_t<std::is_same_v<Raw, VoidMapped>, void, Raw>;
};

template <HashJoin::Type type, typename Data, bool use_offset>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename JoinMappedType<Data>::Type;
    using Mapped = std::conditional_t<std::is_const_v<Data> && !std::is_void_v<Mapped_t>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped, use_offset>::Type;
};

/// Builds one clause's key getter. For ASOF the last key column is the inequality one, which the
/// getter must not see. Only the range getters read `key_range`; callers that run before the build
/// settles it pass the default, i.e. no shift.
template <typename KeyGetter, bool is_asof_join>
KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, HashJoin::RightTableData::KeyRange key_range = {})
{
    KeyGetter getter = [&]
    {
        if constexpr (is_asof_join)
        {
            auto key_column_copy = key_columns;
            auto key_size_copy = key_sizes;
            key_column_copy.pop_back();
            key_size_copy.pop_back();
            return KeyGetter(key_column_copy, key_size_copy, nullptr);
        }
        else
            return KeyGetter(key_columns, key_sizes, nullptr);
    }();

    if constexpr (ColumnsHashing::IsHashMethodInRange<KeyGetter>::value)
    {
        getter.min_key = static_cast<decltype(getter.min_key)>(key_range.min_key);
        getter.range_size = static_cast<decltype(getter.range_size)>(key_range.size);
    }

    return getter;
}
}
