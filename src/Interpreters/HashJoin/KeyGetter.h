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

/// Key getter for a single LowCardinality column, tailored to HashJoin. Unlike the aggregation
/// method `HashMethodSingleLowCardinalityColumn`, this one is const-correct on the probe side
/// (probe maps expose `const RowRef`/`const RowRefList` and `ConstLookupResult`), produces an
/// offset-carrying `FindResult` when `use_offset` is set (HashJoin indexes `JoinUsedFlags` by it), and has no null-key
/// path (chooseMethod only routes here for non-nullable dictionaries). It wraps a base method that
/// operates on the dictionary's nested column to produce key holders, and deduplicates the
/// hash-table work per dictionary index within a block — that dedup is the whole point. The
/// probe/left key may be a plain (non-LowCardinality) column even when this map is chosen (joins
/// allow plain T vs LowCardinality(T)); such a column is handled by running the base method on it
/// directly, with no dictionary indirection or deduplication.
template <typename BaseMethod, typename Mapped, bool use_offset>
struct LowCardinalityKeyGetterForJoin
{
    using MappedNonConst = std::remove_const_t<Mapped>;
    static constexpr bool has_mapped = !std::is_same_v<Mapped, void>;
    using EmplaceResult = typename BaseMethod::EmplaceResult;
    using FindResult = typename BaseMethod::FindResult;

    /// Resolving a key needs a dictionary-index lookup; do not advertise it as cheap, which keeps
    /// the probe-loop software prefetch path (which would fight the per-dictionary cache) disabled.
    static constexpr bool has_cheap_key_calculation = false;

    BaseMethod base;
    const IColumn * positions = nullptr;
    size_t size_of_index_type = 0;
    /// Dictionary positions outside it have no saved hash and are hashed from the key.
    std::span<const UInt64> saved_hash;
    ColumnPtr dictionary_holder;

    /// Per-dictionary-index probe cache. We cache a POINTER into the hash-table cell (stable for the
    /// immutable probe phase and for as long as the join result lives — the lazy output dereferences
    /// these pointers later), not a copy of the mapped value: a copy would dangle. Caching pointers
    /// also works for any mapped type, including the move-only AsofRowRefs.
    PaddedPODArray<UInt8> visit_cache;       /// 0 = not visited, 1 = found, 2 = not found
    PaddedPODArray<Mapped *> mapped_cache;
    PaddedPODArray<size_t> offset_cache;

    /// The base method runs on the dictionary's nested column for a LowCardinality key, or directly
    /// on the column itself for a plain key.
    static const IColumn * getBaseColumn(const IColumn * column)
    {
        if (const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(column))
            return low_cardinality_column->getDictionary().getNestedNotNullableColumn().get();
        return column;
    }

    LowCardinalityKeyGetterForJoin(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, const ColumnsHashing::HashMethodContextPtr &)
        : base({getBaseColumn(key_columns[0])}, key_sizes, nullptr)
    {
        /// The build/right key is always LowCardinality (that is why this map was chosen), but the
        /// probe/left key may be a plain column: joins allow plain T vs LowCardinality(T) without a
        /// cast. For a plain column there is no dictionary, so `positions` stays null and the base
        /// method is used directly (no per-dictionary deduplication). The map stores key values, so a
        /// plain probe and a dictionary-encoded build still produce compatible keys.
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

    /// True when the current column is LowCardinality (dictionary path); false for a plain column.
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

    /// Build side: every row must be inserted/appended into the real hash-table cell, so there is no
    /// per-dictionary-index deduplication here (the mapped RowRefList lives in the cell, not behind a
    /// stable pointer as in aggregation). The dictionary speedup is realized on the probe side only.
    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplaceKey(Data & data, size_t row_, Arena & pool)
    {
        /// A plain key (no dictionary) is handled directly by the base method. The build side is
        /// always LowCardinality, so this branch is only reached when a plain key reaches a build.
        if (!isLowCardinality())
            return base.emplaceKey(data, row_, pool);

        const size_t row = getIndexAt(row_);

        auto key_holder = base.getKeyHolder(row, pool);

        typename Data::LookupResult it;
        bool inserted = false;
        if (row < saved_hash.size())
            data.emplace(key_holder, it, inserted, saved_hash[row]);
        else
            data.emplace(key_holder, it, inserted);

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
        /// A plain probe key (no dictionary) is looked up directly by the base method. The map stores
        /// key values, so this finds the rows inserted from the dictionary-encoded build side.
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
