#pragma once

#include <Common/ColumnsHashingImpl.h>
#include <Common/SipHash.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Interpreters/AggregationCommon.h>

namespace DB
{
using Sizes = std::vector<size_t>;
}

namespace DB::ColumnsHashing
{

/// Hash a set of keys into a UInt128 value.
static inline UInt128 ALWAYS_INLINE hash128( /// NOLINT
    size_t i,
    size_t keys_size,
    const ColumnRawPtrs & key_columns)
{
    SipHash hash;
    for (size_t j = 0; j < keys_size; ++j)
        key_columns[j]->updateHashWithValue(i, hash);

    return hash.get128();
}

/// For the case when there is one numeric key.
/// UInt8/16/32/64 for any type with corresponding bit width.
template <typename Value, typename Mapped, typename FieldType, bool use_cache = true, bool need_offset = false, bool nullable = false>
struct HashMethodOneNumber : public columns_hashing_impl::HashMethodBase<
                                 HashMethodOneNumber<Value, Mapped, FieldType, use_cache, need_offset, nullable>,
                                 Value,
                                 Mapped,
                                 use_cache,
                                 need_offset,
                                 nullable>
{
    using Self = HashMethodOneNumber<Value, Mapped, FieldType, use_cache, need_offset, nullable>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;

    static constexpr bool has_cheap_key_calculation = true;

    const char * vec;

    /// If the keys of a fixed length then key_sizes contains their lengths, empty otherwise.
    HashMethodOneNumber(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &) : Base(key_columns[0])
    {
        if constexpr (nullable)
        {
            const auto & null_column = checkAndGetColumn<ColumnNullable>(*key_columns[0]);
            vec = null_column.getNestedColumnPtr()->getRawData().data();
        }
        else
        {
            vec = key_columns[0]->getRawData().data();
        }
    }

    explicit HashMethodOneNumber(const IColumn * column) : Base(column)
    {
        if constexpr (nullable)
        {
            const auto & null_column = checkAndGetColumn<ColumnNullable>(*column);
            vec = null_column.getNestedColumnPtr()->getRawData().data();
        }
        else
        {
            vec = column->getRawData().data();
        }
    }

    /// Creates context. Method is called once and result context is used in all threads.
    using Base::createContext; /// (const HashMethodContext::Settings &) -> HashMethodContextPtr

    /// Emplace key into HashTable or HashMap. If Data is HashMap, returns ptr to value, otherwise nullptr.
    /// Data is a HashTable where to insert key from column's row.
    /// For Serialized method, key may be placed in pool.
    using Base::emplaceKey; /// (Data & data, size_t row, Arena & pool) -> EmplaceResult

    /// Find key into HashTable or HashMap. If Data is HashMap and key was found, returns ptr to value, otherwise nullptr.
    using Base::findKey;  /// (Data & data, size_t row, Arena & pool) -> FindResult

    /// Get hash value of row.
    using Base::getHash; /// (const Data & data, size_t row, Arena & pool) -> size_t

    /// Is used for default implementation in HashMethodBase.
    FieldType getKeyHolder(size_t row, Arena &) const { return unalignedLoad<FieldType>(vec + row * sizeof(FieldType)); }

    const FieldType * getKeyData() const { return reinterpret_cast<const FieldType *>(vec); }
};


/// For the case when there is one string key.
template <
    typename Value,
    typename Mapped,
    bool place_string_to_arena = true,
    bool use_cache = true,
    bool need_offset = false,
    bool nullable = false>
struct HashMethodString : public columns_hashing_impl::HashMethodBase<
                              HashMethodString<Value, Mapped, place_string_to_arena, use_cache, need_offset, nullable>,
                              Value,
                              Mapped,
                              use_cache,
                              need_offset,
                              nullable>
{
    using Self = HashMethodString<Value, Mapped, place_string_to_arena, use_cache, need_offset, nullable>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;

    static constexpr bool has_cheap_key_calculation = false;

    const IColumn::Offset * offsets;
    const UInt8 * chars;

    HashMethodString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &) : Base(key_columns[0])
    {
        const IColumn * column;
        if constexpr (nullable)
        {
            column = checkAndGetColumn<ColumnNullable>(*key_columns[0]).getNestedColumnPtr().get();
        }
        else
        {
            column = key_columns[0];
        }
        const ColumnString & column_string = assert_cast<const ColumnString &>(*column);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();
    }

    auto getKeyHolder(ssize_t row, [[maybe_unused]] Arena & pool) const
    {
        StringRef key(chars + offsets[row - 1], offsets[row] - offsets[row - 1]);

        if constexpr (place_string_to_arena)
        {
            return ArenaKeyHolder{key, pool};
        }
        else
        {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;
};


/// For the case when there is one fixed-length string key.
template <
    typename Value,
    typename Mapped,
    bool place_string_to_arena = true,
    bool use_cache = true,
    bool need_offset = false,
    bool nullable = false>
struct HashMethodFixedString : public columns_hashing_impl::HashMethodBase<
                                   HashMethodFixedString<Value, Mapped, place_string_to_arena, use_cache, need_offset, nullable>,
                                   Value,
                                   Mapped,
                                   use_cache,
                                   need_offset,
                                   nullable>
{
    using Self = HashMethodFixedString<Value, Mapped, place_string_to_arena, use_cache, need_offset, nullable>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;

    static constexpr bool has_cheap_key_calculation = false;

    size_t n;
    const ColumnFixedString::Chars * chars;

    HashMethodFixedString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &) : Base(key_columns[0])
    {
        const IColumn * column;
        if constexpr (nullable)
        {
            column = checkAndGetColumn<ColumnNullable>(*key_columns[0]).getNestedColumnPtr().get();
        }
        else
        {
            column = key_columns[0];
        }
        const ColumnFixedString & column_string = assert_cast<const ColumnFixedString &>(*column);
        n = column_string.getN();
        chars = &column_string.getChars();
    }

    auto getKeyHolder(size_t row, [[maybe_unused]] Arena & pool) const
    {
        StringRef key(&(*chars)[row * n], n);

        if constexpr (place_string_to_arena)
        {
            return ArenaKeyHolder{key, pool};
        }
        else
        {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;
};

// Optional mask for low cardinality columns.
template <bool has_low_cardinality>
struct LowCardinalityKeys
{
    ColumnRawPtrs nested_columns;
    ColumnRawPtrs positions;
    Sizes position_sizes;
};

template <>
struct LowCardinalityKeys<false> {};


/// For the case when all keys are of fixed length, and they fit in N (for example, 128) bits.
template <
    typename Value,
    typename Key,
    typename Mapped,
    bool has_nullable_keys_ = false,
    bool has_low_cardinality_ = false,
    bool use_cache = true,
    bool need_offset = false>
struct HashMethodKeysFixed
    : private columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>
    , public columns_hashing_impl::HashMethodBase<HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, has_low_cardinality_, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, has_low_cardinality_, use_cache, need_offset>;
    using BaseHashed = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;
    using Base = columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>;

    static constexpr bool has_nullable_keys = has_nullable_keys_;
    static constexpr bool has_low_cardinality = has_low_cardinality_;

    static constexpr bool has_cheap_key_calculation = true;

    LowCardinalityKeys<has_low_cardinality> low_cardinality_keys;
    Sizes key_sizes;
    size_t keys_size;

    /// SSSE3 shuffle method can be used. Shuffle masks will be calculated and stored here.
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
    std::unique_ptr<uint8_t[]> masks;
    std::unique_ptr<const char*[]> columns_data;
#endif

    PaddedPODArray<Key> prepared_keys;

    static bool usePreparedKeys(const Sizes & key_sizes)
    {
        if (has_low_cardinality || has_nullable_keys || sizeof(Key) > 16)
            return false;

        for (auto size : key_sizes)
            if (size != 1 && size != 2 && size != 4 && size != 8 && size != 16)
                return false;

        return true;
    }

    HashMethodKeysFixed(const ColumnRawPtrs & key_columns, const Sizes & key_sizes_, const HashMethodContextPtr &)
        : Base(key_columns), key_sizes(key_sizes_), keys_size(key_columns.size())
    {
        if constexpr (has_low_cardinality)
        {
            low_cardinality_keys.nested_columns.resize(key_columns.size());
            low_cardinality_keys.positions.assign(key_columns.size(), nullptr);
            low_cardinality_keys.position_sizes.resize(key_columns.size());
            for (size_t i = 0; i < key_columns.size(); ++i)
            {
                if (const auto * low_cardinality_col = typeid_cast<const ColumnLowCardinality *>(key_columns[i]))
                {
                    low_cardinality_keys.nested_columns[i] = low_cardinality_col->getDictionary().getNestedColumn().get();
                    low_cardinality_keys.positions[i] = &low_cardinality_col->getIndexes();
                    low_cardinality_keys.position_sizes[i] = low_cardinality_col->getSizeOfIndexType();
                }
                else
                    low_cardinality_keys.nested_columns[i] = key_columns[i];
            }
        }

        if (usePreparedKeys(key_sizes))
        {
            packFixedBatch(keys_size, Base::getActualColumns(), key_sizes, prepared_keys);
        }

#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
        else if constexpr (!has_low_cardinality && !has_nullable_keys && sizeof(Key) <= 16)
        {
            /** The task is to "pack" multiple fixed-size fields into single larger Key.
              * Example: pack UInt8, UInt32, UInt16, UInt64 into UInt128 key:
              * [- ---- -- -------- -] - the resulting uint128 key
              *  ^  ^   ^   ^       ^
              *  u8 u32 u16 u64    zero
              *
              * We can do it with the help of SSSE3 shuffle instruction.
              *
              * There will be a mask for every GROUP BY element (keys_size masks in total).
              * Every mask has 16 bytes but only sizeof(Key) bytes are used (other we don't care).
              *
              * Every byte in the mask has the following meaning:
              * - if it is 0..15, take the element at this index from source register and place here in the result;
              * - if it is 0xFF - set the elemend in the result to zero.
              *
              * Example:
              * We want to copy UInt32 to offset 1 in the destination and set other bytes in the destination as zero.
              * The corresponding mask will be: FF, 0, 1, 2, 3, FF, FF, FF, FF, FF, FF, FF, FF, FF, FF, FF
              *
              * The max size of destination is 16 bytes, because we cannot process more with SSSE3.
              *
              * The method is disabled under MSan, because it's allowed
              * to load into SSE register and process up to 15 bytes of uninitialized memory in columns padding.
              * We don't use this uninitialized memory but MSan cannot look "into" the shuffle instruction.
              *
              * 16-bytes masks can be placed overlapping, only first sizeof(Key) bytes are relevant in each mask.
              * We initialize them to 0xFF and then set the needed elements.
              */
            size_t total_masks_size = sizeof(Key) * keys_size + (16 - sizeof(Key));
            masks.reset(new uint8_t[total_masks_size]);
            memset(masks.get(), 0xFF, total_masks_size);

            size_t offset = 0;
            for (size_t i = 0; i < keys_size; ++i)
            {
                for (size_t j = 0; j < key_sizes[i]; ++j)
                {
                    masks[i * sizeof(Key) + offset] = j;
                    ++offset;
                }
            }

            columns_data.reset(new const char*[keys_size]);

            for (size_t i = 0; i < keys_size; ++i)
                columns_data[i] = Base::getActualColumns()[i]->getRawData().data();
        }
#endif
    }

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &) const
    {
        if constexpr (has_nullable_keys)
        {
            auto bitmap = Base::createBitmap(row);
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes, bitmap);
        }
        else
        {
            if constexpr (has_low_cardinality)
                return packFixed<Key, true>(row, keys_size, low_cardinality_keys.nested_columns, key_sizes,
                                            &low_cardinality_keys.positions, &low_cardinality_keys.position_sizes);

            if (!prepared_keys.empty())
                return prepared_keys[row];

#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
            if constexpr (sizeof(Key) <= 16)
            {
                assert(!has_low_cardinality && !has_nullable_keys);
                return packFixedShuffle<Key>(columns_data.get(), keys_size, key_sizes.data(), row, masks.get());
            }
#endif
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes);
        }
    }

    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        if (!usePreparedKeys(key_sizes))
            return {};

        std::vector<IColumn *> new_columns;
        new_columns.reserve(key_columns.size());

        Sizes new_sizes;
        auto fill_size = [&](size_t size)
        {
            for (size_t i = 0; i < key_sizes.size(); ++i)
            {
                if (key_sizes[i] == size)
                {
                    new_columns.push_back(key_columns[i]);
                    new_sizes.push_back(size);
                }
            }
        };

        fill_size(16);
        fill_size(8);
        fill_size(4);
        fill_size(2);
        fill_size(1);

        key_columns.swap(new_columns);
        return new_sizes;
    }
};

/// For the case when there is one string key.
template <typename Value, typename Mapped, bool use_cache = true, bool need_offset = false>
struct HashMethodHashed
    : public columns_hashing_impl::HashMethodBase<HashMethodHashed<Value, Mapped, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Key = UInt128;
    using Self = HashMethodHashed<Value, Mapped, use_cache, need_offset>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

    static constexpr bool has_cheap_key_calculation = false;

    ColumnRawPtrs key_columns;

    HashMethodHashed(ColumnRawPtrs key_columns_, const Sizes &, const HashMethodContextPtr &)
        : key_columns(std::move(key_columns_)) {}

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &) const
    {
        return hash128(row, key_columns.size(), key_columns);
    }
};

}
