#pragma once

#include <Common/ColumnsHashingImpl.h>
#include <Common/SipHash.h>
#include <bit>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Interpreters/AggregationCommon.h>
#include <base/types.h>

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

/** Hash methods declare two independent prefetch predicates. They are not interchangeable, and the
  * places that read them are disjoint.
  *
  * `has_cheap_key_holder` - read by `Aggregator::executeImpl`.
  *
  * Is it acceptable to call `getKeyHolder(row)` a second time for the same row purely to issue a
  * software prefetch? The aggregation prefetch pipeline runs
  *
  *     auto && key_holder = state.getKeyHolder(i + look_ahead, pool);
  *     data.prefetch(std::move(key_holder));
  *
  * ahead of the `emplaceKey`/`findKey` loop, so the look-ahead row's key holder is built once for
  * the prefetch and once again when that row is actually processed. Hiding a cache miss is only a
  * win when that duplicated work is cheaper than the miss.
  *
  * `true` means `getKeyHolder` reads the key in place: an unaligned load, a `packFixed`, or a
  * `string_view` over the column's own memory. Rebuilding it costs a handful of instructions.
  *
  * `false` means `getKeyHolder` materializes the key - serializing every key column into the arena,
  * or hashing every key column through virtual `IColumn` calls. For those methods building the key
  * *is* the dominant cost of the aggregation, so paying it twice per row costs far more than the
  * miss it hides.
  *
  * Note this is deliberately not "is the hash cheap". Hashing a string is not cheap, but a prefetch
  * has to hash the key by definition, and `HashMethodString`/`HashMethodPackedString` still profit
  * because building their key holder is free.
  *
  * `has_cheap_key_calculation` - read by the JOIN probe loop, via `join_prefetch_supported` in
  * HashJoinMethodsImpl.h (the `KeyGetterForType` aliases in HashJoin/KeyGetter.h resolve to these
  * same hash methods). It is the stricter "the whole key calculation, hashing included, is cheap",
  * and is left as it was: the JOIN probe loop has its own cost balance, which this file's
  * aggregation-side reasoning says nothing about. Only the aggregator reads
  * `has_cheap_key_holder`.
  */

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
    /// An unaligned load from the column's own memory.
    static constexpr bool has_cheap_key_holder = true;
    static constexpr bool has_pre_computed_hashes = false;

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


/// Like HashMethodOneNumber, but subtracts min_key from each key and validates if the key is in range.
/// Used for hash join's fixed-range optimization, where the hash table stores keys shifted to [0, max_key - min_key].
template <typename Value, typename Mapped, typename FieldType, bool use_cache = true, bool need_offset = false, bool nullable = false>
struct HashMethodOneNumberInRange : public columns_hashing_impl::HashMethodBase<
                                            HashMethodOneNumberInRange<Value, Mapped, FieldType, use_cache, need_offset, nullable>,
                                            Value,
                                            Mapped,
                                            use_cache,
                                            need_offset,
                                            nullable>
{
    using Self = HashMethodOneNumberInRange<Value, Mapped, FieldType, use_cache, need_offset, nullable>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;

    static constexpr bool has_range_check = true;
    static constexpr bool has_cheap_key_calculation = true;
    /// An unaligned load from the column's own memory.
    static constexpr bool has_cheap_key_holder = true;

    const char * vec;
    FieldType min_key{};
    FieldType range_size{};

    HashMethodOneNumberInRange(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
        : HashMethodOneNumberInRange(key_columns[0])
    {
    }

    explicit HashMethodOneNumberInRange(const IColumn * column) : Base(column)
    {
        if constexpr (nullable)
            vec = checkAndGetColumn<ColumnNullable>(*column).getNestedColumnPtr()->getRawData().data();
        else
            vec = column->getRawData().data();
    }

    using Base::createContext;
    using Base::emplaceKey;
    using Base::findKey;
    using Base::getHash;

    FieldType getKeyHolder(size_t row, Arena &) const
    {
        return unalignedLoad<FieldType>(vec + row * sizeof(FieldType)) - min_key;
    }

    std::pair<FieldType, bool> getKeyHolderInRange(size_t row, Arena &) const
    {
        FieldType shifted_key = unalignedLoad<FieldType>(vec + row * sizeof(FieldType)) - min_key;
        return {shifted_key, shifted_key < range_size};
    }
};


template <typename T>
struct IsHashMethodInRange : std::false_type {};

template <typename Value, typename Mapped, typename FieldType, bool use_cache, bool need_offset, bool nullable>
struct IsHashMethodInRange<HashMethodOneNumberInRange<Value, Mapped, FieldType, use_cache, need_offset, nullable>> : std::true_type {};


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
    /// A `string_view` over the column's own chars; the arena copy only happens on persist.
    static constexpr bool has_cheap_key_holder = true;
    static constexpr bool has_pre_computed_hashes = false;

    const IColumn::Offset * offsets;
    const UInt8 * chars;

    HashMethodString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &) : Base(key_columns[0])
    {
        const IColumn * column = nullptr;
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
        std::string_view key(reinterpret_cast<const char *>(chars) + offsets[row - 1], offsets[row] - offsets[row - 1]);

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

/// For the case when there is one packed string key.
/// Unlike `HashMethodString`, this method does not support nullable keys or key offsets,
/// and the key is always persisted into the arena by `keyHolderPersistKey`.
template <typename Value, typename Mapped, bool use_cache>
struct HashMethodPackedString : public columns_hashing_impl::HashMethodBase<
                              HashMethodPackedString<Value, Mapped, use_cache>,
                              Value,
                              Mapped,
                              use_cache,
                              /*need_offset=*/ false,
                              /*nullable=*/ false>
{
    using Self = HashMethodPackedString<Value, Mapped, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, false, false>;

    static constexpr bool has_cheap_key_calculation = false;
    /// `PackedStringRef::build` reads the key in place; see the note on `Hash` below for why
    /// rebuilding it (and its content hash) in the look-ahead is an accepted trade here.
    static constexpr bool has_cheap_key_holder = true;

    const IColumn::Offset * offsets;
    const UInt8 * chars;

    HashMethodPackedString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
        : Base(key_columns[0])
    {
        const ColumnString & column_string = assert_cast<const ColumnString &>(*key_columns[0]);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();
    }

    /// Content hash stored inside the packed key. `PackedStringRef::build` invokes it
    /// only for lengths that store a hash (1..UInt32 max): the empty value hashes to
    /// zero by construction and oversized strings use the length as a hash surrogate,
    /// trading hash quality for a uniform cell layout (full string comparison remains
    /// the final equality check).
    ///
    /// Computing the hash inside `build` keeps a single pass over the string data:
    /// a separate per-block hashing pass would read every key twice and allocate a
    /// hash array per block. The flip side is that when the `Aggregator` prefetch
    /// pipeline is active (hash table larger than L2), the look-ahead `getKeyHolder`
    /// call rebuilds the key and hashes it a second time - the same behaviour as the
    /// `StringHashTable` prefetch path this method replaces.
    ///
    /// A 32-bit hash is sufficient for in-memory aggregation hash tables; external
    /// aggregation derives a 64-bit hash via a dedicated conversion path.
    struct Hash
    {
        ALWAYS_INLINE UInt32 operator()(const char * data, size_t size) const
        {
#if defined(CRC_INT)
            /// Tiny keys (1..7 bytes) go through a single CRC instruction on the masked
            /// word, exactly like `StringHashTableHash` for `StringKey8`. This avoids the
            /// multiply-heavy `hashLessThan8` path inside `StringViewHash` for short strings,
            /// which otherwise dominates low-cardinality short-string aggregation
            /// (`group_by_sundy_li`, `if_transform_strings_to_enum`). Keys of 8 bytes or more
            /// already use the cheap CRC loop in `StringViewHash` and are left unchanged, so
            /// medium/large keys (e.g. URLs) keep the same hash and bucketing as before.
            if (size < 8)
                return hashTinyKey(data, size);
#endif
            return static_cast<UInt32>(StringViewHash()(std::string_view(data, size)));
        }
    };

#if defined(CRC_INT)
    /// Hash a 1..7 byte key with a single CRC instruction.
    /// Reading 8 bytes from the key start is safe: `ColumnString::Chars` is a `PaddedPODArray`
    /// with at least 15 bytes of right padding, so the load never crosses the allocation end.
    /// Trailing bytes beyond the key length are masked off, so the result depends only on the
    /// key content and is independent of neighbouring data.
    static ALWAYS_INLINE UInt32 hashTinyKey(const char * data, size_t size)
    {
        const UInt8 shift = static_cast<UInt8>((-size & 7) * 8);
        UInt64 word = 0;
        memcpy(&word, data, sizeof(word));
        /// `memcpy` places the key in the low bytes of `word` on little-endian and in the high
        /// bytes on big-endian, so the trailing-byte mask has to follow the same direction.
        /// `CRC_INT` is also defined on big-endian s390x, so masking the wrong end there would
        /// fold neighbouring padding bytes into the hash and split a single tiny key into
        /// several groups (the hash is stored in `PackedStringRef::low` and gates `operator==`).
        if constexpr (std::endian::native == std::endian::little)
            word &= (~UInt64(0) >> shift);
        else
            word &= (~UInt64(0) << shift);
        size_t res = static_cast<size_t>(-1);
        res = CRC_INT(static_cast<UInt32>(res), word);
        return static_cast<UInt32>(res);
    }
#endif

    ArenaPackedStringHolder getKeyHolder(ssize_t row, Arena & pool) const
    {
        const char * data = reinterpret_cast<const char *>(chars + offsets[row - 1]);
        const size_t size = offsets[row] - offsets[row - 1];
        return ArenaPackedStringHolder{PackedStringRef::build(data, size, Hash{}), pool};
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, false, false>;
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
    /// A `string_view` over the column's own chars; the arena copy only happens on persist.
    static constexpr bool has_cheap_key_holder = true;
    static constexpr bool has_pre_computed_hashes = false;

    size_t n;
    const ColumnFixedString::Chars * chars;

    HashMethodFixedString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &) : Base(key_columns[0])
    {
        const IColumn * column = nullptr;
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
        std::string_view key(reinterpret_cast<const char *>(&(*chars)[row * n]), n);
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
    /// `packFixed` copies a few fixed-width fields into the key; no allocation.
    static constexpr bool has_cheap_key_holder = true;
    static constexpr bool has_pre_computed_hashes = false;

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
                    masks[i * sizeof(Key) + offset] = static_cast<uint8_t>(j);
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
                chassert(!has_low_cardinality && !has_nullable_keys);
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
    /// `hash128` SipHashes every key column through a virtual `IColumn::updateHashWithValue`.
    static constexpr bool has_cheap_key_holder = false;
    static constexpr bool has_pre_computed_hashes = false;

    ColumnRawPtrs key_columns;

    HashMethodHashed(ColumnRawPtrs key_columns_, const Sizes &, const HashMethodContextPtr &)
        : key_columns(std::move(key_columns_)) {}

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &) const
    {
        return hash128(row, key_columns.size(), key_columns);
    }
};

}
