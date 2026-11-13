#pragma once

#include <Common/ColumnsHashingImpl.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
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
  * the prefetch and once again when that row is actually processed. A key that stores its hash
  * (`key_holder_key_stores_hash`) is the exception: the loop keeps the keys built for the prefetch
  * and inserts them (see `LookAheadKeysFor`). Hiding a cache miss is only a win when the duplicated
  * work is cheaper than the miss.
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
    HashMethodOneNumber(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &, RowRange /*rows*/ = {}) : Base(key_columns[0])
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

    HashMethodString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &, RowRange /*rows*/ = {}) : Base(key_columns[0])
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

    /// Returns the row's bytes in the padded source column, without persisting the key. The view remains
    /// valid for this block and permits reads up to 15 bytes past its end.
    std::string_view getKeyBytes(ssize_t row) const
    {
        return {reinterpret_cast<const char *>(chars) + offsets[row - 1], offsets[row] - offsets[row - 1]};
    }

    auto getKeyHolder(ssize_t row, [[maybe_unused]] Arena & pool) const
    {
        const auto key = getKeyBytes(row);

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

/// Content hash stored inside a packed string key. `PackedStringRef::build` invokes it
/// only for lengths that store a hash (1..UInt32 max): the empty value hashes to
/// zero by construction and oversized strings use the length as a hash surrogate,
/// trading hash quality for a uniform cell layout (full string comparison remains
/// the final equality check).
///
/// Computing the hash inside `build` keeps a single pass over the string data:
/// a separate per-block hashing pass would read every key twice and allocate a
/// hash array per block. When the `Aggregator` prefetches (hash table larger than
/// L2) it builds a row's key ahead of the row and keeps it for the insert (see
/// `KeptLookAheadKeys`), so the string is hashed once.
///
/// A 32-bit hash is sufficient for in-memory aggregation hash tables; external
/// aggregation derives a 64-bit hash via a dedicated conversion path.
struct PackedStringHash
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
    /// `PackedStringRef::build` reads the key in place, and the look-ahead keeps the keys it builds (see
    /// `KeptLookAheadKeys`), so a key and its content hash are built once.
    static constexpr bool has_cheap_key_holder = true;

    /// The content hash stored in the packed key. Callers that rebuild a key from its bytes to probe a table
    /// use it.
    using Hash = PackedStringHash;

    const IColumn::Offset * offsets;
    const UInt8 * chars;

    HashMethodPackedString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &, RowRange /*rows*/ = {})
        : Base(key_columns[0])
    {
        const ColumnString & column_string = assert_cast<const ColumnString &>(*key_columns[0]);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();
    }

    /// Returns the row's bytes in the padded source column, without packing or persisting the key. The
    /// view remains valid for this block and permits reads up to 15 bytes past its end.
    std::string_view getKeyBytes(ssize_t row) const
    {
        const char * data = reinterpret_cast<const char *>(chars + offsets[row - 1]);
        const size_t size = offsets[row] - offsets[row - 1];
        return {data, size};
    }

    ALWAYS_INLINE ArenaPackedStringHolder getKeyHolder(ssize_t row, Arena & pool) const
    {
        const auto key = getKeyBytes(row);
        return ArenaPackedStringHolder{PackedStringRef::build(key.data(), key.size(), Hash{}), pool};
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, false, false>;
};

/// For the case of fixed-width keys and one non-nullable `String` key, stored as a
/// `KeysAndPackedString<Keys>`: the fixed-width values packed into `Keys` longest first, and the string as a
/// packed reference. Like `HashMethodPackedString`, it reads the key in place and persists the string only
/// when the key is inserted. It keeps no consecutive-keys cache, because `KeysAndPackedStringHashTable`
/// returns bare mapped pointers, or untyped cell addresses for a set, from which the cache cannot read the key.
/// With `for_sub_range`, it is the `SubRangeState` of the method.
///
/// The `String` key can be `LowCardinality(String)`: the string of a row is then its dictionary entry. When
/// the dictionary has no more entries than the state has rows, the state hashes each entry for the first row
/// with it and reuses the hash for the later ones; otherwise each row hashes its string (see `entry_hashes`).
/// The key and its hash are the same as from a `String` column, so shards that store the column as `String`
/// and as `LowCardinality(String)` number the two-level buckets alike, which the merge of a distributed query
/// over both needs. A `LowCardinality` fixed-width key column is materialized, as the packing reads the
/// values in place. A state of a sub-range of the block materializes only its rows. Aggregation in order
/// builds such a state for every run of equal sorting key values, so its work stays proportional to the run.
template <typename Value, typename Mapped, typename Keys, bool for_sub_range = false>
struct HashMethodKeysAndPackedString : public columns_hashing_impl::HashMethodBase<
                                           HashMethodKeysAndPackedString<Value, Mapped, Keys, for_sub_range>,
                                           Value,
                                           Mapped,
                                           /*consecutive_keys_optimization=*/ false,
                                           /*need_offset=*/ false,
                                           /*nullable=*/ false>
{
    using Self = HashMethodKeysAndPackedString<Value, Mapped, Keys, for_sub_range>;

    static constexpr bool has_cheap_key_calculation = false;
    /// As in `HashMethodPackedString`, the key is read in place, and the look-ahead keeps the keys it builds
    /// (see `KeptLookAheadKeys`), so a key and its hash are built once.
    static constexpr bool has_cheap_key_holder = true;

    /// The hash that the key stores (see `KeysAndPackedString`): the content hash of the string, continued
    /// with the fixed-width values, so that it covers the whole key.
    struct Hash
    {
        Keys keys;

        /// Continues the content hash of a string (`PackedStringHash`) with the fixed-width values.
        static ALWAYS_INLINE UInt32 combine(Keys fixed_keys, UInt32 content_hash)
        {
            return static_cast<UInt32>(intHashCRC32(fixed_keys, content_hash));
        }

        ALWAYS_INLINE UInt32 operator()(const char * data, size_t size) const
        {
            return combine(keys, PackedStringHash{}(data, size));
        }
    };

    /// The fixed-width key columns and the sizes of their values (1, 2, 4, 8 or 16 bytes), in the order of
    /// the key columns. A `LowCardinality` column is replaced by its copy materialized into
    /// `materialized_columns`, except in a state of a sub-range, which packs a copy of its rows (see
    /// `packCopyOfSubRange`).
    ColumnRawPtrs fixed_columns;
    Sizes fixed_sizes;
    Columns materialized_columns;

    /// The values of the only fixed-width key column when they are as wide as `Keys`: such values are their
    /// own packed form, so they are read in place, without packing. Null otherwise.
    const Keys * column_keys = nullptr;

    /// The fixed-width values of the block, packed longest first (see `packFixedBatch`). A state built for a
    /// sub-range of the block holds those of the sub-range, from row `prepared_keys_begin` on. It holds none
    /// for a sub-range shorter than `min_rows_to_batch_pack_sub_range` without a `LowCardinality` fixed-width
    /// key, whose rows it packs one by one.
    PaddedPODArray<Keys> prepared_keys;
    size_t prepared_keys_begin = 0;

    /// The buffers of the `String` key column, whose rows are the strings, or of the dictionary of a
    /// `LowCardinality(String)` key column, whose entries are.
    const IColumn::Offset * string_offsets = nullptr;
    const UInt8 * string_chars = nullptr;

    /// A `LowCardinality(String)` key column, whose rows are positions in its dictionary.
    const ColumnLowCardinality * low_cardinality_strings = nullptr;

    /// The content hashes of the dictionary entries that rows have asked for, so that the rows of one entry
    /// hash its string once. 0 marks an entry not hashed yet, so a string whose hash is 0 is hashed for each
    /// of its rows. `getKeyHolder` fills it, so it is mutable. It is empty when the dictionary has more
    /// entries than the state has rows, as for a filtered or cut block, which keeps the whole dictionary of
    /// its source: each row then hashes its string, so the state does no work for entries that its rows do
    /// not use.
    mutable PaddedPODArray<UInt32> entry_hashes;

    /// The `String` key is the one column without a value size. A state built for a sub-range of the block
    /// (`rows`) packs only that sub-range, so building a state per sub-range does not repeat whole-block
    /// work; the other states pack the whole block. Every state counts the rows of `rows`, the whole block by
    /// default, to decide whether to keep `entry_hashes`.
    HashMethodKeysAndPackedString(
        const ColumnRawPtrs & key_columns, const Sizes & key_sizes, const HashMethodContextPtr & /*context*/, RowRange rows = {})
    {
        const size_t block_rows = key_columns[0]->size();
        const size_t rows_end = std::min(rows.end, block_rows);
        const bool sub_range = for_sub_range && (rows.begin != 0 || rows_end < block_rows);

        bool has_low_cardinality_fixed_key = false;
        for (size_t i = 0; i < key_columns.size(); ++i)
        {
            const IColumn * column = key_columns[i];
            const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column);
            if (key_sizes[i] == 0)
            {
                if (low_cardinality)
                {
                    low_cardinality_strings = low_cardinality;
                    column = low_cardinality->getDictionary().getNestedNotNullableColumn().get();
                    if (column->size() <= rows_end - rows.begin)
                        entry_hashes.resize_fill(column->size(), 0);
                }
                const auto & column_string = assert_cast<const ColumnString &>(*column);
                string_offsets = column_string.getOffsets().data();
                string_chars = column_string.getChars().data();
            }
            else
            {
                has_low_cardinality_fixed_key |= low_cardinality != nullptr;
                fixed_columns.push_back(column);
                fixed_sizes.push_back(key_sizes[i]);
            }
        }
        chassert(fixed_columns.size() + 1 == key_columns.size());

        if (has_low_cardinality_fixed_key && !sub_range)
            materializeLowCardinalityColumns();

        if (has_low_cardinality_fixed_key && sub_range)
        {
            packCopyOfSubRange(rows.begin, rows_end);
        }
        else if (fixed_columns.size() == 1 && fixed_sizes[0] == sizeof(Keys))
        {
            column_keys = reinterpret_cast<const Keys *>(
                static_cast<const ColumnFixedSizeHelper *>(fixed_columns[0])->getRawDataBegin<sizeof(Keys)>());
        }
        else if (!sub_range)
        {
            packFixedBatch(fixed_columns.size(), fixed_columns, fixed_sizes, prepared_keys);
        }
        else if (rows_end > rows.begin && rows_end - rows.begin >= min_rows_to_batch_pack_sub_range)
        {
            prepared_keys_begin = rows.begin;
            packFixedBatchRange(fixed_columns.size(), fixed_columns, fixed_sizes, prepared_keys, rows.begin, rows_end);
        }
    }

    /// Returns the packed fixed-width values of the row and its string, in the padded buffers of the `String`
    /// column or of the dictionary, without hashing or persisting the key. The view remains valid for this
    /// block and permits reads up to 15 bytes past its end.
    ALWAYS_INLINE std::pair<Keys, std::string_view> getKeyParts(ssize_t row) const
    {
        return {getFixedKeys(row), stringAt(stringPositionOf(row))};
    }

    ALWAYS_INLINE ArenaKeysAndPackedStringHolder<Keys> getKeyHolder(ssize_t row, Arena & pool) const
    {
        const Keys keys = getFixedKeys(row);
        const ssize_t position = stringPositionOf(row);
        const std::string_view str = stringAt(position);
        if (!entry_hashes.empty())
        {
            /// The content hash of the entry, computed for the first row with it, continues with the
            /// fixed-width values as `Hash` continues the hash of a string, so the key is the one a `String`
            /// column builds. As with `Hash`, `build` calls it only for a string that stores a hash, not for
            /// an empty one.
            const auto hash_of_entry = [this, keys, position](const char * data, size_t size)
            {
                UInt32 & content_hash = entry_hashes[position];
                if (content_hash == 0)
                    content_hash = PackedStringHash{}(data, size);
                return Hash::combine(keys, content_hash);
            };
            return {KeysAndPackedString<Keys>::build(keys, str.data(), str.size(), hash_of_entry), pool};
        }
        return {KeysAndPackedString<Keys>::build(keys, str.data(), str.size(), Hash{keys}), pool};
    }

    /// Puts the key columns in the order of the key's bytes: the fixed-width values as `packFixedBatch` packs them,
    /// and the `String` column, which has no value size, last.
    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        std::vector<size_t> order = packedFixedKeysOrder(key_sizes);
        order.push_back(std::ranges::find(key_sizes, 0) - key_sizes.begin());
        chassert(order.size() == key_columns.size());

        std::vector<IColumn *> new_columns;
        new_columns.reserve(key_columns.size());
        Sizes new_sizes;
        new_sizes.reserve(key_sizes.size());
        for (const size_t i : order)
        {
            new_columns.push_back(key_columns[i]);
            new_sizes.push_back(key_sizes[i]);
        }

        key_columns.swap(new_columns);
        return new_sizes;
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false, false, false>;

private:
    /// The position of the string of the row in the buffers that `stringAt` reads: the row itself for a
    /// `String` key column, its dictionary entry for a `LowCardinality(String)` one.
    ALWAYS_INLINE ssize_t stringPositionOf(ssize_t row) const
    {
        if (low_cardinality_strings)
            return static_cast<ssize_t>(low_cardinality_strings->getIndexAt(static_cast<size_t>(row)));
        return row;
    }

    /// The string of row `i` of the `String` column, or of entry `i` of the dictionary, in its padded
    /// buffers.
    ALWAYS_INLINE std::string_view stringAt(ssize_t i) const
    {
        const char * data = reinterpret_cast<const char *>(string_chars + string_offsets[i - 1]);
        return {data, string_offsets[i] - string_offsets[i - 1]};
    }

    /// Replaces the `LowCardinality` fixed-width key columns with copies materialized into
    /// `materialized_columns`, as the packing reads the values in place.
    void materializeLowCardinalityColumns()
    {
        for (const IColumn *& column : fixed_columns)
        {
            if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column))
            {
                materialized_columns.push_back(low_cardinality->convertToFullColumn());
                column = materialized_columns.back().get();
            }
        }
    }

    /// Packs the fixed-width values of rows [begin, end) into `prepared_keys`, for a state of a sub-range
    /// with a `LowCardinality` fixed-width key. It packs a copy of these rows only, with the
    /// `LowCardinality` columns materialized, as the packing reads the values in place.
    void packCopyOfSubRange(size_t begin, size_t end)
    {
        Columns copies;
        ColumnRawPtrs copied_columns;
        for (const IColumn * column : fixed_columns)
        {
            copies.push_back(column->cut(begin, end - begin)->convertToFullColumnIfLowCardinality());
            copied_columns.push_back(copies.back().get());
        }
        prepared_keys_begin = begin;
        packFixedBatch(copied_columns.size(), copied_columns, fixed_sizes, prepared_keys);
    }

    /// The packed fixed-width values of the row.
    ALWAYS_INLINE Keys getFixedKeys(ssize_t row) const
    {
        if (column_keys)
            return column_keys[row];
        if constexpr (for_sub_range)
        {
            if (prepared_keys.empty())
                return packFixedKeys(row);
            return prepared_keys[row - prepared_keys_begin];
        }
        else
            return prepared_keys[row];
    }

    /// Packs the fixed-width values of a row of a short sub-range, which has no prepared values. Out of line,
    /// so the accessors stay small enough to inline into the per-row loops.
    NO_INLINE Keys packFixedKeys(ssize_t row) const
    {
        return packFixedLongestFirst<Keys>(row, fixed_columns.size(), fixed_columns, fixed_sizes);
    }
};

template <typename Value, typename Mapped, typename Keys>
struct SubRangeStateOf<HashMethodKeysAndPackedString<Value, Mapped, Keys, false>>
{
    using Type = HashMethodKeysAndPackedString<Value, Mapped, Keys, true>;
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

    HashMethodFixedString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &, RowRange /*rows*/ = {}) : Base(key_columns[0])
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

    /// Returns the row's bytes in the padded source column, without persisting the key. The view remains
    /// valid for this block and permits reads up to 15 bytes past its end.
    std::string_view getKeyBytes(size_t row) const
    {
        return {reinterpret_cast<const char *>(&(*chars)[row * n]), n};
    }

    auto getKeyHolder(size_t row, [[maybe_unused]] Arena & pool) const
    {
        const auto key = getKeyBytes(row);
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
    bool need_offset = false,
    /// Built over a sub-range of the block (see `SubRangeState`), so it packs only the rows of the range.
    bool for_sub_range = false>
struct HashMethodKeysFixed
    : private columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>
    , public columns_hashing_impl::HashMethodBase<HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, has_low_cardinality_, use_cache, need_offset, for_sub_range>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, has_low_cardinality_, use_cache, need_offset, for_sub_range>;
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

    /// Set when this state covers only a sub-range of the block. `prepared_keys` then holds that
    /// sub-range, starting at `prepared_keys_begin`, or is empty; a row it does not hold is packed on
    /// its own.
    bool covers_sub_range = false;
    size_t prepared_keys_begin = 0;

    static bool usePreparedKeys(const Sizes & key_sizes)
    {
        if (has_low_cardinality || has_nullable_keys || sizeof(Key) > 16)
            return false;

        for (auto size : key_sizes)
            if (size != 1 && size != 2 && size != 4 && size != 8 && size != 16)
                return false;

        return true;
    }

    /// Nullable keys are batch-packed in clause order, the layout of the per-row nullable `packFixed`, so
    /// `packedKeysOrder` stays empty for them.
    static bool useOrderedPreparedKeys(const Sizes & key_sizes)
    {
        if constexpr (!has_nullable_keys || has_low_cardinality)
            return false;
        else
        {
            size_t packed_size = std::tuple_size_v<KeysNullMap<Key>>;
            for (auto size : key_sizes)
            {
                if (size != 1 && size != 2 && size != 4 && size != 8 && size != 16)
                    return false;
                packed_size += size;
            }
            return packed_size <= sizeof(Key);
        }
    }

    /// The batch buffer is resized before probing, even when every input key is already present.
    /// Match the padding and capacity rounding of `PaddedPODArray::resize_fill` on an empty array.
    static size_t estimatePreparedKeysMemory(size_t num_rows, const Sizes & key_sizes)
    {
        if (!num_rows || !(usePreparedKeys(key_sizes) || useOrderedPreparedKeys(key_sizes)))
            return 0;

        using Array = PaddedPODArray<Key>;
        return roundUpToPowerOfTwoOrZero(PODArrayDetails::minimum_memory_for_elements(
            num_rows, sizeof(Key), Array::pad_left, Array::pad_right));
    }

    HashMethodKeysFixed(const ColumnRawPtrs & key_columns, const Sizes & key_sizes_, const HashMethodContextPtr &, RowRange rows = {})
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
            const RowRange packed = rowsToBatchPack(key_columns, rows);
            if (!covers_sub_range)
                packFixedBatch(keys_size, Base::getActualColumns(), key_sizes, prepared_keys);
            else if (packed.end > packed.begin)
                packFixedBatchRange(keys_size, Base::getActualColumns(), key_sizes, prepared_keys, packed.begin, packed.end);
        }
        else if constexpr (has_nullable_keys && !has_low_cardinality)
        {
            if (useOrderedPreparedKeys(key_sizes))
            {
                const RowRange packed = rowsToBatchPack(key_columns, rows);
                if (packed.end > packed.begin)
                {
                    packFixedBatchOrdered<Key>(
                        keys_size, Base::getActualColumns(), key_sizes, std::tuple_size_v<KeysNullMap<Key>>, prepared_keys,
                        packed.begin, packed.end);
                    repackRowsWithNulls(packed.begin, packed.end);
                }
            }
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
            if constexpr (for_sub_range)
            {
                if (covers_sub_range)
                {
                    const size_t index = row - prepared_keys_begin;
                    if (index < prepared_keys.size())
                        return prepared_keys[index];
                    return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes, Base::createBitmap(row));
                }
            }

            if (!prepared_keys.empty())
                return prepared_keys[row];

            auto bitmap = Base::createBitmap(row);
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes, bitmap);
        }
        else
        {
            if constexpr (has_low_cardinality)
                return packFixed<Key, true>(row, keys_size, low_cardinality_keys.nested_columns, key_sizes,
                                            &low_cardinality_keys.positions, &low_cardinality_keys.position_sizes);

            if constexpr (for_sub_range)
            {
                if (covers_sub_range)
                {
                    const size_t index = row - prepared_keys_begin;
                    if (index < prepared_keys.size())
                        return prepared_keys[index];
                    return packFixedLongestFirst<Key>(row, keys_size, Base::getActualColumns(), key_sizes);
                }
            }

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

    /// Returns the column order used to pack prepared keys: descending value size.
    /// Returns `std::nullopt` when packing uses the original column order.
    /// `unpackFixedKeyIntoColumns` uses the same order to recover key values.
    static std::optional<std::vector<size_t>> packedKeysOrder(const Sizes & key_sizes)
    {
        if (!usePreparedKeys(key_sizes))
            return {};
        return packedFixedKeysOrder(key_sizes);
    }

    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        const auto order = packedKeysOrder(key_sizes);
        if (!order)
            return {};

        std::vector<IColumn *> new_columns;
        new_columns.reserve(key_columns.size());
        Sizes new_sizes;
        new_sizes.reserve(key_sizes.size());

        for (const size_t i : *order)
        {
            new_columns.push_back(key_columns[i]);
            new_sizes.push_back(key_sizes[i]);
        }

        key_columns.swap(new_columns);
        return new_sizes;
    }

private:
    /// The rows to batch-pack: the whole block, the state's sub-range if it is long enough, or none.
    RowRange rowsToBatchPack(const ColumnRawPtrs & key_columns, RowRange rows)
    {
        const size_t block_rows = key_columns.empty() ? 0 : key_columns[0]->size();
        if constexpr (for_sub_range)
        {
            /// Batch-packing costs one pass over the rows it packs, so a state that will be asked about
            /// only a sub-range packs just that sub-range: one state per run of a sorted prefix would
            /// otherwise pay for the whole block each time. Same trade as `Params::aggregation_in_order`.
            const size_t rows_end = std::min(rows.end, block_rows);
            if (rows.begin != 0 || rows_end < block_rows)
            {
                covers_sub_range = true;
                if (rows_end > rows.begin && rows_end - rows.begin >= min_rows_to_batch_pack_sub_range)
                {
                    prepared_keys_begin = rows.begin;
                    return {rows.begin, rows_end};
                }
                return {0, 0};
            }
        }
        return {0, block_rows};
    }

    /// A row whose null-map bytes are all zero gets no bit from `createBitmap`, so only the other rows of
    /// the batch-packed range need packing again.
    void repackRowsWithNulls(size_t begin, size_t end)
    {
        std::array<const UInt8 *, sizeof(Key)> maps_with_nulls{};
        size_t num_maps_with_nulls = 0;
        for (const auto * null_map_column : Base::getNullMaps())
        {
            if (!null_map_column)
                continue;
            const UInt8 * null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData().data();
            if (!memoryIsZero(null_map, begin, end))
            {
                chassert(num_maps_with_nulls < maps_with_nulls.size());
                maps_with_nulls[num_maps_with_nulls++] = null_map;
            }
        }

        if (num_maps_with_nulls == 0)
            return;

        for (size_t row = begin; row < end; ++row)
        {
            UInt8 any_null = 0;
            for (size_t k = 0; k < num_maps_with_nulls; ++k)
                any_null |= maps_with_nulls[k][row];
            if (any_null)
                prepared_keys[row - begin] = packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes, Base::createBitmap(row));
        }
    }
};

/// Only a state that can batch-pack its keys has a sub-range variant; the others would be identical copies.
template <typename Value, typename Key, typename Mapped, bool has_nullable_keys, bool has_low_cardinality, bool use_cache, bool need_offset>
struct SubRangeStateOf<HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys, has_low_cardinality, use_cache, need_offset, false>>
{
    using Type = HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys, has_low_cardinality, use_cache, need_offset,
        !has_low_cardinality && (has_nullable_keys || sizeof(Key) <= 16)>;
};

/// Bitwise comparator of rows over fixed-width contiguous key columns, flattening tuples
/// element-wise. Bitwise equality of all collected slices implies key equality; the converse
/// does not hold (e.g. `-0.` and `0.` are equal values with different bytes), but a false
/// negative only costs falling back to the full key calculation. Unusable (and empty) if some
/// key column has no such raw representation.
class FixedSizeKeySlices
{
public:
    FixedSizeKeySlices() = default;

    explicit FixedSizeKeySlices(const ColumnRawPtrs & key_columns)
    {
        for (const auto * column : key_columns)
        {
            if (!collect(*column))
            {
                slices.clear();
                return;
            }
        }
        usable = true;
    }

    bool isUsable() const { return usable; }

    /// The comparator must be usable and both rows must be valid.
    ALWAYS_INLINE bool rowsEqual(size_t row, size_t other_row) const
    {
        for (const auto & [data, size] : slices)
        {
            if (memcmp(data + row * size, data + other_row * size, size) != 0)
                return false;
        }
        return true;
    }

private:
    bool collect(const IColumn & column)
    {
        if (const auto * tuple = typeid_cast<const ColumnTuple *>(&column))
        {
            for (size_t i = 0; i < tuple->tupleSize(); ++i)
            {
                if (!collect(tuple->getColumn(i)))
                    return false;
            }
            return true;
        }
        if (column.valuesHaveFixedSize() && column.isFixedAndContiguous())
        {
            slices.emplace_back(column.getRawData().data(), column.sizeOfValueIfFixed());
            return true;
        }
        return false;
    }

    std::vector<std::pair<const char *, size_t>> slices;
    bool usable = false;
};

/// For the case when the key is a 128-bit hash of all key columns (the fallback method for
/// keys with no fixed-width packed representation, e.g. a `Tuple` column).
/// With `prepare_keys`, the keys of the next rows are hashed together before their lookups; a state's rows are read in ascending order.
template <typename Value, typename Mapped, bool use_cache = true, bool need_offset = false, bool prepare_keys = false>
struct HashMethodHashed
    : public columns_hashing_impl::HashMethodBase<HashMethodHashed<Value, Mapped, use_cache, need_offset, prepare_keys>, Value, Mapped, use_cache, need_offset>
{
    using Key = UInt128;
    using Self = HashMethodHashed<Value, Mapped, use_cache, need_offset, prepare_keys>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

    static constexpr bool has_cheap_key_calculation = false;
    /// `hash128` SipHashes every key column through a virtual `IColumn::updateHashWithValue`.
    static constexpr bool has_cheap_key_holder = false;
    static constexpr bool has_pre_computed_hashes = false;

    ColumnRawPtrs key_columns;

    /// The consecutive-keys cache alone cannot skip the key calculation for this method: its
    /// check needs the key, and here the key is the hash itself. Clustered inputs (e.g. sorted
    /// by a primary key prefix) arrive in runs of equal consecutive rows, so additionally
    /// compare the raw key bytes with the last processed row and reuse the cached result on
    /// equality, skipping the hashing entirely.
    FixedSizeKeySlices key_slices;
    static constexpr size_t no_last_row = std::numeric_limits<size_t>::max();
    size_t last_row = no_last_row;

    struct PreparedKeys /// NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - only keys[0, size) is read
    {
        static constexpr size_t min_rows = 8;
        static constexpr size_t max_rows = 256;
        std::array<UInt128, max_rows> keys;
        size_t begin = 0;
        size_t size = 0;
        size_t next_rows = min_rows;
        size_t rows_end = 0;
        bool enabled = false;
    };
    struct NoPreparedKeys {};
    [[no_unique_address]] std::conditional_t<prepare_keys, PreparedKeys, NoPreparedKeys> prepared;

    HashMethodHashed(ColumnRawPtrs key_columns_, const Sizes &, const HashMethodContextPtr &, [[maybe_unused]] RowRange rows = {})
        : key_columns(std::move(key_columns_))
    {
        if constexpr (use_cache)
            key_slices = FixedSizeKeySlices(key_columns);

        if constexpr (prepare_keys)
        {
            prepared.rows_end = std::min(rows.end, key_columns.empty() ? 0 : key_columns[0]->size());
            prepared.enabled = !rows.skips_rows && prepared.rows_end >= rows.begin + PreparedKeys::min_rows;
        }
    }

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &) const requires (!prepare_keys)
    {
        return hash128(row, key_columns.size(), key_columns);
    }

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &) requires (prepare_keys)
    {
        if (!isPrepared(row))
        {
            if (!prepared.enabled)
                return hash128(row, key_columns.size(), key_columns);
            prepareKeys(row);
        }
        return prepared.keys[row - prepared.begin];
    }

    /// A prepared key is compared with the cached one by `Base`.
    template <typename Data>
    ALWAYS_INLINE typename Base::EmplaceResult emplaceKey(Data & data, size_t row, Arena & pool)
    {
        if constexpr (use_cache)
        {
            /// An emplace can reuse the cache only when the key is known to be in the table.
            if (!isPrepared(row) && last_row != no_last_row && this->cache.found && key_slices.rowsEqual(row, last_row))
                return Base::getCachedEmplaceResult();
            if (key_slices.isUsable())
                last_row = row;
        }
        return Base::emplaceKey(data, row, pool);
    }

    template <typename Data>
    ALWAYS_INLINE typename Base::FindResult findKey(Data & data, size_t row, Arena & pool)
    {
        if constexpr (use_cache)
        {
            if (!isPrepared(row) && last_row != no_last_row && !this->cache.empty && key_slices.rowsEqual(row, last_row))
                return Base::getCachedFindResult();
            if (key_slices.isUsable())
                last_row = row;
        }
        return Base::findKey(data, row, pool);
    }

private:
    ALWAYS_INLINE bool isPrepared(size_t row) const
    {
        if constexpr (prepare_keys)
            return row - prepared.begin < prepared.size;
        else
            return false;
    }

    NO_INLINE void prepareKeys(size_t row) requires (prepare_keys)
    {
        const size_t end = std::min(prepared.rows_end, row + prepared.next_rows);
        prepared.next_rows = std::min(PreparedKeys::max_rows, 2 * prepared.next_rows);
        prepared.begin = row;
        prepared.size = end > row ? end - row : 1;
        prepared.keys[0] = hash128(row, key_columns.size(), key_columns);
        for (size_t i = 1; i < prepared.size; ++i)
        {
            if (key_slices.isUsable() && key_slices.rowsEqual(row + i, row + i - 1))
                prepared.keys[i] = prepared.keys[i - 1];
            else
                prepared.keys[i] = hash128(row + i, key_columns.size(), key_columns);
        }
    }
};

}
