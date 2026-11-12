#pragma once
#include <Interpreters/HashJoin/HashJoin.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/Prefetching.h>
#include <Common/SipHash.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/canonicalizeNegativeZero.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <base/PackedStringRef.h>

#include <algorithm>
#include <bit>
#include <concepts>
#include <span>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <HashJoin::Type type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

constexpr bool use_offset = true;

/// Key getter for a single LowCardinality column, tailored to HashJoin. Unlike the aggregation
/// method `HashMethodSingleLowCardinalityColumn`, this one is const-correct on the probe side
/// (probe maps expose `const RowRef`/`const RowRefList` and `ConstLookupResult`), produces an
/// offset-carrying `FindResult` (HashJoin indexes `JoinUsedFlags` by it), and has no null-key
/// path (chooseMethod only routes here for non-nullable dictionaries). It wraps a base method that
/// operates on the dictionary's nested column to produce key holders, and deduplicates the
/// hash-table work per dictionary index within a block — that dedup is the whole point. The
/// probe/left key may be a plain (non-LowCardinality) column even when this map is chosen (joins
/// allow plain T vs LowCardinality(T)); such a column is handled by running the base method on it
/// directly, with no dictionary indirection or deduplication.
template <typename BaseMethod, typename Mapped>
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
            if constexpr (has_mapped)
                return FindResult(mapped_cache[row], visit_cache[row] == 1, offset_cache[row]);
            else
                return FindResult(visit_cache[row] == 1, offset_cache[row]);
        }

        auto key_holder = base.getKeyHolder(row, pool);
        const auto key = keyHolderGetKey(key_holder);

        auto it = row < saved_hash.size() ? data.find(key, saved_hash[row]) : data.find(key);

        const bool found = it;
        const size_t offset = found ? data.offsetInternal(it) : 0;

        visit_cache[row] = found ? 1 : 2;
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

/// A base of the join key getters that can build the keys of many rows in one call. `Derived` is the key getter. It has:
/// - `UInt128 buildKey(size_t row) const`, the key of row `row`;
/// - `void buildKeys(size_t begin, size_t end, UInt128 * keys, const UInt8 * skip)`, which sets `keys[i - begin]` to the
///   key of row `i` for the rows [begin, end). `skip` is the skip bytes of the block: the keys of the rows `i` with
///   `skip[i] != 0` may be left unset.
/// `getKeyHolder` builds the key of a row, or reads keys built ahead:
/// - after `buildAllKeys` or `usePrebuiltKeys`, the keys of the rows that the slot scatter of a parallel build routed.
///   The scatter calls `buildAllKeys`, and the slot inserts pass the keys it built to `usePrebuiltKeys`;
/// - after `prepareRows`, the keys of a continuous range of rows, built 256 rows at a time when a read goes past the
///   built ones. A range that starts after the first row is the rest of a block that an earlier call left because of
///   `max_joined_block_rows`. The call for the rest can stop early too, so its chunks start at 8 rows and double up to
///   256. Each join loop calls it for its first map. A prefetching loop also calls it for every other map that it
///   prefetches for each row: then each key is built once for both the prefetch and the lookup. A loop that stops early
///   at `max_joined_block_rows` pays only for the chunks it reached.
///   The loop of an `OR` join with a condition in `ON` calls it for every map. For a list of rows in ascending order, it
///   builds the keys of the listed rows at once.
template <typename Derived>
struct PreparedJoinKeys
{
    /// The chunk size of `SipHashStrings`, so that a call usually hashes one whole chunk there.
    static constexpr size_t max_chunk_rows = 256;
    /// The rows that `SipHashStrings` hashes as one group.
    static constexpr size_t min_chunk_rows = 8;

    /// Keys built ahead of the lookups, those of the rows [ready_begin, ready_begin + ready_keys.size()). While
    /// `ready_keys` is empty, each key is built where it is read.
    size_t ready_begin = 0;
    std::span<const UInt128> ready_keys;
    /// After `prepareRows`: the buffer that `ready_keys` views, the end of the rows to prepare, the rows of the next chunk,
    /// and the skip bytes.
    PaddedPODArray<UInt128> prepared_keys;
    size_t prepare_end = 0;
    size_t next_chunk_rows = max_chunk_rows;
    const UInt8 * skip_rows = nullptr;
    /// A caller can build the skip bytes for `prepareRows` here. They must stay valid while the key getter reads keys.
    IColumn::Filter skip_buffer;
    /// After `prepareAscendingRows`: the skip bytes of the rows that are not listed, or that `skip` marks.
    IColumn::Filter unlisted_skip;
    /// See `skipOrAllSelected`.
    IColumn::Filter all_selected;
    /// The rows of the block.
    size_t num_rows;

    explicit PreparedJoinKeys(size_t num_rows_)
        : num_rows(num_rows_)
    {
    }

    /// Returns `skip`, or if it is nullptr, skip bytes that mark no row of the block.
    const UInt8 * skipOrAllSelected(const UInt8 * skip)
    {
        if (skip)
            return skip;
        all_selected.resize_fill(num_rows, 0);
        return all_selected.data();
    }

    /// Returns skip bytes for the rows [0, end): 1 for each row that `rows` does not list, and `skip` for each listed row.
    const UInt8 * skipUnlisted(const PaddedPODArray<UInt64> & rows, size_t end, const UInt8 * skip)
    {
        unlisted_skip.assign(end, UInt8(1));
        for (const auto row : rows)
            unlisted_skip[row] = skip[row];
        return unlisted_skip.data();
    }

    /// Builds the key of every row that `selector` lists into `keys[row]`, and reads the keys from there. The other rows
    /// of the block get no key.
    void buildAllKeys(const ScatteredBlock::Selector & selector, PaddedPODArray<UInt128> & keys)
    {
        keys.resize(num_rows);
        /// The scatter routes every listed row by its key, including the rows that the slot inserts skip.
        const UInt8 * skip = skipOrAllSelected(nullptr);
        size_t begin = 0;
        size_t end = num_rows;
        if (selector.isContinuousRange())
            std::tie(begin, end) = selector.getRange();
        else
            skip = skipUnlisted(selector.getIndexes().getData(), num_rows, skip);
        derived().buildKeys(begin, end, keys.data() + begin, skip);
        ready_begin = 0;
        ready_keys = {keys.data(), keys.size()};
    }

    /// `keys[row]` is the key of each row that `buildAllKeys` listed. The key of `check_row`, a listed row, is built again
    /// to check that they are the keys of this column.
    void usePrebuiltKeys(std::span<const UInt128> keys, size_t check_row)
    {
        if (check_row >= keys.size() || keys[check_row] != derived().buildKey(check_row))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The join key built for row {} is not the key of that row", check_row);
        ready_begin = 0;
        ready_keys = keys;
    }

    /// Prepares the keys of the rows of `selector` if they are a continuous range, or if they are in ascending order;
    /// otherwise each key is built where it is read. It builds the first chunk here, so that a prefetcher created after
    /// this call times only the lookups when it calibrates its look-ahead. `skip` is the skip bytes of the block, as in
    /// `buildKeys`. The caller must not read the key of a row that `skip` marks, not even to prefetch it.
    template <typename Selector>
    void prepareRows(const Selector & selector, const UInt8 * skip)
    {
        if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Selector>)
        {
            if (selector.isContinuousRange())
                prepareRange(selector.getRange(), skip);
            else
                prepareRows(selector.getIndexes(), skip);
        }
        else if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Selector::Range>)
            prepareRange(selector, skip);
        else if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Selector::Indexes>)
            prepareAscendingRows(selector.getData(), skip);
    }

    void prepareAscendingRows(const PaddedPODArray<UInt64> & rows, const UInt8 * skip)
    {
        if (rows.empty() || !std::is_sorted(rows.begin(), rows.end()))
            return;
        ready_begin = rows.front();
        prepare_end = rows.back() + 1;
        skip_rows = skipUnlisted(rows, prepare_end, skip);
        /// All at once, not in chunks: the rows between the listed ones are left out, so the look-ahead of a
        /// prefetching loop can span more rows than `buildNextKeys` keeps.
        prepared_keys.resize(prepare_end - ready_begin);
        derived().buildKeys(ready_begin, prepare_end, prepared_keys.data(), skip_rows);
        ready_keys = {prepared_keys.data(), prepared_keys.size()};
    }

    /// Drops the keys prepared before. With an empty range, each key is built where it is read again.
    void prepareRange(ScatteredBlock::Selector::Range range, const UInt8 * skip)
    {
        ready_begin = range.first;
        prepare_end = range.second;
        next_chunk_rows = range.first == 0 ? max_chunk_rows : min_chunk_rows;
        skip_rows = skip;
        prepared_keys.clear();
        ready_keys = {};
        if (range.first < range.second)
            buildNextKeys(range.first);
    }

    /// Builds the next chunk of the prepared keys, which ends after `row`. It runs once per chunk, so it is not inlined
    /// into the join loops that call `getKeyHolder`.
    NO_INLINE void buildNextKeys(size_t row)
    {
        const size_t begin = ready_begin + prepared_keys.size();
        if (row < begin || row >= prepare_end)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The join key of row {} was not prepared", row);

        /// A join loop reads the rows in order, except that its lookups trail its prefetch reads by at most the look-ahead,
        /// see `JoinPrefetcher`. So the buffer keeps only the last `kept_rows` keys built before.
        static constexpr size_t kept_rows = PrefetchingHelper::getMaxLookAheadValue();
        if (prepared_keys.size() > kept_rows)
        {
            const size_t dropped = prepared_keys.size() - kept_rows;
            memmove(prepared_keys.data(), prepared_keys.data() + dropped, kept_rows * sizeof(UInt128));
            prepared_keys.resize(kept_rows);
            ready_begin += dropped;
        }

        /// After a run of `next_chunk_rows` or more skipped rows, the chunk grows to cover the whole run, up to `row`.
        const size_t end = std::min(prepare_end, std::max(row + 1, begin + next_chunk_rows));
        next_chunk_rows = std::min(max_chunk_rows, 2 * next_chunk_rows);
        const size_t old_size = prepared_keys.size();
        prepared_keys.resize(old_size + (end - begin));
        derived().buildKeys(begin, end, prepared_keys.data() + old_size, skip_rows);
        ready_keys = {prepared_keys.data(), prepared_keys.size()};
    }

    ALWAYS_INLINE UInt128 getKeyHolder(size_t row, Arena &)
    {
        /// `prepareRows` does not build the keys of skipped rows.
        chassert(!skip_rows || !skip_rows[row]);
        if (ready_keys.empty())
            return derived().buildKey(row);

        if (row - ready_begin >= ready_keys.size()) [[unlikely]]
            buildNextKeys(row);
        return ready_keys[row - ready_begin];
    }

private:
    Derived & derived() { return static_cast<Derived &>(*this); }
};

template <typename KeyGetter>
concept PreparedKeyGetter = std::derived_from<KeyGetter, PreparedJoinKeys<KeyGetter>>;

/// The key of a row in the `hashed` and `two_level_hashed` maps: `ColumnsHashing::hash128` of its key columns. The build
/// and the probe of one map can see different column types for one key, for example `Tuple(LowCardinality(String))` and
/// `Tuple(String)`, so every way of building a key must give exactly `hash128`.
class HashedJoinKey
{
public:
    explicit HashedJoinKey(const ColumnRawPtrs & key_columns_);

    UInt128 build(size_t row) const { return ColumnsHashing::hash128(row, key_columns.size(), key_columns); }

    /// Sets `keys[i - begin]` to `build(i)` for the rows [begin, end). The keys of the rows `i` with `skip[i] != 0` may be
    /// left unset.
    void buildRange(size_t begin, size_t end, UInt128 * __restrict keys, const UInt8 * skip);

private:
    enum class Layout : uint8_t
    {
        /// One fixed-width column, hashed where it is.
        Fixed,
        /// Several fixed-width columns, copied into rows of `fixed_size` bytes.
        FixedRows,
        /// The bytes that `hash128` hashes for each row, copied into a buffer laid out like the chars of a `ColumnString`.
        Rows,
        /// `build` for each row.
        PerRow,
    };

    /// A column whose `updateHashWithValue` hashes `size` bytes at `data + row * size`, or, when `offsets` is not
    /// nullptr, a `String` column with these chars and offsets. With `null_map`, it is the nested column of a `Nullable`.
    struct Part
    {
        const UInt8 * data = nullptr;
        size_t size = 0;
        const IColumn::Offset * offsets = nullptr;
        const UInt8 * null_map = nullptr;
    };

    /// Appends the parts of `column`. Returns false if `buildRange` cannot copy the bytes that `hash128` hashes for it.
    static bool addParts(const IColumn & column, std::vector<Part> & parts);
    /// These set `keys[i]` to `build(row_at(i))` for `i` in [0, num_rows), for at most 256 rows.
    template <typename RowAt>
    void buildFixedRows(RowAt row_at, size_t num_rows, UInt128 * __restrict keys);
    template <typename RowAt>
    void buildRows(RowAt row_at, size_t num_rows, UInt128 * __restrict keys);

    ColumnRawPtrs key_columns;
    Layout layout = Layout::PerRow;
    std::vector<Part> parts;
    /// The total size of the fixed-width parts.
    size_t fixed_size = 0;
    /// The rows that `buildFixedRows` and `buildRows` copy. The kernel reads up to 8 bytes before a row shorter than 8
    /// bytes and up to 7 bytes past a row, and the padding of a `PaddedPODArray` allows that.
    PaddedPODArray<UInt8> buffer;
};

/// Key getter of `hashed` and `two_level_hashed`. It builds the `HashedJoinKey` of a row, or reads keys built ahead, see
/// `PreparedJoinKeys`.
template <typename Value, typename Mapped>
struct HashMethodHashedJoinKey : public ColumnsHashing::columns_hashing_impl::HashMethodBase<
                                     HashMethodHashedJoinKey<Value, Mapped>,
                                     Value,
                                     Mapped,
                                     /*consecutive_keys_optimization=*/false,
                                     use_offset>,
                                 public PreparedJoinKeys<HashMethodHashedJoinKey<Value, Mapped>>
{
    /// Hashing a key is not cheap, but `true` lets the join loops prefetch: after `prepareRows`, they build each key
    /// once.
    static constexpr bool has_cheap_key_calculation = true;

    /// Owns the key columns with negative zeros replaced by positive zeros, if there were any.
    Columns canonicalized_columns;
    HashedJoinKey key;

    /// The keys are hashed by their binary representation, so negative zeros are canonicalized in advance,
    /// unless `rows.negative_zeros_canonicalized` says the caller has done it.
    HashMethodHashedJoinKey(
        const ColumnRawPtrs & key_columns,
        const Sizes &,
        const ColumnsHashing::HashMethodContextPtr &,
        ColumnsHashing::RowRange rows = {})
        : PreparedJoinKeys<HashMethodHashedJoinKey>(key_columns.at(0)->size())
        , key(canonicalizeKeyColumns(key_columns, rows.negative_zeros_canonicalized, canonicalized_columns))
    {
    }

    ALWAYS_INLINE UInt128 buildKey(size_t row) const { return key.build(row); }

    void buildKeys(size_t begin, size_t end, UInt128 * keys, const UInt8 * skip) { key.buildRange(begin, end, keys, skip); }

private:
    static ColumnRawPtrs canonicalizeKeyColumns(ColumnRawPtrs key_columns, bool negative_zeros_canonicalized, Columns & holder)
    {
        if (!negative_zeros_canonicalized)
            canonicalizeNegativeZeroInKeyColumns(key_columns, holder);
        return key_columns;
    }
};

/// The 16-byte key of a single `String` join key in the `key_packed_string` and `two_level_key_packed_string` maps.
///
/// - Up to 15 bytes: the `UnhashedPackedStringRef` of the string. It is exact: byte 15 holds the length, and the bytes
///   past the end are zero. The empty string is the zero key.
/// - 16 bytes or more: the unkeyed `sipHash128Padded` of the string with bit 7 of byte 15 set. No short key has that
///   bit set, so a hashed key never equals a short one. Two long strings whose other 127 hash bits match share a key.
///
/// `build` and `buildRange` read up to 16 bytes from the start of a string; the chars of a `ColumnString` have that
/// padding.
struct PackedStringJoinKey
{
    static constexpr size_t max_inline_size = UnhashedPackedStringRef::MAX_SMALL_LEN;

    /// Bit 7 of byte 15, which is in `items[1]` on both byte orders.
    static constexpr UInt64 hashed_flag = UInt64(UnhashedPackedStringRef::LARGE_TAG_BYTE)
        << (std::endian::native == std::endian::little ? 56 : 0);

    static ALWAYS_INLINE UInt128 build(const char * data, size_t size)
    {
        if (size <= max_inline_size)
            return std::bit_cast<UInt128>(UnhashedPackedStringRef::buildShort(data, size));

        UInt128 key = sipHash128Padded(data, size);
        key.items[1] |= hashed_flag;
        return key;
    }

    /// `keys[i - begin]` = the key of row `i` of a `ColumnString`, for the rows [begin, end). The rows `i` with
    /// `skip[i] != 0` are not hashed, and their keys are unspecified. On aarch64 and on x86 with AVX2 it hashes long
    /// strings 8 at a time: 4 in NEON lanes and 4 in general registers, or all 8 in AVX2 lanes.
    static void buildRange(
        const char * chars, const IColumn::Offset * offsets, size_t begin, size_t end, UInt128 * __restrict keys, const UInt8 * skip);
};

/// Key getter of `key_packed_string` and `two_level_key_packed_string`. It builds the `PackedStringJoinKey` of a row,
/// or reads keys built ahead, see `PreparedJoinKeys`.
template <typename Value, typename Mapped>
struct HashMethodPackedStringJoinKey : public ColumnsHashing::columns_hashing_impl::HashMethodBase<
                                           HashMethodPackedStringJoinKey<Value, Mapped>,
                                           Value,
                                           Mapped,
                                           /*consecutive_keys_optimization=*/false,
                                           use_offset>,
                                       public PreparedJoinKeys<HashMethodPackedStringJoinKey<Value, Mapped>>
{
    using Base = ColumnsHashing::columns_hashing_impl::
        HashMethodBase<HashMethodPackedStringJoinKey<Value, Mapped>, Value, Mapped, false, use_offset>;

    /// Hashing a long string is not cheap, but `true` lets the join loops prefetch: after `prepareRows`, they build
    /// each key once.
    static constexpr bool has_cheap_key_calculation = true;

    const IColumn::Offset * offsets;
    const UInt8 * chars;

    /// The number of `emplaceKey` calls, and how many of their keys are hashed.
    size_t emplaced_keys = 0;
    size_t hashed_keys = 0;

    HashMethodPackedStringJoinKey(const ColumnRawPtrs & key_columns, const Sizes &, const ColumnsHashing::HashMethodContextPtr &)
        : HashMethodPackedStringJoinKey(assert_cast<const ColumnString &>(*key_columns[0]))
    {
    }

    explicit HashMethodPackedStringJoinKey(const ColumnString & column)
        : PreparedJoinKeys<HashMethodPackedStringJoinKey>(column.size())
        , offsets(column.getOffsets().data())
        , chars(column.getChars().data())
    {
    }

    ALWAYS_INLINE UInt128 buildKey(size_t row) const
    {
        return PackedStringJoinKey::build(
            reinterpret_cast<const char *>(chars) + offsets[ssize_t(row) - 1], offsets[row] - offsets[ssize_t(row) - 1]);
    }

    void buildKeys(size_t begin, size_t end, UInt128 * keys, const UInt8 * skip) const
    {
        PackedStringJoinKey::buildRange(reinterpret_cast<const char *>(chars), offsets, begin, end, keys, skip);
    }

    template <typename Data>
    ALWAYS_INLINE typename Base::EmplaceResult emplaceKey(Data & data, size_t row, Arena & pool)
    {
        auto result = Base::emplaceKey(data, row, pool);
        ++emplaced_keys;
        hashed_keys += (result.getKey().items[1] & PackedStringJoinKey::hashed_flag) != 0;
        return result;
    }
};

template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt32, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt64, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::hashed, Value, Mapped>
{
    using Type = HashMethodHashedJoinKey<Value, Mapped>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::low_cardinality_key_string, Value, Mapped>
{
    using Type = LowCardinalityKeyGetterForJoin<
        ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>, Mapped>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::low_cardinality_key_fixed_string, Value, Mapped>
{
    using Type = LowCardinalityKeyGetterForJoin<
        ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>, Mapped>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_keys32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt32, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_keys64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt64, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_hashed, Value, Mapped>
{
    using Type = HashMethodHashedJoinKey<Value, Mapped>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::two_level_key_packed_string, Value, Mapped>
{
    using Type = HashMethodPackedStringJoinKey<Value, Mapped>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_packed_string, Value, Mapped>
{
    using Type = HashMethodPackedStringJoinKey<Value, Mapped>;
};
#define KEYGETTER_RANGE_IMPL(TYPE, FIELD_TYPE) \
    template <typename Value, typename Mapped> \
    struct KeyGetterForTypeImpl<HashJoin::Type::TYPE, Value, Mapped> \
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

/// Set tables, which joins that never read a right-side row run on, have no mapped value in their
/// cells: they report the `VoidMapped` placeholder, while the column hashing methods spell the absence
/// of a mapped value as `void`.
template <typename Data>
struct JoinMappedType
{
    using Raw = typename std::remove_const_t<Data>::mapped_type;
    using Type = std::conditional_t<std::is_same_v<Raw, VoidMapped>, void, Raw>;
};

template <HashJoin::Type type, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename JoinMappedType<Data>::Type;
    using Mapped = std::conditional_t<std::is_const_v<Data> && !std::is_void_v<Mapped_t>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
};
}
