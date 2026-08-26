#pragma once

#include <base/demangle.h>
#include <base/getL2CacheSize.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ColumnsHashing/HashMethod.h>
#include <Common/HashTable/Prefetching.h>
#include <Common/ColumnsHashingImpl.h>
#include <Common/Arena.h>
#include <Common/CacheBase.h>
#include <Common/SipHash.h>
#include <Common/CurrentMetrics.h>
#include <Common/assert_cast.h>
#include <base/unaligned.h>

#include <Columns/ColumnLowCardinality.h>

#include <Core/Defines.h>
#include <algorithm>
#include <memory>
#include <Common/HashTable/Hash.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ColumnsHashing
{

/// Cache stores dictionaries and saved_hash per dictionary key.
class LowCardinalityDictionaryCache : public HashMethodContext
{
public:
    /// Will assume that dictionaries with same hash has the same keys.
    /// Just in case, check that they have also the same size.
    struct DictionaryKey
    {
        UInt128 hash;
        UInt64 size;

        bool operator== (const DictionaryKey & other) const { return hash == other.hash && size == other.size; }
    };

    struct DictionaryKeyHash
    {
        size_t operator()(const DictionaryKey & key) const
        {
            SipHash hash;
            hash.update(key.hash);
            hash.update(key.size);
            return hash.get64();
        }
    };

    struct CachedValues
    {
        /// Store ptr to dictionary to be sure it won't be deleted.
        ColumnPtr dictionary_holder;
        /// Hashes for dictionary keys.
        std::span<const UInt64> saved_hash;
    };

    using CachedValuesPtr = std::shared_ptr<CachedValues>;

    explicit LowCardinalityDictionaryCache(const HashMethodContextSettings & settings)
        : cache(CurrentMetrics::end(), CurrentMetrics::end(), settings.max_threads)
    {}

    CachedValuesPtr get(const DictionaryKey & key) { return cache.get(key); }
    void set(const DictionaryKey & key, const CachedValuesPtr & mapped) { cache.set(key, mapped); }

private:
    using Cache = CacheBase<DictionaryKey, CachedValues, DictionaryKeyHash>;
    Cache cache;
};

/// Single low cardinality column.
template <typename SingleColumnMethod, typename Mapped, bool use_cache>
struct HashMethodSingleLowCardinalityColumn : public SingleColumnMethod
{
    using Base = SingleColumnMethod;

    enum class VisitValue : uint8_t
    {
        Empty = 0,
        Found = 1,
        NotFound = 2,
    };

    static constexpr bool has_mapped = !std::is_same_v<Mapped, void>;
    using EmplaceResult = typename Base::EmplaceResult;
    using FindResult = columns_hashing_impl::FindResultImpl<Mapped>;

    static constexpr bool has_cheap_key_calculation = Base::has_cheap_key_calculation;
    static constexpr bool has_cheap_key_holder = Base::has_cheap_key_holder;
    static constexpr bool has_pre_computed_hashes = Base::has_pre_computed_hashes;

    static HashMethodContextPtr createContext(const HashMethodContextSettings & settings)
    {
        return std::make_shared<LowCardinalityDictionaryCache>(settings);
    }

    ColumnRawPtrs key_columns;
    const IColumn * positions = nullptr;
    size_t size_of_index_type = 0;

    /// saved hash is from current column or from cache. Dictionary positions outside it have no
    /// saved hash and are hashed from the key.
    std::span<const UInt64> saved_hash;
    /// Hold dictionary in case saved_hash is from cache to be sure it won't be deleted.
    ColumnPtr dictionary_holder;

    /// Cache AggregateDataPtr for current column in order to decrease the number of hash table usages.
    columns_hashing_impl::MappedCache<Mapped> mapped_cache;
    PaddedPODArray<VisitValue> visit_cache;

    PaddedPODArray<UInt64> filled_visit_cache_indexes;

    ALWAYS_INLINE void setVisited(size_t index, VisitValue value)
    {
        if (visit_cache[index] == VisitValue::Empty)
            filled_visit_cache_indexes.push_back(index);
        visit_cache[index] = value;
    }

    /// If initialized column is nullable.
    bool is_nullable = false;

    static const ColumnLowCardinality & getLowCardinalityColumn(const IColumn * column)
    {
        const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(column);
        if (!low_cardinality_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid aggregation key type for HashMethodSingleLowCardinalityColumn method. "
                            "Excepted LowCardinality, got {}", column->getName());
        return *low_cardinality_column;
    }

    HashMethodSingleLowCardinalityColumn(
        const ColumnRawPtrs & key_columns_low_cardinality, const Sizes & key_sizes, const HashMethodContextPtr & context)
        : Base({getLowCardinalityColumn(key_columns_low_cardinality[0]).getDictionary().getNestedNotNullableColumn().get()}, key_sizes, context)
    {
        const auto * column = &getLowCardinalityColumn(key_columns_low_cardinality[0]);

        if (!context)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache wasn't created for HashMethodSingleLowCardinalityColumn");

        LowCardinalityDictionaryCache * lcd_cache = nullptr;
        if constexpr (use_cache)
        {
            lcd_cache = typeid_cast<LowCardinalityDictionaryCache *>(context.get());
            if (!lcd_cache)
            {
                const auto & cached_val = *context;
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid type for HashMethodSingleLowCardinalityColumn cache: {}",
                                demangle(typeid(cached_val).name()));
            }
        }

        const auto * dict = column->getDictionary().getNestedNotNullableColumn().get();
        is_nullable = column->getDictionary().nestedColumnIsNullable();
        key_columns = {dict};
        const bool is_shared_dict = column->isSharedDictionary();

        typename LowCardinalityDictionaryCache::DictionaryKey dictionary_key{};
        typename LowCardinalityDictionaryCache::CachedValuesPtr cached_values;

        if (is_shared_dict)
        {
            dictionary_key = {column->getDictionary().getHash(), dict->size()};
            if constexpr (use_cache)
                cached_values = lcd_cache->get(dictionary_key);
        }

        if (cached_values)
        {
            saved_hash = cached_values->saved_hash;
            dictionary_holder = cached_values->dictionary_holder;
        }
        else
        {
            saved_hash = column->getDictionary().tryGetSavedHash();
            dictionary_holder = column->getDictionaryPtr();

            if constexpr (use_cache)
            {
                if (is_shared_dict)
                {
                    cached_values = std::make_shared<typename LowCardinalityDictionaryCache::CachedValues>();
                    cached_values->saved_hash = saved_hash;
                    cached_values->dictionary_holder = dictionary_holder;

                    lcd_cache->set(dictionary_key, cached_values);
                }
            }
        }

        if constexpr (has_mapped)
            mapped_cache.resize(key_columns[0]->size());

        visit_cache.assign(key_columns[0]->size(), VisitValue::Empty);

        size_of_index_type = column->getSizeOfIndexType();
        positions = column->getIndexesPtr().get();
    }

    ALWAYS_INLINE void resetCache()
    {
        Base::resetCache();

        if (filled_visit_cache_indexes.size() > visit_cache.size() / 4)
            std::fill(visit_cache.begin(), visit_cache.end(), VisitValue::Empty);
        else
            for (UInt64 index : filled_visit_cache_indexes)
                visit_cache[index] = VisitValue::Empty;

        filled_visit_cache_indexes.clear();
    }

    ALWAYS_INLINE size_t getIndexAt(size_t row) const
    {
        switch (size_of_index_type)
        {
            case sizeof(UInt8): return assert_cast<const ColumnUInt8 *>(positions)->getElement(row);
            case sizeof(UInt16): return assert_cast<const ColumnUInt16 *>(positions)->getElement(row);
            case sizeof(UInt32): return assert_cast<const ColumnUInt32 *>(positions)->getElement(row);
            case sizeof(UInt64): return assert_cast<const ColumnUInt64 *>(positions)->getElement(row);
            default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column.");
        }
    }

    /// Get the key holder from the key columns for insertion into the hash table.
    ALWAYS_INLINE auto getKeyHolder(size_t row, Arena & pool) const
    {
        return Base::getKeyHolder(getIndexAt(row), pool);
    }

    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplaceKey(Data & data, size_t row_, Arena & pool)
    {
        size_t row = getIndexAt(row_);

        if (is_nullable && row == 0)
        {
            setVisited(row, VisitValue::Found);
            bool has_null_key = data.hasNullKeyData();
            data.hasNullKeyData() = true;

            if constexpr (has_mapped)
                return EmplaceResult(data.getNullKeyData(), mapped_cache[0], !has_null_key);
            else
                return EmplaceResult(!has_null_key);
        }

        if (visit_cache[row] == VisitValue::Found)
        {
            if constexpr (has_mapped)
                return EmplaceResult(mapped_cache[row], mapped_cache[row], false);
            else
                return EmplaceResult(false);
        }

        auto key_holder = getKeyHolder(row_, pool);
        auto key = keyHolderGetKey(key_holder);

        bool inserted = false;
        typename Data::LookupResult it;
        if (row < saved_hash.size())
            data.emplace(key_holder, it, inserted, saved_hash[row]);
        else
            data.emplace(key_holder, it, inserted);

        setVisited(row, VisitValue::Found);

        if constexpr (has_mapped)
        {
            auto & mapped = it->getMapped();
            if (inserted)
            {
                new (&mapped) Mapped();
            }
            mapped_cache[row] = mapped;
            return EmplaceResult(mapped, mapped_cache[row], inserted, std::move(key));
        }
        else
            return EmplaceResult(inserted);
    }

    ALWAYS_INLINE bool isNullAt(size_t i)
    {
        if (!is_nullable)
            return false;

        return getIndexAt(i) == 0;
    }

    template <typename Data>
    ALWAYS_INLINE FindResult findKey(Data & data, size_t row_, Arena & pool)
    {
        size_t row = getIndexAt(row_);

        if (is_nullable && row == 0)
        {
            if constexpr (has_mapped)
                return FindResult(data.hasNullKeyData() ? &data.getNullKeyData() : nullptr, data.hasNullKeyData(), 0);
            else
                return FindResult(data.hasNullKeyData(), 0);
        }

        if (visit_cache[row] != VisitValue::Empty)
        {
            if constexpr (has_mapped)
                return FindResult(&mapped_cache[row], visit_cache[row] == VisitValue::Found, 0);
            else
                return FindResult(visit_cache[row] == VisitValue::Found, 0);
        }

        auto key_holder = getKeyHolder(row_, pool);

        typename Data::LookupResult it;
        if (row < saved_hash.size())
            it = data.find(keyHolderGetKey(key_holder), saved_hash[row]);
        else
            it = data.find(keyHolderGetKey(key_holder));

        bool found = it;
        setVisited(row, found ? VisitValue::Found : VisitValue::NotFound);

        if constexpr (has_mapped)
        {
            if (found)
                mapped_cache[row] = it->getMapped();
        }

        size_t offset = 0;

        if constexpr (FindResult::has_offset)
            offset = found ? data.offsetInternal(it) : 0;

        if constexpr (has_mapped)
            return FindResult(&mapped_cache[row], found, offset);
        else
            return FindResult(found, offset);
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & pool)
    {
        row = getIndexAt(row);
        if (row < saved_hash.size())
            return saved_hash[row];

        return Base::getHash(data, row, pool);
    }
};

class HashMethodSerializedContext : public HashMethodContext
{
public:
    explicit HashMethodSerializedContext(const HashMethodContextSettings & settings_)
        : settings(settings_)
    {}

    HashMethodContextSettings settings;
};

/** Hash by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename Value, typename Mapped, bool nullable, bool prealloc>
struct HashMethodSerialized
    : public columns_hashing_impl::HashMethodBase<HashMethodSerialized<Value, Mapped, nullable, prealloc>, Value, Mapped, false>
{
    using Self = HashMethodSerialized<Value, Mapped, nullable, prealloc>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    static HashMethodContextPtr createContext(const HashMethodContextSettings & settings)
    {
        return std::make_shared<HashMethodSerializedContext>(settings);
    }

    static constexpr bool has_cheap_key_calculation = false;
    /// `getKeyHolder` serializes every key column for the row. With `prealloc = false` that means a
    /// fresh `serializeKeysToPoolContiguous` into the arena; with `prealloc = true` and batch
    /// serialization disabled it means a per-row heap allocation plus the same serialization. This is
    /// the dominant cost of the aggregation, so `Aggregator` must not pay it twice per row to
    /// prefetch. When the keys *are* batch-serialized upfront this method prefetches on its own,
    /// using `precomputed_hashes` below, which needs no second `getKeyHolder` call.
    static constexpr bool has_cheap_key_holder = false;
    static constexpr bool has_pre_computed_hashes = prealloc;

    ColumnRawPtrs key_columns;
    size_t keys_size;
    std::vector<const UInt8 *> null_maps;

    /// Only used if prealloc is true.
    PaddedPODArray<UInt64> row_sizes;
    size_t total_size = 0;

    IColumn::SerializationSettings serialization_settings;
    std::vector<std::string_view> serialized_keys;

    /// The block laid out in one piece, for a caller that does not go through its rows in order and
    /// so cannot be given a chunk at a time.
    PaddedPODArray<char> serialized_buffer;
    bool use_whole_block = false;
    /// Whether a caller that cannot take the chunk gets the whole block laid out, or its keys
    /// serialized a row at a time. Set from the row width, see the constructor.
    bool whole_block_allowed = false;
    /// Which of the three layouts this block gets is settled on the first key, by which point the
    /// caller has had its chance to ask for the chunk.
    bool layout_decided = false;

    size_t avg_row_size = 0;

    /// Whether the block's keys are laid out a chunk at a time, one key column at a time, rather
    /// than a row at a time. Only a caller that goes through the rows in order may turn it on.
    bool can_use_key_region = false;
    /// Set when the chunk goes into a buffer reused across chunks rather than into the arena: the
    /// cells carry an aggregate state, and the hash table copies an inserted key out of it.
    bool use_chunk_scratch = false;
    PaddedPODArray<char> chunk_scratch;
    bool use_key_region = false;
    /// Rows are done a chunk at a time, so that what is written, hashed and probed stays in cache -
    /// a block laid out in one piece is read back from memory, and that costs from 7% of the
    /// aggregation at 128 KiB to 27% at 32 MiB. A block that is small enough to stay in cache anyway
    /// is done in one chunk instead, which only applies where the chunk is a reused buffer.
    static constexpr size_t region_chunk_bytes = 128 * 1024;
    static constexpr size_t chunk_whole_block_below = 4 * 1024 * 1024;
    /// A region is carved from the arena in one piece and handed out to the chunks that follow, so
    /// that a chunk which inserts nothing costs no allocation of its own.
    static constexpr size_t region_bytes = 1024 * 1024;
    char * region_free = nullptr;   /// first byte no inserted key owns
    VectorWithMemoryTracking<char *> chunk_memories;
    char * region_end = nullptr;
    /// The arena the region was taken from, so that its unused tail can be given back.
    Arena * region_pool = nullptr;
    size_t chunk_begin = 0;
    size_t chunk_end = 0;

    /// Reused across the rows of a block, for the keys that are serialized one row at a time.
    PaddedPODArray<char> key_scratch;

    /// Per-row canonical hashes computed from `serialized_keys` using the hash table's hash function.
    /// Filled lazily on the first emplace/find call (because we need access to `Data::hash`).
    /// Only used when `can_precompute_hashes` is true.
    PaddedPODArray<size_t> precomputed_hashes;
    /// `precomputed_hashes_initialized` starts `true` by default so the hot path skips the lazy-init
    /// gate when precomputation is statically disabled. It is set to `false` in the constructor only
    /// when we actually plan to precompute hashes (and is flipped back to `true` after the first call).
    bool precomputed_hashes_initialized = true;
    /// One past the last row of `precomputed_hashes` that holds a computed value. The region
    /// prepares one chunk at a time, so this is the chunk's end rather than the block's, and both
    /// the row's own hash and the one the look-ahead reaches for have to stay below it.
    size_t precomputed_hashes_end = 0;
    bool can_precompute_hashes = false;
    bool prefetch_enabled = false;
    /// The look-ahead is measured over the first rows of the block. The arena batch runs
    /// `initPrecomputedHashes` once per chunk, but the measurement is only meaningful once.
    bool prefetch_calibrated = false;

    /// Skip the precomputed-hash prefetch path when the hash table's buffer is below this size,
    /// matching the existing `min_bytes_for_prefetch` contract used by `Aggregator::executeImpl`.
    /// Checked lazily on the first emplace/find call.
    size_t min_bytes_for_prefetch = 0;

    std::unique_ptr<PrefetchingHelper> prefetching;
    size_t prefetch_look_ahead = PrefetchingHelper::getInitialLookAheadValue();
    /// Absolute row index at which `calcPrefetchLookAhead` should fire. Computed lazily as
    /// `first_row + PrefetchingHelper::iterationsToMeasure()` so that calibration is
    /// interval-relative — matches the pattern used in `Aggregator::executeImplBatch` and
    /// remains correct when `emplaceKey`/`findKey` are called over sliced ranges
    /// (e.g. `executeOnBlockSmall` with non-zero `row_begin`).
    size_t calibration_row = PrefetchingHelper::iterationsToMeasure();

    HashMethodSerialized(const ColumnRawPtrs & key_columns_, const Sizes & /*key_sizes*/, const HashMethodContextPtr & context)
        : key_columns(key_columns_), keys_size(key_columns_.size())
    {
        const auto * hash_serialized_context = typeid_cast<const HashMethodSerializedContext *>(context.get());
        if (!hash_serialized_context)
        {
            const auto & cached_val = *context;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid type for HashMethodSerialized context: {}",
                            demangle(typeid(cached_val).name()));
        }

        serialization_settings.serialize_string_with_zero_byte = hash_serialized_context->settings.serialize_string_with_zero_byte;
        if constexpr (nullable)
        {
            null_maps.resize(keys_size, nullptr);
            for (size_t i = 0; i < keys_size; ++i)
            {
                if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(key_columns[i]))
                {
                    null_maps[i] = nullable_column->getNullMapData().data();
                    key_columns[i] = nullable_column->getNestedColumnPtr().get();
                }
            }
        }

        if constexpr (prealloc)
        {
            null_maps.resize(keys_size, nullptr);

            /// Calculate serialized value size for each key column in each row.
            for (size_t i = 0; i < keys_size; ++i)
                key_columns[i]->collectSerializedValueSizes(row_sizes, null_maps[i], &serialization_settings);

            for (auto row_size : row_sizes)
                total_size += row_size;

            /// Laying a block's keys out ahead of the probe loop is what makes their hashes, and
            /// with them the probe prefetch, possible at all: a key serialized a row at a time
            /// exists only for the row that asked for it, and that path measures the same with the
            /// prefetch on and off. There are two ways to lay them out, and they cost differently.
            ///
            /// A caller that goes through the rows in order takes them a chunk at a time. A chunk
            /// stays in cache between being written and being probed, so it is worth taking at any
            /// width - measured from 40 to 1024 bytes per row, on Graviton4, Zen5 and Granite
            /// Rapids, at one thread and at 32, it never costs anything and is worth up to 3x.
            avg_row_size = total_size / std::max(row_sizes.size(), 1UL);
            can_use_key_region = total_size != 0;

            /// Any other caller - the adaptive aggregator visits a block's rows grouped by bucket -
            /// can only take the whole block at once, which is written to memory and read back
            /// rather than kept in cache. That pays while a row is narrow: 44% at 56 bytes per row,
            /// against 22-40% lost at 192 and beyond once threads compete for bandwidth.
            whole_block_allowed = avg_row_size < 128;
        }

        /// We can only precompute canonical per-row hashes when:
        ///   1. We have the serialized keys upfront (batch serialization is in use), and
        ///   2. We use the hash table's actual hash function (deferred to first emplace/find), and
        ///   3. Software prefetch is enabled by the caller (mirrors `enable_software_prefetch_in_aggregation`).
        /// Without batch serialization, fall back to the regular `data.prefetch(key_holder)` path.
        /// The hash-table size threshold (`min_bytes_for_prefetch`) is enforced lazily on the first
        /// emplace/find call, once `Data` is known.
        if constexpr (has_pre_computed_hashes)
        {
            prefetch_enabled = hash_serialized_context->settings.enable_prefetch;
            min_bytes_for_prefetch = hash_serialized_context->settings.min_bytes_for_prefetch;
        }
    }

    /// The block is done with: give back what the last chunk did not need. Declaring this also
    /// stops the state from being moved - moving it would leave two owners of the same tail.
    ~HashMethodSerialized()
    {
        releaseUnusedRegionTail();
    }

    /// Compute per-row canonical hashes from `serialized_keys` using `Data::hash`.
    /// Called once on the first `emplaceKey`/`findKey`, when `Data` becomes known.
    /// Also applies the `min_bytes_for_prefetch` size-threshold contract: skip the precomputed-hash
    /// + prefetch path when the hash table is small enough to fit in caches. Matches
    /// `Aggregator::executeImpl`'s `prefetch` gate.
    template <typename Data>
    NO_INLINE void initPrecomputedHashes(const Data & data, size_t first_row)
        requires prealloc
    {
        precomputed_hashes_initialized = true;
        if (!prefetch_calibrated)
        {
            prefetch_calibrated = true;
            calibration_row = first_row + PrefetchingHelper::iterationsToMeasure();
        }

        if (min_bytes_for_prefetch != 0 && data.getBufferSizeInBytes() <= min_bytes_for_prefetch)
        {
            can_precompute_hashes = false;
            precomputed_hashes_end = 0;
            return;
        }

        const size_t rows = serialized_keys.size();
        precomputed_hashes.resize(rows);
        /// The key region materialises one chunk at a time, so only that chunk's keys are there.
        const size_t begin = use_key_region ? chunk_begin : 0;
        const size_t end = use_key_region ? chunk_end : rows;
        for (size_t i = begin; i < end; ++i)
            precomputed_hashes[i] = data.hash(serialized_keys[i]);
        precomputed_hashes_end = end;
    }


    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    /// `table_bytes` is what the table the rows are about to be probed against occupies now.
    ///
    /// Laying the keys out pays for itself as soon as the probe misses, at any width: from 13
    /// thousand keys on it is worth 20-50% out to 1024 bytes per row. Against a table small enough
    /// to stay in cache it buys only the batch serialization, and past some row width writing the
    /// row out and reading it back costs more than that.
    ///
    /// Where that width lies is the machine's answer, not ours: at a thousand keys, Zen5 and Granite
    /// Rapids still gain at 464 bytes per row and only lose past 528, while the machine the
    /// performance tests run on loses 11-19% already at 144 (`group_by_multiple_strings`, its 64
    /// character keys, at one to thirty-two threads). The bound is set where the machines agree
    /// rather than where the fastest of them would have it, which is the same 128 bytes the
    /// whole-block layout is bounded by: a cache-resident table keeps the layout only while its rows
    /// are narrow enough that nobody disputes it.
    void enableKeyRegion(size_t table_bytes)
    {
        static constexpr size_t cache_resident_table_max_bytes = 256 * 1024;
        static constexpr size_t cache_resident_max_row_size = 128;
        if (table_bytes <= cache_resident_table_max_bytes && avg_row_size >= cache_resident_max_row_size)
            return;

        enableKeyRegion();
    }

    void enableKeyRegion()
    {
        if (!can_use_key_region || layout_decided)
            return;

        use_key_region = true;
        layout_decided = true;
        /// A key in a cell that also holds an aggregate state is copied into the arena in front of
        /// that state, so the layout goes into a buffer reused across chunks rather than the arena.
        if constexpr (Base::has_mapped)
            use_chunk_scratch = true;
        armPrecomputedHashes();
    }

    /// With the keys materialised ahead of the loop their hashes can be too, which is what the
    /// prefetch needs. Until one of the two layouts is settled there is nothing to compute them from.
    void armPrecomputedHashes()
    {
        if (has_pre_computed_hashes && prefetch_enabled && !prefetching)
        {
            can_precompute_hashes = true;
            precomputed_hashes_initialized = false;
            prefetching = std::make_unique<PrefetchingHelper>();
        }
    }

    /// Whether the key views this state hands out keep pointing at the same bytes for the whole
    /// block, rather than at scratch the next row or the next chunk writes over. A caller that
    /// remembers a view past the row it came from - the count kernel's run tracking - may only do
    /// so when this holds. The layout is settled here if it has not been already, because the
    /// answer depends on it.
    bool keyViewsAreBlockStable()
    requires prealloc
    {
        if (!layout_decided && !use_key_region)
            prepareWholeBlock();

        /// The whole block is laid out in a buffer of its own, and the chunk is the keys' home
        /// when the cells hold nothing else. A chunk that is a reused buffer is not stable, and
        /// neither is the row-at-a-time scratch.
        return use_whole_block || (use_key_region && !use_chunk_scratch);
    }

    bool keyViewsAreBlockStable() const
    requires(!prealloc)
    {
        /// The key is serialized into the arena a row at a time, and it stays there only if the row
        /// inserts: a `SerializedKeyHolder` that is discarded - which is what a lookup that finds
        /// its key does, and what the caller does with a key it only staged - rolls the arena back,
        /// and the next row writes the same bytes again.
        return false;
    }

    /// Lay the whole block out at once, for a caller that never asked for the chunk. This is the
    /// layout the rows of a block get when they are visited in some order of the caller's own - the
    /// adaptive aggregator groups them by bucket, for one - which a chunk cannot serve.
    void NO_INLINE prepareWholeBlock()
    {
        layout_decided = true;
        if (total_size == 0 || !whole_block_allowed)
            return;

        use_whole_block = true;
        armPrecomputedHashes();

        const size_t rows = row_sizes.size();
        serialized_buffer.resize(total_size);
        serialized_keys.resize(rows);
        chunk_memories.resize(rows);

        char * memory = serialized_buffer.data();
        for (size_t i = 0; i < rows; ++i)
        {
            serialized_keys[i] = std::string_view(memory, row_sizes[i]);
            chunk_memories[i] = memory;
            memory += row_sizes[i];
        }

        for (size_t j = 0; j < keys_size; ++j)
        {
            if constexpr (nullable)
                key_columns[j]->batchSerializeValueIntoMemoryWithNull(chunk_memories, 0, rows, null_maps[j], &serialization_settings);
            else
                key_columns[j]->batchSerializeValueIntoMemory(chunk_memories, 0, rows, &serialization_settings);
        }
    }

    /// Serialize the next chunk of rows into the key region, where their keys will stay. The
    /// region is carved from the arena in one go and managed here: the keys kept so far sit at its
    /// front, the chunk is laid out after them, and the rows that turn out to be duplicates are
    /// written over by the rows that follow.
    /// Only the rows that turn out to be new keys keep their bytes; the rest of the region is
    /// untouched, and a block of nothing but duplicates leaves nearly all of it that way. Give the
    /// tail back rather than let a block's worth of arena go with every block - what a set map
    /// holds should follow the keys, not the number of blocks that went past it.
    ///
    /// The arena hands memory out by bumping a pointer, so the tail can only go back while it is
    /// still the last thing handed out. Nothing else takes from this arena today - a region is only
    /// used where the cells hold no aggregate state - but the arena's own pointer says so, and that
    /// stays true whatever else starts sharing it.
    void releaseUnusedRegionTail()
    {
        if (!region_pool || region_free == region_end)
            return;

        if (region_pool->position() == region_end)
            region_pool->rollback(static_cast<size_t>(region_end - region_free));

        region_pool = nullptr;
        region_free = nullptr;
        region_end = nullptr;
    }

    void NO_INLINE prepareKeyChunk(size_t first_row, Arena & pool)
    {
        const size_t rows = row_sizes.size();
        chunk_begin = first_row;
        chunk_end = first_row;

        /// Only where the chunk is a buffer reused across chunks. Where it is the keys' home, a
        /// chunk covering the whole block would leave the bytes of its duplicate rows behind
        /// instead of letting the next chunk write over them.
        const size_t chunk_bytes
            = use_chunk_scratch && total_size <= chunk_whole_block_below ? total_size : region_chunk_bytes;

        size_t bytes = 0;
        while (chunk_end < rows && (bytes == 0 || bytes + row_sizes[chunk_end] <= chunk_bytes))
        {
            bytes += row_sizes[chunk_end];
            ++chunk_end;
        }

        if (use_chunk_scratch)
        {
            chunk_scratch.resize(bytes);
            region_free = chunk_scratch.data();
            region_end = region_free + bytes;
        }
        /// Both pointers start unset, and subtracting one unset pointer from another is not the
        /// zero it looks like, so the first chunk asks the question the other way round.
        else if (!region_free || static_cast<size_t>(region_end - region_free) < bytes)
        {
            /// What is left of the old region is too small for this chunk, so it is over: give back
            /// whatever no key took before asking for the next one.
            releaseUnusedRegionTail();

            const size_t size = std::max(region_bytes, bytes);
            region_free = pool.alloc(size);
            region_end = region_free + size;
            region_pool = &pool;
        }

        serialized_keys.resize(rows);
        chunk_memories.resize(rows);

        char * memory = region_free;
        for (size_t i = chunk_begin; i < chunk_end; ++i)
        {
            serialized_keys[i] = std::string_view(memory, row_sizes[i]);
            chunk_memories[i] = memory;
            memory += row_sizes[i];
        }

        /// One pass per key column over the chunk, which keeps the per-row call out of the loop.
        for (size_t j = 0; j < keys_size; ++j)
        {
            if constexpr (nullable)
                key_columns[j]->batchSerializeValueIntoMemoryWithNull(chunk_memories, chunk_begin, chunk_end, null_maps[j], &serialization_settings);
            else
                key_columns[j]->batchSerializeValueIntoMemory(chunk_memories, chunk_begin, chunk_end, &serialization_settings);
        }

        /// The chunk's hashes are computed on the next `emplaceKey`, which is where `Data` is known -
        /// but only while they are still read. Once the mode is off - the caller asked for no
        /// prefetch, or a chunk found the table small enough to skip it - nothing looks at them, and
        /// computing them is a second hash of every row.
        if (can_precompute_hashes)
            precomputed_hashes_initialized = false;
    }

    /// Closes the hole a duplicate row left behind, right after the row that follows it was
    /// inserted: its key moves down onto the hole and its cell - still hot, and safe from a resize
    /// that has not happened yet - is repointed. A key keeps its bytes, so its hash and its slot do
    /// not change. The snapshot the caller took before the emplace is re-pointed along with the
    /// cell, because the move can overwrite the bytes it was reading.
    template <typename Data, typename LookupResult, typename Key>
    void ALWAYS_INLINE onEmplaced(size_t row, Data &, LookupResult cell, bool inserted, Key & snapshot)
    {
        if (!use_key_region || use_chunk_scratch || !inserted)
            return;

        if constexpr (std::is_pointer_v<LookupResult>)
        {
            const auto key = serialized_keys[row];
            if (key.data() != region_free)
            {
                memmove(region_free, key.data(), key.size());
                cell->relocateKey(typename Data::key_type(region_free, key.size()));
                serialized_keys[row] = std::string_view(region_free, key.size());
                snapshot = Key(region_free, key.size());
            }
            region_free += key.size();
        }
    }

    ALWAYS_INLINE ArenaKeyHolder getKeyHolder(size_t row, Arena & pool)
    requires prealloc
    {
        if (use_key_region)
        {
            if (row < chunk_begin || row >= chunk_end) [[unlikely]]
                prepareKeyChunk(row, pool);
            return ArenaKeyHolder{
                serialized_keys[row], pool, use_chunk_scratch ? ArenaKeyPlacement::NeedsCopy : ArenaKeyPlacement::InArena};
        }

        if (!layout_decided) [[unlikely]]
            prepareWholeBlock();

        /// The whole block is laid out in a buffer of its own, which the hash table copies an
        /// inserted key out of - the buffer is reused by the next block.
        if (use_whole_block)
            return ArenaKeyHolder{serialized_keys[row], pool};

        /// Nothing to lay out - a block of empty keys. One buffer for the whole block instead of one
        /// allocation per row. The key stays valid
        /// until the next call for this state, which is what its callers need: they persist it into
        /// the arena or discard it before asking for the next row, and the key snapshot that
        /// `EmplaceResult` carries is read once `emplaceKey` has returned (the top-K heap keeps it).
        key_scratch.resize(row_sizes[row]);
        char * memory = key_scratch.data();
        std::string_view key(memory, row_sizes[row]);
        for (size_t j = 0; j < keys_size; ++j)
        {
            if constexpr (nullable)
                memory = key_columns[j]->serializeValueIntoMemoryWithNull(row, memory, null_maps[j], &serialization_settings);
            else
                memory = key_columns[j]->serializeValueIntoMemory(row, memory, &serialization_settings);
        }

        return ArenaKeyHolder{key, pool};
    }

    ALWAYS_INLINE SerializedKeyHolder getKeyHolder(size_t row, Arena & pool) const
    requires(!prealloc)
    {
        if constexpr (nullable)
        {
            const char * begin = nullptr;

            size_t sum_size = 0;
            for (size_t j = 0; j < keys_size; ++j)
                sum_size += key_columns[j]->serializeValueIntoArenaWithNull(row, pool, begin, null_maps[j], &serialization_settings).size();

            return SerializedKeyHolder{{begin, sum_size}, pool};
        }

        return SerializedKeyHolder{
            serializeKeysToPoolContiguous(row, keys_size, key_columns, pool, &serialization_settings),
            pool};
    }
};

}

/// Explicit instantiation of LowCardinalityDictionaryCache::cache which is a really heavy template
extern template class CacheBase<
    ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKey,
    ColumnsHashing::LowCardinalityDictionaryCache::CachedValues,
    ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKeyHash>;
}
