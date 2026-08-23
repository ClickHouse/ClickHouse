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
    using EmplaceResult = columns_hashing_impl::EmplaceResultImpl<Mapped>;
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

        VisitValue empty(VisitValue::Empty);
        visit_cache.assign(key_columns[0]->size(), empty);

        size_of_index_type = column->getSizeOfIndexType();
        positions = column->getIndexesPtr().get();
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
            visit_cache[row] = VisitValue::Found;
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

        bool inserted = false;
        typename Data::LookupResult it;
        if (row < saved_hash.size())
            data.emplace(key_holder, it, inserted, saved_hash[row]);
        else
            data.emplace(key_holder, it, inserted);

        visit_cache[row] = VisitValue::Found;

        if constexpr (has_mapped)
        {
            auto & mapped = it->getMapped();
            if (inserted)
            {
                new (&mapped) Mapped();
            }
            mapped_cache[row] = mapped;
            return EmplaceResult(mapped, mapped_cache[row], inserted);
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
        visit_cache[row] = found ? VisitValue::Found : VisitValue::NotFound;

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
    bool use_batch_serialize = false;
    IColumn::SerializationSettings serialization_settings;
    PaddedPODArray<char> serialized_buffer;
    std::vector<std::string_view> serialized_keys;

    /// Whether the block's keys are serialized straight into the arena instead of into
    /// `serialized_buffer` or a per-row buffer. An inserted key is then already where it will stay,
    /// and the bytes of a duplicate row are taken back by the row that follows it.
    bool can_use_key_region = false;
    bool batch_serialized = false;
    /// LOCAL EXPERIMENT ONLY: same chunked one pass, but into a buffer reused across chunks, for the
    /// methods whose cells carry an aggregate state (the hash table copies an inserted key out of it).
    bool use_chunk_scratch = false;
    PaddedPODArray<char> chunk_scratch;
    bool use_key_region = false;
    /// Rows are done a chunk at a time: a whole block would need one large contiguous region, and a
    /// chunk this size is still written, hashed and probed while it is in cache.
    static constexpr size_t region_chunk_bytes = 128 * 1024;
    /// A region is carved from the arena and managed here, so nothing has to be given back to the
    /// arena - which would not be possible anyway once the aggregate states of the new keys are
    /// allocated from it between one chunk and the next.
    static constexpr size_t region_bytes = 1024 * 1024;
    char * region_free = nullptr;   /// first byte no inserted key owns
    VectorWithMemoryTracking<char *> chunk_memories;
    char * region_end = nullptr;
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

            const size_t avg_row_size = total_size / std::max(row_sizes.size(), 1UL);

            /// Where the cells hold only the key, serializing the block into a region of its own and
            /// letting the kept keys stay there saves the copy that batch serialization needs for
            /// every inserted key. Where they also hold an aggregate state it does not pay: batch
            /// serialization puts a new key in the arena right in front of its state, and keeping the
            /// two together is worth more than the copy.
            ///
            /// Laying the block out writes it with a row-sized stride, so like batch serialization
            /// this stops paying once a row is much wider than a cache line - measured, it wins from
            /// -72% at 128 bytes per row down to -9% at 512, and nothing beyond that.
            can_use_key_region = total_size != 0 && !Base::has_mapped && avg_row_size <= 512;
            use_batch_serialize = shouldUseBatchSerialize();

            /// LOCAL EXPERIMENT ONLY
            static const bool exp_chunk_mapped = std::getenv("CH_SER_CHUNK_MAPPED") != nullptr;
            if (exp_chunk_mapped && Base::has_mapped && total_size != 0)
                can_use_key_region = true;
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
            if (use_batch_serialize && prefetch_enabled)
            {
                can_precompute_hashes = true;
                precomputed_hashes_initialized = false;
                prefetching = std::make_unique<PrefetchingHelper>();
            }
        }
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
            return;
        }

        const size_t rows = serialized_keys.size();
        precomputed_hashes.resize(rows);
        /// The key region materialises one chunk at a time, so only that chunk's keys are there.
        const size_t begin = use_key_region ? chunk_begin : 0;
        const size_t end = use_key_region ? chunk_end : rows;
        for (size_t i = begin; i < end; ++i)
            precomputed_hashes[i] = data.hash(serialized_keys[i]);
    }

    bool shouldUseBatchSerialize() const
    {
#if defined(__aarch64__)
        /// On ARM64 architectures, always use batch serialization, otherwise it would cause performance degradation in related perf tests.
        /// Measured again on Graviton4 with this rule applied instead: every shape from 32 to 1024
        /// bytes per row, one and 32 threads, stayed within +-3% (worst case +6.5%), so the stride
        /// that costs x86 up to +85% at 32 threads costs nothing here and there is nothing to gain
        /// by dropping the shortcut.
        return true;
#endif

        /// One pass per key column writes the block with a row-sized stride, and the hash table then
        /// copies every inserted key out of that buffer, so it only pays while a row is a couple of
        /// cache lines wide. Measured on `group_by_multiple_strings`: it wins up to ~256 bytes per row
        /// with one thread and starts losing from ~128 upwards once threads compete for bandwidth.
        ///
        /// How large the block is does not belong in that decision. A large block of short rows is
        /// exactly where the one pass wins most: a 1M-row block of 20-byte keys is 4x faster with it.
        /// And where a long row does lose, the loss is not the buffer's size either - shrinking such
        /// a block 64 times, from 65536 rows to 1024, only takes it from +29% to +16%, while the row
        /// size alone decides whether it wins or loses at all.
        const size_t avg_row_size = total_size / std::max(row_sizes.size(), 1UL);
        return avg_row_size < 128;
    }

    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    void enableKeyRegion()
    {
        if (!can_use_key_region)
            return;

        use_key_region = true;
        use_batch_serialize = false;
        if constexpr (Base::has_mapped)
            use_chunk_scratch = true;
        /// With the keys materialised ahead of the loop their hashes can be too, which is what the
        /// prefetch needs. Until the region is turned on there is nothing to precompute them from.
        if (has_pre_computed_hashes && prefetch_enabled && !prefetching)
        {
            can_precompute_hashes = true;
            precomputed_hashes_initialized = false;
            prefetching = std::make_unique<PrefetchingHelper>();
        }
    }

    /// Serialize the whole block into `serialized_buffer`, one pass per key column. Done on the
    /// first key asked for rather than in the constructor, so that a caller which opts into the key
    /// region (`enableKeyRegion`) does not pay for a buffer it will not read - and one which does
    /// not opt in still gets this instead of serializing row by row.
    void NO_INLINE prepareBatchSerialize()
    {
        batch_serialized = true;

        serialized_buffer.resize(total_size);

        const size_t rows = row_sizes.size();
        char * memory = serialized_buffer.data();
        VectorWithMemoryTracking<char *> memories(rows);
        serialized_keys.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            memories[i] = memory;
            serialized_keys[i] = std::string_view(memory, row_sizes[i]);
            memory += row_sizes[i];
        }

        for (size_t i = 0; i < keys_size; ++i)
        {
            if constexpr (nullable)
                key_columns[i]->batchSerializeValueIntoMemoryWithNull(memories, null_maps[i], &serialization_settings);
            else
                key_columns[i]->batchSerializeValueIntoMemory(memories, &serialization_settings);
        }
    }

    /// Serialize the next chunk of rows into the key region, where their keys will stay. The
    /// region is carved from the arena in one go and managed here: the keys kept so far sit at its
    /// front, the chunk is laid out after them, and the rows that turn out to be duplicates are
    /// written over by the rows that follow.
    void NO_INLINE prepareKeyChunk(size_t first_row, Arena & pool)
    {
        const size_t rows = row_sizes.size();
        chunk_begin = first_row;
        chunk_end = first_row;

        size_t bytes = 0;
        while (chunk_end < rows && (bytes == 0 || bytes + row_sizes[chunk_end] <= region_chunk_bytes))
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
        else if (static_cast<size_t>(region_end - region_free) < bytes)
        {
            const size_t size = std::max(region_bytes, bytes);
            region_free = pool.alloc(size);
            region_end = region_free + size;
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

        /// The chunk's hashes are computed on the next `emplaceKey`, which is where `Data` is known.
        if (has_pre_computed_hashes)
            precomputed_hashes_initialized = false;
    }

    /// Closes the hole a duplicate row left behind, right after the row that follows it was
    /// inserted: its key moves down onto the hole and its cell - still hot, and safe from a resize
    /// that has not happened yet - is repointed. A key keeps its bytes, so its hash and its slot do
    /// not change.
    template <typename Data, typename LookupResult>
    void ALWAYS_INLINE onEmplaced(size_t row, Data &, LookupResult cell, bool inserted)
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
                serialized_keys[row], pool, {}, use_chunk_scratch ? ArenaKeyPlacement::NeedsCopy : ArenaKeyPlacement::InArena};
        }

        if (use_batch_serialize)
        {
            if (!batch_serialized) [[unlikely]]
                prepareBatchSerialize();
            return ArenaKeyHolder{serialized_keys[row], pool};
        }

        /// One buffer for the whole block instead of one allocation per row. The key stays valid
        /// only until the next call for this state, which is all its callers need: they persist it
        /// into the arena or discard it before asking for the next row.
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
