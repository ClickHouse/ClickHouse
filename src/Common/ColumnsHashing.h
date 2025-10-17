#pragma once

#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ColumnsHashing/HashMethod.h>
#include <Common/ColumnsHashingImpl.h>
#include <Common/Arena.h>
#include <Common/CacheBase.h>
#include <Common/SipHash.h>
#include <Common/CurrentMetrics.h>
#include <Common/assert_cast.h>
#include <Interpreters/AggregationCommon.h>
#include <base/unaligned.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>

#include <Core/Defines.h>
#include <memory>
#include <cassert>

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
        const UInt64 * saved_hash = nullptr;
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

    static HashMethodContextPtr createContext(const HashMethodContextSettings & settings)
    {
        return std::make_shared<LowCardinalityDictionaryCache>(settings);
    }

    ColumnRawPtrs key_columns;
    const IColumn * positions = nullptr;
    size_t size_of_index_type = 0;

    /// saved hash is from current column or from cache.
    const UInt64 * saved_hash = nullptr;
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

        LowCardinalityDictionaryCache * lcd_cache;
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

        typename LowCardinalityDictionaryCache::DictionaryKey dictionary_key;
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
        if (saved_hash)
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
        if (saved_hash)
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
        if (saved_hash)
            return saved_hash[row];

        return Base::getHash(data, row, pool);
    }
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

    static constexpr bool has_cheap_key_calculation = false;

    ColumnRawPtrs key_columns;
    size_t keys_size;
    std::vector<const UInt8 *> null_maps;

    /// Only used if prealloc is true.
    PaddedPODArray<UInt64> row_sizes;
    size_t total_size = 0;
    bool use_batch_serialize = false;
    PaddedPODArray<char> serialized_buffer;
    std::vector<StringRef> serialized_keys;

    HashMethodSerialized(const ColumnRawPtrs & key_columns_, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
        : key_columns(key_columns_), keys_size(key_columns_.size())
    {
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
                key_columns[i]->collectSerializedValueSizes(row_sizes, null_maps[i]);

            for (auto row_size : row_sizes)
                total_size += row_size;

            use_batch_serialize = shouldUseBatchSerialize();
            if (use_batch_serialize)
            {
                serialized_buffer.resize(total_size);

                const size_t rows = row_sizes.size();
                char * memory = serialized_buffer.data();
                std::vector<char *> memories(rows);
                serialized_keys.resize(rows);
                for (size_t i = 0; i < row_sizes.size(); ++i)
                {
                    memories[i] = memory;
                    serialized_keys[i].data = memory;
                    serialized_keys[i].size = row_sizes[i];

                    memory += row_sizes[i];
                }

                for (size_t i = 0; i < keys_size; ++i)
                {
                    if constexpr (nullable)
                        key_columns[i]->batchSerializeValueIntoMemoryWithNull(memories, null_maps[i]);
                    else
                        key_columns[i]->batchSerializeValueIntoMemory(memories);
                }
            }
        }
    }

    bool shouldUseBatchSerialize() const
    {
#if defined(__aarch64__)
        // On ARM64 architectures, always use batch serialization, otherwise it would cause performance degradation in related perf tests.
        return true;
#endif

        size_t l2_size = 256 * 1024;
#if defined(OS_LINUX) && defined(_SC_LEVEL2_CACHE_SIZE)
        if (auto ret = sysconf(_SC_LEVEL2_CACHE_SIZE); ret != -1)
            l2_size = ret;
#endif
        // Calculate the average row size.
        size_t avg_row_size = total_size / std::max(row_sizes.size(), 1UL);
        // Use batch serialization only if total size fits in 4x L2 cache and average row size is small.
        return total_size <= 4 * l2_size && avg_row_size < 128;
    }

    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ALWAYS_INLINE ArenaKeyHolder getKeyHolder(size_t row, Arena & pool) const
    requires(prealloc)
    {
        if (use_batch_serialize)
            return ArenaKeyHolder{serialized_keys[row], pool};
        else
        {
            std::unique_ptr<char[]> holder = std::make_unique<char[]>(row_sizes[row]);
            char * memory = holder.get();
            StringRef key(memory, row_sizes[row]);
            for (size_t j = 0; j < keys_size; ++j)
            {
                if constexpr (nullable)
                    memory = key_columns[j]->serializeValueIntoMemoryWithNull(row, memory, null_maps[j]);
                else
                    memory = key_columns[j]->serializeValueIntoMemory(row, memory);
            }

            return ArenaKeyHolder{key, pool, std::move(holder)};
        }
    }

    ALWAYS_INLINE SerializedKeyHolder getKeyHolder(size_t row, Arena & pool) const
    requires(!prealloc)
    {
        if constexpr (nullable)
        {
            const char * begin = nullptr;

            size_t sum_size = 0;
            for (size_t j = 0; j < keys_size; ++j)
                sum_size += key_columns[j]->serializeValueIntoArenaWithNull(row, pool, begin, null_maps[j]).size;

            return SerializedKeyHolder{{begin, sum_size}, pool};
        }

        return SerializedKeyHolder{
            serializeKeysToPoolContiguous(row, keys_size, key_columns, pool),
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
