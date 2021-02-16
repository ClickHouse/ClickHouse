#pragma once

#include <chrono>

#include <pcg_random.hpp>

#include <Common/randomSeed.h>
#include <Common/Arena.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/HashTable/LRUHashMap.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ICacheDictionaryStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct CacheDictionaryStorageConfiguration
{
    const size_t max_size_in_cells;
    const size_t strict_max_lifetime_seconds;
    const DictionaryLifetime lifetime;
};

template <typename CacheDictionaryStorage>
class ArenaCellDisposer
{
public:
    CacheDictionaryStorage & storage;

    template <typename Key, typename Value>
    void operator()(const Key & key, const Value & value) const
    {
        /// In case of complex key we keep it in arena
        if constexpr (std::is_same_v<Key, StringRef>)
        {
            storage.arena.free(const_cast<char *>(key.data), key.size);
        }

        storage.arena.free(value.place_for_serialized_columns, value.allocated_size_for_columns);
    }
};

/// TODO: Fix name
template <DictionaryKeyType dictionary_key_type>
class CacheDictionaryStorage final : public ICacheDictionaryStorage
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, UInt64, StringRef>;
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by CacheDictionaryStorage");

    explicit CacheDictionaryStorage(CacheDictionaryStorageConfiguration & configuration_)
        : configuration(configuration_)
        , rnd_engine(randomSeed())
        , cache(configuration.max_size_in_cells, false, ArenaCellDisposer<CacheDictionaryStorage<dictionary_key_type>> { *this })
    {
    }

    bool supportsSimpleKeys() const override
    {
        return dictionary_key_type == DictionaryKeyType::simple;
    }

    SimpleKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<UInt64> & keys,
        const DictionaryStorageFetchRequest & fetch_request) const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
        {
            SimpleKeysStorageFetchResult result;
            fetchColumnsForKeysImpl(keys, fetch_request, result);
            return result;
        }
        else
            throw Exception("Method insertColumnsForKeys is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertColumnsForKeys(const PaddedPODArray<UInt64> & keys, Columns columns) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
            insertColumnsForKeysImpl(keys, columns);
        else
            throw Exception("Method insertColumnsForKeys is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    PaddedPODArray<UInt64> getCachedSimpleKeys() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
            return getCachedKeysImpl();
        else
            throw Exception("Method getCachedSimpleKeys is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool supportsComplexKeys() const override
    {
        return dictionary_key_type == DictionaryKeyType::complex;
    }

    ComplexKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<StringRef> & keys,
        const DictionaryStorageFetchRequest & column_fetch_requests) const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
        {
            ComplexKeysStorageFetchResult result;
            fetchColumnsForKeysImpl(keys, column_fetch_requests, result);
            return result;
        }
        else
            throw Exception("Method fetchColumnsForKeys is not supported for simple key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertColumnsForKeys(const PaddedPODArray<StringRef> & keys, Columns columns) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
            insertColumnsForKeysImpl(keys, columns);
        else
            throw Exception("Method insertColumnsForKeys is not supported for simple key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    PaddedPODArray<StringRef> getCachedComplexKeys() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
            return getCachedKeysImpl();
        else
            throw Exception("Method getCachedSimpleKeys is not supported for simple key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t getSize() const override
    {
        return cache.size();
    }

    size_t getBytesAllocated() const override
    {
        return arena.size() + cache.getSizeInBytes();
    }
private:

    template <typename KeysStorageFetchResult>
    void fetchColumnsForKeysImpl(
        const PaddedPODArray<KeyType> & keys,
        const DictionaryStorageFetchRequest & fetch_request,
        KeysStorageFetchResult & result) const
    {
        result.fetched_columns = fetch_request.makeAttributesResultColumns();
        result.found_keys_to_fetched_columns_index.reserve(keys.size());

        const auto now = std::chrono::system_clock::now();

        size_t fetched_columns_index = 0;

        for (size_t key_index = 0; key_index < keys.size(); ++key_index)
        {
            auto key = keys[key_index];
            auto * it = cache.find(key);

            if (it)
            {
                /// Columns values for key are serialized in cache now deserialize them
                auto & cell = it->getMapped();

                if (now > cell.deadline + std::chrono::seconds(configuration.strict_max_lifetime_seconds))
                {
                    result.not_found_or_expired_keys.emplace_back(key);
                    continue;
                }
                else if (now > cell.deadline)
                {
                    result.expired_keys_to_fetched_columns_index[key] = fetched_columns_index;
                    result.not_found_or_expired_keys.emplace_back(key);
                }
                else
                    result.found_keys_to_fetched_columns_index[key] = fetched_columns_index;

                ++fetched_columns_index;

                const char * place_for_serialized_columns = cell.place_for_serialized_columns;

                for (size_t column_index = 0; column_index < result.fetched_columns.size(); ++column_index)
                {
                    auto & column = result.fetched_columns[column_index];

                    if (fetch_request.shouldFillResultColumnWithIndex(column_index))
                        place_for_serialized_columns = column->deserializeAndInsertFromArena(place_for_serialized_columns);
                    else
                        place_for_serialized_columns = column->skipSerializedInArena(place_for_serialized_columns);
                }
            }
            else
            {
                result.not_found_or_expired_keys.emplace_back(key);
                result.not_found_or_expired_keys_indexes.emplace_back(key_index);
            }
        }
    }

    void insertColumnsForKeysImpl(const PaddedPODArray<KeyType> & keys, Columns columns)
    {
        Arena temporary_values_pool;

        size_t columns_to_serialize_size = columns.size();
        PaddedPODArray<StringRef> temporary_column_data(columns_to_serialize_size);

        const auto now = std::chrono::system_clock::now();

        size_t keys_size = keys.size();

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            size_t allocated_size_for_columns = 0;
            const char * block_start = nullptr;

            auto key = keys[key_index];
            auto * it = cache.find(key);

            for (size_t column_index = 0; column_index < columns_to_serialize_size; ++column_index)
            {
                auto & column = columns[column_index];
                temporary_column_data[column_index] = column->serializeValueIntoArena(key_index, temporary_values_pool, block_start);
                allocated_size_for_columns += temporary_column_data[column_index].size;
            }

            char * place_for_serialized_columns = arena.alloc(allocated_size_for_columns);
            memcpy(reinterpret_cast<void*>(place_for_serialized_columns), reinterpret_cast<const void*>(block_start), allocated_size_for_columns);

            if (it)
            {
                /// Cell exists need to free previous serialized place and update deadline
                auto & cell = it->getMapped();

                arena.free(cell.place_for_serialized_columns, cell.allocated_size_for_columns);

                setCellDeadline(cell, now);
                cell.allocated_size_for_columns = allocated_size_for_columns;
                cell.place_for_serialized_columns = place_for_serialized_columns;
            }
            else
            {
                /// No cell exists so create and put in cache
                Cell cell;

                setCellDeadline(cell, now);
                cell.allocated_size_for_columns = allocated_size_for_columns;
                cell.place_for_serialized_columns = place_for_serialized_columns;

                if constexpr (dictionary_key_type == DictionaryKeyType::complex)
                {
                    /// Copy complex key into arena and put in cache
                    size_t key_size = key.size;
                    char * place_for_key = arena.alloc(key_size);
                    memcpy(reinterpret_cast<void*>(place_for_key), reinterpret_cast<const void*>(key.data), key_size);
                    KeyType updated_key { place_for_key, key_size };
                    key = updated_key;
                }

                cache.insert(key, cell);
            }

            temporary_values_pool.rollback(allocated_size_for_columns);
        }
    }

    PaddedPODArray<KeyType> getCachedKeysImpl() const
    {
        PaddedPODArray<KeyType> result;
        result.reserve(cache.size());

        for (auto & node : cache)
            result.emplace_back(node.getKey());

        return result;
    }

    using TimePoint = std::chrono::system_clock::time_point;

    struct Cell
    {
        TimePoint deadline;
        size_t allocated_size_for_columns;
        char * place_for_serialized_columns;
    };

    inline void setCellDeadline(Cell & cell, TimePoint now)
    {
        /// TODO: Fix deadlines

        size_t min_sec_lifetime = configuration.lifetime.min_sec;
        size_t max_sec_lifetime = configuration.lifetime.max_sec;

        std::uniform_int_distribution<UInt64> distribution{min_sec_lifetime, max_sec_lifetime};
        cell.deadline = now + std::chrono::seconds{distribution(rnd_engine)};
    }

    template <typename>
    friend class ArenaCellDisposer;

    CacheDictionaryStorageConfiguration configuration;

    ArenaWithFreeLists arena;

    pcg64 rnd_engine;

    using SimpleKeyLRUHashMap = LRUHashMap<UInt64, Cell, ArenaCellDisposer<CacheDictionaryStorage<dictionary_key_type>>>;
    using ComplexKeyLRUHashMap = LRUHashMapWithSavedHash<StringRef, Cell, ArenaCellDisposer<CacheDictionaryStorage<dictionary_key_type>>>;

    using CacheLRUHashMap = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::simple,
        SimpleKeyLRUHashMap,
        ComplexKeyLRUHashMap>;

    CacheLRUHashMap cache;
};

}
