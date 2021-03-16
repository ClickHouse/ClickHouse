#pragma once

#include <chrono>

#include <pcg_random.hpp>

#include <Common/randomSeed.h>
#include <Common/Arena.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/HashTable/LRUHashMap.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ICacheDictionaryStorage.h>
#include <Dictionaries/DictionaryHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct CacheDictionaryStorageConfiguration
{
    /// Max size of storage in cells
    const size_t max_size_in_cells;
    /// Needed to perform check if cell is expired or not found. Default value is dictionary max lifetime.
    const size_t strict_max_lifetime_seconds;
    /// Lifetime of dictionary. Cell deadline is random value between lifetime min and max seconds.
    const DictionaryLifetime lifetime;
};

/** Keys are stored in LRUCache and column values are serialized into arena.

    Cell in LRUCache consists of allocated size and place in arena were columns serialized data is stored.

    Columns are serialized by rows.

    When cell is removed from LRUCache data associated with it is also removed from arena.

    In case of complex key we also store key data in arena and it is removed from arena.
*/
template <DictionaryKeyType dictionary_key_type>
class CacheDictionaryStorage final : public ICacheDictionaryStorage
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, UInt64, StringRef>;
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by CacheDictionaryStorage");

    explicit CacheDictionaryStorage(CacheDictionaryStorageConfiguration & configuration_)
        : configuration(configuration_)
        , rnd_engine(randomSeed())
        , cache(configuration.max_size_in_cells, false, { arena })
    {
    }

    bool returnsFetchedColumnsInOrderOfRequestedKeys() const override { return true; }

    String getName() const override
    {
        if (dictionary_key_type == DictionaryKeyType::simple)
            return "Cache";
        else
            return "ComplexKeyCache";
    }

    bool supportsSimpleKeys() const override { return dictionary_key_type == DictionaryKeyType::simple; }

    SimpleKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<UInt64> & keys,
        const DictionaryStorageFetchRequest & fetch_request) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
        {
            return fetchColumnsForKeysImpl<SimpleKeysStorageFetchResult>(keys, fetch_request);
        }
        else
            throw Exception("Method fetchColumnsForKeys is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertColumnsForKeys(const PaddedPODArray<UInt64> & keys, Columns columns) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
            insertColumnsForKeysImpl(keys, columns);
        else
            throw Exception("Method insertColumnsForKeys is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertDefaultKeys(const PaddedPODArray<UInt64> & keys) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
            insertDefaultKeysImpl(keys);
        else
            throw Exception("Method insertDefaultKeysImpl is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    PaddedPODArray<UInt64> getCachedSimpleKeys() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
            return getCachedKeysImpl();
        else
            throw Exception("Method getCachedSimpleKeys is not supported for complex key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool supportsComplexKeys() const override { return dictionary_key_type == DictionaryKeyType::complex; }

    ComplexKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<StringRef> & keys,
        const DictionaryStorageFetchRequest & column_fetch_requests) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
        {
            return fetchColumnsForKeysImpl<ComplexKeysStorageFetchResult>(keys, column_fetch_requests);
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

    void insertDefaultKeys(const PaddedPODArray<StringRef> & keys) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
            insertDefaultKeysImpl(keys);
        else
            throw Exception("Method insertDefaultKeysImpl is not supported for simple key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    PaddedPODArray<StringRef> getCachedComplexKeys() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
            return getCachedKeysImpl();
        else
            throw Exception("Method getCachedComplexKeys is not supported for simple key storage", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t getSize() const override { return cache.size(); }

    size_t getMaxSize() const override { return cache.getMaxSize(); }

    size_t getBytesAllocated() const override { return arena.size() + cache.getSizeInBytes(); }

private:

    template <typename KeysStorageFetchResult>
    ALWAYS_INLINE KeysStorageFetchResult fetchColumnsForKeysImpl(
        const PaddedPODArray<KeyType> & keys,
        const DictionaryStorageFetchRequest & fetch_request)
    {
        KeysStorageFetchResult result;

        result.fetched_columns = fetch_request.makeAttributesResultColumns();
        result.key_index_to_state.resize_fill(keys.size(), {KeyState::not_found});

        const auto now = std::chrono::system_clock::now();

        size_t fetched_columns_index = 0;

        std::chrono::seconds max_lifetime_seconds(configuration.strict_max_lifetime_seconds);

        size_t keys_size = keys.size();

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys[key_index];
            auto * it = cache.find(key);

            if (it)
            {
                /// Columns values for key are serialized in cache now deserialize them
                const auto & cell = it->getMapped();

                bool has_deadline = cellHasDeadline(cell);

                if (has_deadline && now > cell.deadline + max_lifetime_seconds)
                {
                    result.key_index_to_state[key_index] = {KeyState::not_found};
                    ++result.not_found_keys_size;
                    continue;
                }
                else if (has_deadline && now > cell.deadline)
                {
                    result.key_index_to_state[key_index] = {KeyState::expired, fetched_columns_index};
                    ++result.expired_keys_size;
                }
                else
                {
                    result.key_index_to_state[key_index] = {KeyState::found, fetched_columns_index};
                    ++result.found_keys_size;
                }

                ++fetched_columns_index;

                if (cell.isDefault())
                {
                    result.key_index_to_state[key_index].setDefault();
                    ++result.default_keys_size;
                    insertDefaultValuesIntoColumns(result.fetched_columns, fetch_request, key_index);
                }
                else
                {
                    const char * place_for_serialized_columns = cell.place_for_serialized_columns;
                    deserializeAndInsertIntoColumns(result.fetched_columns, fetch_request, place_for_serialized_columns);
                }
            }
            else
            {
                result.key_index_to_state[key_index] = {KeyState::not_found};
                ++result.not_found_keys_size;
            }
        }

        return result;
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

                if (cell.place_for_serialized_columns)
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

                insertCellInCache(key, cell);
            }

            temporary_values_pool.rollback(allocated_size_for_columns);
        }
    }

    void insertDefaultKeysImpl(const PaddedPODArray<KeyType> & keys)
    {
        const auto now = std::chrono::system_clock::now();

        for (auto key : keys)
        {
            auto * it = cache.find(key);

            if (it)
            {
                auto & cell = it->getMapped();

                setCellDeadline(cell, now);

                if (cell.place_for_serialized_columns)
                    arena.free(cell.place_for_serialized_columns, cell.allocated_size_for_columns);

                cell.allocated_size_for_columns = 0;
                cell.place_for_serialized_columns = nullptr;
            }
            else
            {
                Cell cell;

                setCellDeadline(cell, now);
                cell.allocated_size_for_columns = 0;
                cell.place_for_serialized_columns = nullptr;

                insertCellInCache(key, cell);
            }
        }
    }

    PaddedPODArray<KeyType> getCachedKeysImpl() const
    {
        PaddedPODArray<KeyType> result;
        result.reserve(cache.size());

        for (auto & node : cache)
        {
            auto & cell = node.getMapped();

            if (cell.isDefault())
                continue;

            result.emplace_back(node.getKey());
        }

        return result;
    }

    using TimePoint = std::chrono::system_clock::time_point;

    struct Cell
    {
        TimePoint deadline;
        size_t allocated_size_for_columns;
        char * place_for_serialized_columns;

        inline bool isDefault() const { return place_for_serialized_columns == nullptr; }
        inline void setDefault()
        {
            place_for_serialized_columns = nullptr;
            allocated_size_for_columns = 0;
        }
    };

    void insertCellInCache(KeyType & key, const Cell & cell)
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::complex)
        {
            /// Copy complex key into arena and put in cache
            size_t key_size = key.size;
            char * place_for_key = arena.alloc(key_size);
            memcpy(reinterpret_cast<void *>(place_for_key), reinterpret_cast<const void *>(key.data), key_size);
            KeyType updated_key{place_for_key, key_size};
            key = updated_key;
        }

        cache.insert(key, cell);
    }

    inline static bool cellHasDeadline(const Cell & cell)
    {
        return cell.deadline != std::chrono::system_clock::from_time_t(0);
    }

    inline void setCellDeadline(Cell & cell, TimePoint now)
    {
        if (configuration.lifetime.min_sec == 0 && configuration.lifetime.max_sec == 0)
        {
            cell.deadline = std::chrono::system_clock::from_time_t(0);
            return;
        }

        size_t min_sec_lifetime = configuration.lifetime.min_sec;
        size_t max_sec_lifetime = configuration.lifetime.max_sec;

        std::uniform_int_distribution<UInt64> distribution{min_sec_lifetime, max_sec_lifetime};
        cell.deadline = now + std::chrono::seconds(distribution(rnd_engine));
    }

    template <typename>
    friend class ArenaCellDisposer;

    CacheDictionaryStorageConfiguration configuration;

    ArenaWithFreeLists arena;

    pcg64 rnd_engine;

    class ArenaCellDisposer
    {
    public:
        ArenaWithFreeLists & arena;

        template <typename Key, typename Value>
        void operator()(const Key & key, const Value & value) const
        {
            /// In case of complex key we keep it in arena
            if constexpr (std::is_same_v<Key, StringRef>)
                arena.free(const_cast<char *>(key.data), key.size);

            if (value.place_for_serialized_columns)
                arena.free(value.place_for_serialized_columns, value.allocated_size_for_columns);
        }
    };

    using SimpleKeyLRUHashMap = LRUHashMap<UInt64, Cell, ArenaCellDisposer>;
    using ComplexKeyLRUHashMap = LRUHashMapWithSavedHash<StringRef, Cell, ArenaCellDisposer>;

    using CacheLRUHashMap = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::simple,
        SimpleKeyLRUHashMap,
        ComplexKeyLRUHashMap>;

    CacheLRUHashMap cache;
};

}
