#pragma once

#include <atomic>
#include <chrono>
#include <variant>

#include <pcg_random.hpp>

#include <Common/randomSeed.h>
#include <Common/Arena.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/ArenaUtils.h>
#include <Common/HashTable/HashMap.h>
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
    /// Max size of storage in cells
    const size_t max_size_in_cells;
    /// Needed to perform check if cell is expired or not found. Default value is dictionary max lifetime.
    const size_t strict_max_lifetime_seconds;
    /// Lifetime of dictionary. Cell deadline is random value between lifetime min and max seconds.
    const DictionaryLifetime lifetime;
};

/** ICacheDictionaryStorage implementation that keeps keys in a HashMap
  * with global LRU eviction. Value in hash table points to index in attributes arrays.
  */
template <DictionaryKeyType dictionary_key_type>
class CacheDictionaryStorage final : public ICacheDictionaryStorage
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::Simple, UInt64, std::string_view>;

    explicit CacheDictionaryStorage(
        const DictionaryStructure & dictionary_structure,
        CacheDictionaryStorageConfiguration & configuration_)
        : configuration(configuration_)
        , rnd_engine(randomSeed())
    {
        storage.resize_fill(configuration.max_size_in_cells);
        createAttributes(dictionary_structure);
    }

    bool returnsFetchedColumnsInOrderOfRequestedKeys() const override { return true; }

    String getName() const override
    {
        if (dictionary_key_type == DictionaryKeyType::Simple)
            return "Cache";
        return "ComplexKeyCache";
    }

    bool supportsSimpleKeys() const override { return dictionary_key_type == DictionaryKeyType::Simple; }

    SimpleKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<UInt64> & keys,
        const DictionaryStorageFetchRequest & fetch_request,
        IColumn::Filter * const default_mask) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
            return fetchColumnsForKeysImpl<SimpleKeysStorageFetchResult>(keys, fetch_request, default_mask);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method fetchColumnsForKeys is not supported for complex key storage");
    }

    void insertColumnsForKeys(const PaddedPODArray<UInt64> & keys, Columns columns) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
            insertColumnsForKeysImpl(keys, columns);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertColumnsForKeys is not supported for complex key storage");
    }

    void insertDefaultKeys(const PaddedPODArray<UInt64> & keys) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
            insertDefaultKeysImpl(keys);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertDefaultKeysImpl is not supported for complex key storage");
    }

    PaddedPODArray<UInt64> getCachedSimpleKeys() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
            return getCachedKeysImpl();
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getCachedSimpleKeys is not supported for complex key storage");
    }

    bool supportsComplexKeys() const override { return dictionary_key_type == DictionaryKeyType::Complex; }

    ComplexKeysStorageFetchResult fetchColumnsForKeys(
        const PaddedPODArray<std::string_view> & keys,
        const DictionaryStorageFetchRequest & column_fetch_requests,
        IColumn::Filter * const default_mask) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
            return fetchColumnsForKeysImpl<ComplexKeysStorageFetchResult>(keys, column_fetch_requests, default_mask);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method fetchColumnsForKeys is not supported for simple key storage");
    }

    void insertColumnsForKeys(const PaddedPODArray<std::string_view> & keys, Columns columns) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
            insertColumnsForKeysImpl(keys, columns);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertColumnsForKeys is not supported for simple key storage");
    }

    void insertDefaultKeys(const PaddedPODArray<std::string_view> & keys) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
            insertDefaultKeysImpl(keys);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertDefaultKeysImpl is not supported for simple key storage");
    }

    PaddedPODArray<std::string_view> getCachedComplexKeys() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
            return getCachedKeysImpl();
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getCachedComplexKeys is not supported for simple key storage");
    }

    size_t getSize() const override { return size; }

    double getLoadFactor() const override { return static_cast<double>(size) / static_cast<double>(configuration.max_size_in_cells); }

    size_t getBytesAllocated() const override
    {
        size_t attributes_size_in_bytes = 0;
        size_t attributes_size = attributes.size();

        for (size_t attribute_index = 0; attribute_index < attributes_size; ++attribute_index)
        {
            getAttributeContainer(attribute_index, [&](const auto & container)
            {
                attributes_size_in_bytes += container.capacity() * sizeof(container[0]);
            });
        }

        return arena.allocatedBytes() + storage.allocated_bytes() + key_to_storage.getBufferSizeInBytes() + attributes_size_in_bytes;
    }

private:

    struct FetchedKey
    {
        FetchedKey(size_t element_index_, bool is_default_)
            : element_index(element_index_)
            , is_default(is_default_)
        {}

        size_t element_index;
        bool is_default;
    };

    template <typename KeysStorageFetchResult>
    KeysStorageFetchResult fetchColumnsForKeysImpl(
        const PaddedPODArray<KeyType> & keys,
        const DictionaryStorageFetchRequest & fetch_request,
        IColumn::Filter * const default_mask)
    {
        KeysStorageFetchResult result;

        result.fetched_columns = fetch_request.makeAttributesResultColumns();
        result.key_index_to_state.resize_fill(keys.size());

        const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

        size_t fetched_columns_index = 0;
        size_t keys_size = keys.size();

        PaddedPODArray<FetchedKey> fetched_keys;
        fetched_keys.resize_fill(keys_size);

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys[key_index];
            auto [key_state, storage_index] = findKey(key, now);

            if (unlikely(key_state == KeyState::not_found))
            {
                result.key_index_to_state[key_index] = {KeyState::not_found};
                ++result.not_found_keys_size;
                continue;
            }

            auto & cell = storage[storage_index];
            std::atomic_ref<uint8_t>(cell.clock_count).store(max_clock_count, std::memory_order_relaxed);

            result.expired_keys_size += static_cast<size_t>(key_state == KeyState::expired);

            result.key_index_to_state[key_index] = {key_state, fetched_columns_index};
            fetched_keys[fetched_columns_index] = FetchedKey(cell.element_index, cell.is_default);
            ++fetched_columns_index;

            result.key_index_to_state[key_index].setDefaultValue(cell.is_default);
            result.default_keys_size += cell.is_default;
        }

        result.found_keys_size = keys_size - (result.expired_keys_size + result.not_found_keys_size);

        for (size_t attribute_index = 0; attribute_index < fetch_request.attributesSize(); ++attribute_index)
        {
            if (!fetch_request.shouldFillResultColumnWithIndex(attribute_index))
                continue;

            auto & attribute = attributes[attribute_index];
            auto & fetched_column = *result.fetched_columns[attribute_index];
            fetched_column.reserve(fetched_columns_index);

            if (!default_mask)
            {
                const auto & default_value_provider =
                    fetch_request.defaultValueProviderAtIndex(attribute_index);

                if (unlikely(attribute.is_nullable))
                {
                    getItemsForFetchedKeys<Field>(
                        attribute,
                        fetched_columns_index,
                        fetched_keys,
                        [&](Field & value) { fetched_column.insert(value); },
                        default_value_provider);
                }
                else
                {
                    auto type_call = [&](const auto & dictionary_attribute_type)
                    {
                        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
                        using AttributeType = typename Type::AttributeType;
                        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;
                        using ColumnType = typename ColumnProvider::ColumnType;
                        using ValueType = DictionaryValueType<AttributeType>;

                        ColumnType & column_typed = static_cast<ColumnType &>(fetched_column);

                        if constexpr (std::is_same_v<ValueType, Array>)
                        {
                            getItemsForFetchedKeys<ValueType>(
                                attribute,
                                fetched_columns_index,
                                fetched_keys,
                                [&](Array & value) { fetched_column.insert(value); },
                                default_value_provider);
                        }
                        else if constexpr (std::is_same_v<ValueType, std::string_view>)
                        {
                            getItemsForFetchedKeys<ValueType>(
                                attribute,
                                fetched_columns_index,
                                fetched_keys,
                                [&](std::string_view value) { fetched_column.insertData(value.data(), value.size()); },
                                default_value_provider);
                        }
                        else
                        {
                            auto & data = column_typed.getData();

                            getItemsForFetchedKeys<ValueType>(
                                attribute,
                                fetched_columns_index,
                                fetched_keys,
                                [&](auto value) { data.push_back(static_cast<AttributeType>(value)); },
                                default_value_provider);
                        }
                    };

                    callOnDictionaryAttributeType(attribute.type, type_call);
                }
            }
            else
            {
                if (unlikely(attribute.is_nullable))
                {
                    getItemsForFetchedKeysShortCircuit<Field>(
                        attribute,
                        fetched_columns_index,
                        fetched_keys,
                        [&](Field & value) { fetched_column.insert(value); },
                        *default_mask);
                }
                else
                {
                    auto type_call = [&](const auto & dictionary_attribute_type)
                    {
                        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
                        using AttributeType = typename Type::AttributeType;
                        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;
                        using ColumnType = typename ColumnProvider::ColumnType;
                        using ValueType = DictionaryValueType<AttributeType>;

                        ColumnType & column_typed = static_cast<ColumnType &>(fetched_column);

                        if constexpr (std::is_same_v<ValueType, Array>)
                        {
                            getItemsForFetchedKeysShortCircuit<ValueType>(
                                attribute,
                                fetched_columns_index,
                                fetched_keys,
                                [&](Array & value) { fetched_column.insert(value); },
                                *default_mask);
                        }
                        else if constexpr (std::is_same_v<ValueType, std::string_view>)
                        {
                            getItemsForFetchedKeysShortCircuit<ValueType>(
                                attribute,
                                fetched_columns_index,
                                fetched_keys,
                                [&](std::string_view value) { fetched_column.insertData(value.data(), value.size()); },
                                *default_mask);
                        }
                        else
                        {
                            auto & data = column_typed.getData();

                            getItemsForFetchedKeysShortCircuit<ValueType>(
                                attribute,
                                fetched_columns_index,
                                fetched_keys,
                                [&](auto value) { data.push_back(static_cast<AttributeType>(value)); },
                                *default_mask);
                        }
                    };

                    callOnDictionaryAttributeType(attribute.type, type_call);
                }
            }
        }

        return result;
    }

    void insertColumnsForKeysImpl(const PaddedPODArray<KeyType> & keys, Columns columns)
    {
        const auto now = std::chrono::system_clock::now();

        Field column_value;

        for (size_t key_index = 0; key_index < keys.size(); ++key_index)
        {
            auto key = keys[key_index];

            size_t slot = findStorageSlotForInsert(key);
            auto & cell = storage[slot];

            bool cell_was_default = cell.is_default;
            cell.is_default = false;

            bool was_inserted = cell.deadline == 0;

            if (was_inserted)
            {
                if constexpr (std::is_same_v<KeyType, std::string_view>)
                    cell.key = copyStringInArena(arena, key);
                else
                    cell.key = key;

                for (size_t attribute_index = 0; attribute_index < columns.size(); ++attribute_index)
                {
                    auto & column = columns[attribute_index];

                    getAttributeContainer(attribute_index, [&](auto & container)
                    {
                        container.emplace_back();
                        cell.element_index = container.size() - 1;

                        using ElementType = std::decay_t<decltype(container[0])>;

                        column->get(key_index, column_value);

                        if constexpr (std::is_same_v<ElementType, Field>)
                        {
                            container.back() = column_value;
                        }
                        else if constexpr (std::is_same_v<ElementType, std::string_view>)
                        {
                            const String & string_value = column_value.safeGet<String>();
                            std::string_view inserted_value = copyStringInArena(arena, string_value);
                            container.back() = inserted_value;
                        }
                        else
                        {
                            container.back() = static_cast<ElementType>(column_value.safeGet<ElementType>());
                        }
                    });
                }

                ++size;
            }
            else
            {
                if (cell.key != key)
                {
                    if constexpr (std::is_same_v<KeyType, std::string_view>)
                    {
                        char * data = const_cast<char *>(cell.key.data());
                        arena.free(data, cell.key.size());
                        cell.key = copyStringInArena(arena, key);
                    }
                    else
                        cell.key = key;
                }

                /// Put values into existing index
                size_t index_to_use = cell.element_index;

                for (size_t attribute_index = 0; attribute_index < columns.size(); ++attribute_index)
                {
                    auto & column = columns[attribute_index];

                    getAttributeContainer(attribute_index, [&](auto & container)
                    {
                        using ElementType = std::decay_t<decltype(container[0])>;

                        column->get(key_index, column_value);

                        if constexpr (std::is_same_v<ElementType, Field>)
                        {
                            container[index_to_use] = column_value;
                        }
                        else if constexpr (std::is_same_v<ElementType, std::string_view>)
                        {
                            const String & string_value = column_value.safeGet<String>();
                            std::string_view inserted_value = copyStringInArena(arena, string_value);

                            if (!cell_was_default)
                            {
                                std::string_view previous_value = container[index_to_use];
                                arena.free(const_cast<char *>(previous_value.data()), previous_value.size());
                            }

                            container[index_to_use] = inserted_value;
                        }
                        else
                        {
                            container[index_to_use] = static_cast<ElementType>(column_value.safeGet<ElementType>());
                        }
                    });
                }
            }

            /// Update HashMap after cell.key is set (points to stable arena memory for string_view keys)
            key_to_storage[cell.key] = slot;

            setCellDeadline(cell, now);
            cell.clock_count = initial_clock_count;
        }
    }

    void insertDefaultKeysImpl(const PaddedPODArray<KeyType> & keys)
    {
        const auto now = std::chrono::system_clock::now();

        size_t keys_size = keys.size();

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys[key_index];

            size_t slot = findStorageSlotForInsert(key);
            auto & cell = storage[slot];

            bool was_inserted = cell.deadline == 0;
            bool cell_was_default = cell.is_default;

            cell.is_default = true;

            if (was_inserted)
            {
                if constexpr (std::is_same_v<KeyType, std::string_view>)
                    cell.key = copyStringInArena(arena, key);
                else
                    cell.key = key;

                for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
                {
                    getAttributeContainer(attribute_index, [&](auto & container)
                    {
                        container.emplace_back();
                        cell.element_index = container.size() - 1;
                    });
                }

                ++size;
            }
            else
            {
                for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
                {
                    getAttributeContainer(attribute_index, [&](const auto & container)
                    {
                        using ElementType = std::decay_t<decltype(container[0])>;

                        if constexpr (std::is_same_v<ElementType, std::string_view>)
                        {
                            if (!cell_was_default)
                            {
                                std::string_view previous_value = container[cell.element_index];
                                arena.free(const_cast<char *>(previous_value.data()), previous_value.size());
                            }
                        }
                    });
                }

                if (cell.key != key)
                {
                    if constexpr (std::is_same_v<KeyType, std::string_view>)
                    {
                        char * data = const_cast<char *>(cell.key.data());
                        arena.free(data, cell.key.size());
                        cell.key = copyStringInArena(arena, key);
                    }
                    else
                        cell.key = key;
                }
            }

            /// Update HashMap after cell.key is set (points to stable arena memory for string_view keys)
            key_to_storage[cell.key] = slot;

            setCellDeadline(cell, now);
            cell.clock_count = initial_clock_count;
        }
    }

    PaddedPODArray<KeyType> getCachedKeysImpl() const
    {
        PaddedPODArray<KeyType> result;
        result.reserve(size);

        for (auto & cell : storage)
        {
            if (cell.deadline == 0)
                continue;

            if (cell.is_default)
                continue;

            result.emplace_back(cell.key);
        }

        return result;
    }

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && func)
    {
        auto & attribute = attributes[attribute_index];
        auto & attribute_type = attribute.type;

        if (unlikely(attribute.is_nullable))
        {
            auto & container = std::get<ContainerType<Field>>(attribute.attribute_container);
            std::forward<GetContainerFunc>(func)(container);
        }
        else
        {
            auto type_call = [&](const auto & dictionary_attribute_type)
            {
                using Type = std::decay_t<decltype(dictionary_attribute_type)>;
                using AttributeType = typename Type::AttributeType;
                using ValueType = DictionaryValueType<AttributeType>;

                auto & container = std::get<ContainerType<ValueType>>(attribute.attribute_container);
                std::forward<GetContainerFunc>(func)(container);
            };

            callOnDictionaryAttributeType(attribute_type, type_call);
        }
    }

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && func) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->getAttributeContainer(attribute_index, std::forward<GetContainerFunc>(func));
    }

    template<typename ValueType>
    using ContainerType = std::conditional_t<
        std::is_same_v<ValueType, Field> || std::is_same_v<ValueType, Array>,
        VectorWithMemoryTracking<ValueType>,
        PaddedPODArray<ValueType>>;

    struct Attribute
    {
        AttributeUnderlyingType type;
        bool is_nullable;

        std::variant<
            ContainerType<UInt8>,
            ContainerType<UInt16>,
            ContainerType<UInt32>,
            ContainerType<UInt64>,
            ContainerType<UInt128>,
            ContainerType<UInt256>,
            ContainerType<Int8>,
            ContainerType<Int16>,
            ContainerType<Int32>,
            ContainerType<Int64>,
            ContainerType<Int128>,
            ContainerType<Int256>,
            ContainerType<Decimal32>,
            ContainerType<Decimal64>,
            ContainerType<Decimal128>,
            ContainerType<Decimal256>,
            ContainerType<DateTime64>,
            ContainerType<Float32>,
            ContainerType<Float64>,
            ContainerType<UUID>,
            ContainerType<IPv4>,
            ContainerType<IPv6>,
            ContainerType<std::string_view>,
            ContainerType<Array>,
            ContainerType<Field>> attribute_container;
    };

    template <typename ValueType, typename ValueSetter>
    void getItemsForFetchedKeys(
        Attribute & attribute,
        size_t fetched_keys_size,
        PaddedPODArray<FetchedKey> & fetched_keys,
        ValueSetter && value_setter,
        const DefaultValueProvider & default_value_provider)
    {
        auto & container = std::get<ContainerType<ValueType>>(attribute.attribute_container);

        for (size_t fetched_key_index = 0; fetched_key_index < fetched_keys_size; ++fetched_key_index)
        {
            auto fetched_key = fetched_keys[fetched_key_index];

            if (unlikely(fetched_key.is_default))
            {
                auto default_value = default_value_provider.getDefaultValue(fetched_key_index);

                if constexpr (std::is_same_v<ValueType, Field>)
                {
                    value_setter(default_value);
                }
                else if constexpr (std::is_same_v<ValueType, std::string_view>)
                {
                    auto & value = default_value.safeGet<String>();
                    value_setter(value);
                }
                else
                {
                    value_setter(default_value.safeGet<ValueType>());
                }
            }
            else
            {
                value_setter(container[fetched_key.element_index]);
            }
        }
    }

    template <typename ValueType, typename ValueSetter>
    void getItemsForFetchedKeysShortCircuit(
        Attribute & attribute,
        size_t fetched_keys_size,
        PaddedPODArray<FetchedKey> & fetched_keys,
        ValueSetter && value_setter,
        IColumn::Filter & default_mask)
    {
        default_mask.resize(fetched_keys_size);
        auto & container = std::get<ContainerType<ValueType>>(attribute.attribute_container);

        for (size_t fetched_key_index = 0; fetched_key_index < fetched_keys_size; ++fetched_key_index)
        {
            auto fetched_key = fetched_keys[fetched_key_index];

            if (unlikely(fetched_key.is_default))
            {
                default_mask[fetched_key_index] = 1;
                auto v = ValueType{};
                value_setter(v);
            }
            else
            {
                default_mask[fetched_key_index] = 0;
                value_setter(container[fetched_key.element_index]);
            }
        }
    }

    void createAttributes(const DictionaryStructure & dictionary_structure)
    {
        /// For each dictionary attribute create storage attribute
        /// For simple attributes create PODArray, for complex vector of Fields

        attributes.reserve(dictionary_structure.attributes.size());

        for (const auto & dictionary_attribute : dictionary_structure.attributes)
        {
            auto attribute_type = dictionary_attribute.underlying_type;

            auto type_call = [&](const auto & dictionary_attribute_type)
            {
                using Type = std::decay_t<decltype(dictionary_attribute_type)>;
                using AttributeType = typename Type::AttributeType;
                using ValueType = DictionaryValueType<AttributeType>;

                attributes.emplace_back();
                auto & last_attribute = attributes.back();
                last_attribute.type = attribute_type;
                last_attribute.is_nullable = dictionary_attribute.is_nullable;

                if (dictionary_attribute.is_nullable)
                    last_attribute.attribute_container = ContainerType<Field>();
                else
                    last_attribute.attribute_container = ContainerType<ValueType>();
            };

            callOnDictionaryAttributeType(attribute_type, type_call);
        }
    }

    using TimePoint = std::chrono::system_clock::time_point;

    struct Cell
    {
        KeyType key;
        size_t element_index;
        bool is_default;
        time_t deadline;
        /// Clock algorithm counter for approximate LRU eviction.
        /// 0 — cell is evictable, >0 — decremented each time the clock hand passes.
        /// Set to max_clock_count on cache hit, initial_clock_count on insert.
        uint8_t clock_count = 0;
    };

    static constexpr uint8_t initial_clock_count = 1;
    static constexpr uint8_t max_clock_count = 3;

    CacheDictionaryStorageConfiguration configuration;

    pcg64 rnd_engine;

    size_t size = 0;

    HashMap<KeyType, size_t> key_to_storage;
    PaddedPODArray<Cell> storage;
    size_t clock_hand = 0;

    ArenaWithFreeLists arena;

    VectorWithMemoryTracking<Attribute> attributes;

    void setCellDeadline(Cell & cell, TimePoint now)
    {
        if (configuration.lifetime.min_sec == 0 && configuration.lifetime.max_sec == 0)
        {
            /// This maybe not obvious, but when we define is this cell is expired or expired permanently, we add strict_max_lifetime_seconds
            /// to the expiration time. And it overflows pretty well.
            auto deadline = std::chrono::time_point<std::chrono::system_clock>::max() - 2 * std::chrono::seconds(configuration.strict_max_lifetime_seconds);
            cell.deadline = std::chrono::system_clock::to_time_t(deadline);
            return;
        }

        size_t min_sec_lifetime = configuration.lifetime.min_sec;
        size_t max_sec_lifetime = configuration.lifetime.max_sec;

        std::uniform_int_distribution<UInt64> distribution{min_sec_lifetime, max_sec_lifetime};

        auto deadline = now + std::chrono::seconds(distribution(rnd_engine));
        cell.deadline = std::chrono::system_clock::to_time_t(deadline);
    }

    using KeyStateAndCellIndex = std::pair<KeyState::State, size_t>;

    /// Look up a key in the cache. Returns the state and the index in storage.
    /// If the key is not found, the returned storage index is undefined.
    KeyStateAndCellIndex findKey(const KeyType key, const time_t now)
    {
        auto it = key_to_storage.find(key);
        if (it == key_to_storage.end())
            return {KeyState::not_found, 0};

        size_t storage_index = it->getMapped();
        const auto & cell = storage[storage_index];

        time_t max_lifetime_seconds = static_cast<time_t>(configuration.strict_max_lifetime_seconds);

        if (unlikely(now > cell.deadline + max_lifetime_seconds))
            return {KeyState::not_found, storage_index};

        if (unlikely(now > cell.deadline))
            return {KeyState::expired, storage_index};

        return {KeyState::found, storage_index};
    }

    /// Find a storage slot for inserting a key.
    /// If the key already exists, returns its current slot.
    /// Otherwise, allocates a new slot or evicts using the clock algorithm (approximate LRU, O(1) amortized).
    /// For new/evicted slots, the caller must update `key_to_storage` after setting `cell.key`.
    size_t findStorageSlotForInsert(const KeyType & key)
    {
        auto it = key_to_storage.find(key);
        if (it != key_to_storage.end())
            return it->getMapped();

        if (size < configuration.max_size_in_cells)
            return size;

        /// Clock algorithm: sweep the circular buffer, decrementing non-zero counts,
        /// until we find a cell with clock_count == 0 to evict.
        while (true)
        {
            auto & cell = storage[clock_hand];
            if (cell.clock_count == 0)
            {
                size_t victim = clock_hand;
                clock_hand = (clock_hand + 1) % configuration.max_size_in_cells;
                key_to_storage.erase(cell.key);
                return victim;
            }
            --cell.clock_count;
            clock_hand = (clock_hand + 1) % configuration.max_size_in_cells;
        }
    }
};

}
