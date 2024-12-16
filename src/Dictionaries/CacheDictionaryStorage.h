#pragma once

#include <chrono>
#include <variant>

#include <pcg_random.hpp>

#include <Common/randomSeed.h>
#include <Common/Arena.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/ArenaUtils.h>
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
    /// Max size of storage in cells
    const size_t max_size_in_cells;
    /// Needed to perform check if cell is expired or not found. Default value is dictionary max lifetime.
    const size_t strict_max_lifetime_seconds;
    /// Lifetime of dictionary. Cell deadline is random value between lifetime min and max seconds.
    const DictionaryLifetime lifetime;
};

/** ICacheDictionaryStorage implementation that keeps key in hash table with fixed collision length.
  * Value in hash table point to index in attributes arrays.
  */
template <DictionaryKeyType dictionary_key_type>
class CacheDictionaryStorage final : public ICacheDictionaryStorage
{

    static constexpr size_t max_collision_length = 10;

public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::Simple, UInt64, StringRef>;

    explicit CacheDictionaryStorage(
        const DictionaryStructure & dictionary_structure,
        CacheDictionaryStorageConfiguration & configuration_)
        : configuration(configuration_)
        , rnd_engine(randomSeed())
    {
        size_t cells_size = roundUpToPowerOfTwoOrZero(std::max(configuration.max_size_in_cells, max_collision_length));

        cells.resize_fill(cells_size);
        size_overlap_mask = cells_size - 1;

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
        const PaddedPODArray<StringRef> & keys,
        const DictionaryStorageFetchRequest & column_fetch_requests,
        IColumn::Filter * const default_mask) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
            return fetchColumnsForKeysImpl<ComplexKeysStorageFetchResult>(keys, column_fetch_requests, default_mask);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method fetchColumnsForKeys is not supported for simple key storage");
    }

    void insertColumnsForKeys(const PaddedPODArray<StringRef> & keys, Columns columns) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
            insertColumnsForKeysImpl(keys, columns);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertColumnsForKeys is not supported for simple key storage");
    }

    void insertDefaultKeys(const PaddedPODArray<StringRef> & keys) override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
            insertDefaultKeysImpl(keys);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertDefaultKeysImpl is not supported for simple key storage");
    }

    PaddedPODArray<StringRef> getCachedComplexKeys() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
            return getCachedKeysImpl();
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getCachedComplexKeys is not supported for simple key storage");
    }

    size_t getSize() const override { return size; }

    double getLoadFactor() const override { return static_cast<double>(size) / configuration.max_size_in_cells; }

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

        return arena.allocatedBytes() + sizeof(Cell) * configuration.max_size_in_cells + attributes_size_in_bytes;
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
            auto [key_state, cell_index] = getKeyStateAndCellIndex(key, now);

            if (unlikely(key_state == KeyState::not_found))
            {
                result.key_index_to_state[key_index] = {KeyState::not_found};
                ++result.not_found_keys_size;
                continue;
            }

            auto & cell = cells[cell_index];

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
                        else if constexpr (std::is_same_v<ValueType, StringRef>)
                        {
                            getItemsForFetchedKeys<ValueType>(
                                attribute,
                                fetched_columns_index,
                                fetched_keys,
                                [&](StringRef value) { fetched_column.insertData(value.data, value.size); },
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
                        else if constexpr (std::is_same_v<ValueType, StringRef>)
                        {
                            getItemsForFetchedKeysShortCircuit<ValueType>(
                                attribute,
                                fetched_columns_index,
                                fetched_keys,
                                [&](StringRef value) { fetched_column.insertData(value.data, value.size); },
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

            size_t cell_index = getCellIndexForInsert(key);
            auto & cell = cells[cell_index];

            bool cell_was_default = cell.is_default;
            cell.is_default = false;

            bool was_inserted = cell.deadline == 0;

            if (was_inserted)
            {
                if constexpr (std::is_same_v<KeyType, StringRef>)
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
                        else if constexpr (std::is_same_v<ElementType, StringRef>)
                        {
                            const String & string_value = column_value.safeGet<String>();
                            StringRef inserted_value = copyStringInArena(arena, string_value);
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
                    if constexpr (std::is_same_v<KeyType, StringRef>)
                    {
                        char * data = const_cast<char *>(cell.key.data);
                        arena.free(data, cell.key.size);
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
                        else if constexpr (std::is_same_v<ElementType, StringRef>)
                        {
                            const String & string_value = column_value.safeGet<String>();
                            StringRef inserted_value = copyStringInArena(arena, string_value);

                            if (!cell_was_default)
                            {
                                StringRef previous_value = container[index_to_use];
                                arena.free(const_cast<char *>(previous_value.data), previous_value.size);
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

            setCellDeadline(cell, now);
        }
    }

    void insertDefaultKeysImpl(const PaddedPODArray<KeyType> & keys)
    {
        const auto now = std::chrono::system_clock::now();

        size_t keys_size = keys.size();

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys[key_index];

            size_t cell_index = getCellIndexForInsert(key);
            auto & cell = cells[cell_index];

            bool was_inserted = cell.deadline == 0;
            bool cell_was_default = cell.is_default;

            cell.is_default = true;

            if (was_inserted)
            {
                if constexpr (std::is_same_v<KeyType, StringRef>)
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

                        if constexpr (std::is_same_v<ElementType, StringRef>)
                        {
                            if (!cell_was_default)
                            {
                                StringRef previous_value = container[cell.element_index];
                                arena.free(const_cast<char *>(previous_value.data), previous_value.size);
                            }
                        }
                    });
                }

                if (cell.key != key)
                {
                    if constexpr (std::is_same_v<KeyType, StringRef>)
                    {
                        char * data = const_cast<char *>(cell.key.data);
                        arena.free(data, cell.key.size);
                        cell.key = copyStringInArena(arena, key);
                    }
                    else
                        cell.key = key;
                }
            }

            setCellDeadline(cell, now);
        }
    }

    PaddedPODArray<KeyType> getCachedKeysImpl() const
    {
        PaddedPODArray<KeyType> result;
        result.reserve(size);

        for (auto & cell : cells)
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
        std::vector<ValueType>,
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
            ContainerType<StringRef>,
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
                else if constexpr (std::is_same_v<ValueType, StringRef>)
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
    };

    CacheDictionaryStorageConfiguration configuration;

    pcg64 rnd_engine;

    size_t size_overlap_mask = 0;

    size_t size = 0;

    PaddedPODArray<Cell> cells;

    ArenaWithFreeLists arena;

    std::vector<Attribute> attributes;

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

    size_t getCellIndex(const KeyType key) const
    {
        const size_t hash = DefaultHash<KeyType>()(key);
        const size_t index = hash & size_overlap_mask;
        return index;
    }

    using KeyStateAndCellIndex = std::pair<KeyState::State, size_t>;

    KeyStateAndCellIndex getKeyStateAndCellIndex(const KeyType key, const time_t now) const
    {
        size_t place_value = getCellIndex(key);
        const size_t place_value_end = place_value + max_collision_length;

        time_t max_lifetime_seconds = static_cast<time_t>(configuration.strict_max_lifetime_seconds);

        for (; place_value < place_value_end; ++place_value)
        {
            const auto cell_place_value = place_value & size_overlap_mask;
            const auto & cell = cells[cell_place_value];

            if (cell.key != key)
                continue;

            if (unlikely(now > cell.deadline + max_lifetime_seconds))
                return std::make_pair(KeyState::not_found, cell_place_value);

            if (unlikely(now > cell.deadline))
                return std::make_pair(KeyState::expired, cell_place_value);

            return std::make_pair(KeyState::found, cell_place_value);
        }

        return std::make_pair(KeyState::not_found, place_value & size_overlap_mask);
    }

    size_t getCellIndexForInsert(const KeyType & key) const
    {
        size_t place_value = getCellIndex(key);
        const size_t place_value_end = place_value + max_collision_length;
        size_t oldest_place_value = place_value;

        time_t oldest_time = std::numeric_limits<time_t>::max();

        for (; place_value < place_value_end; ++place_value)
        {
            const size_t cell_place_value = place_value & size_overlap_mask;
            const Cell cell = cells[cell_place_value];

            if (cell.deadline == 0)
                return cell_place_value;

            if (cell.key == key)
                return cell_place_value;

            if (cell.deadline < oldest_time)
            {
                oldest_time = cell.deadline;
                oldest_place_value = cell_place_value;
            }
        }

        return oldest_place_value;
    }
};

}
