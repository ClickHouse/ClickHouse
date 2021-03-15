#pragma once

#include <chrono>
#include <variant>

#include <pcg_random.hpp>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <Common/randomSeed.h>
#include <Common/Arena.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/HashTable/LRUHashMap.h>
#include <Common/HashTable/FixedDeadlineHashMap.h>
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

/// TODO: Add documentation
template <DictionaryKeyType dictionary_key_type>
class CacheDictionaryStorage final : public ICacheDictionaryStorage
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, UInt64, StringRef>;
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by CacheDictionaryStorage");

    explicit CacheDictionaryStorage(
        const DictionaryStructure & dictionary_structure,
        CacheDictionaryStorageConfiguration & configuration_)
        : configuration(configuration_)
        , rnd_engine(randomSeed())
        , cache(configuration.max_size_in_cells, 10, { *this })
    {
        setup(dictionary_structure);
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
            return fetchColumnsForKeysImpl<SimpleKeysStorageFetchResult>(keys, fetch_request);
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
            return fetchColumnsForKeysImpl<ComplexKeysStorageFetchResult>(keys, column_fetch_requests);
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

        return arena.size() + cache.getSizeInBytes() + attributes_size_in_bytes;
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
        const DictionaryStorageFetchRequest & fetch_request)
    {
        KeysStorageFetchResult result;

        result.fetched_columns = fetch_request.makeAttributesResultColumns();
        result.key_index_to_state.resize_fill(keys.size(), {KeyState::not_found});

        const auto now = std::chrono::system_clock::now();

        size_t fetched_columns_index = 0;
        size_t keys_size = keys.size();

        std::chrono::seconds max_lifetime_seconds(configuration.strict_max_lifetime_seconds);

        PaddedPODArray<FetchedKey> fetched_keys;
        fetched_keys.resize_fill(keys_size);

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys[key_index];
            auto * it = cache.get(key);

            if (!it)
            {
                result.key_index_to_state[key_index] = {KeyState::not_found};
                ++result.not_found_keys_size;
                continue;
            }

            auto deadline = it->getDeadline();
            const auto & cell = it->getMapped();

            if (now > deadline + max_lifetime_seconds)
            {
                result.key_index_to_state[key_index] = {KeyState::not_found};
                ++result.not_found_keys_size;
                continue;
            }

            bool cell_is_expired = false;
            KeyState::State key_state = KeyState::found;

            if (now > deadline)
            {
                cell_is_expired = true;
                key_state = KeyState::expired;
            }

            result.key_index_to_state[key_index] = {key_state, fetched_columns_index};
            ++fetched_columns_index;

            result.expired_keys_size += cell_is_expired;
            result.found_keys_size += !cell_is_expired;

            result.key_index_to_state[key_index].setDefaultValue(cell.is_default);
            result.default_keys_size += cell.is_default;

            fetched_keys[key_index] = FetchedKey{cell.element_index, cell.is_default};
        }

        for (size_t attribute_index = 0; attribute_index < fetch_request.attributesSize(); ++attribute_index)
        {
            if (!fetch_request.shouldFillResultColumnWithIndex(attribute_index))
                continue;

            size_t fetched_keys_size = fetched_keys.size();
            auto & attribute = attributes[attribute_index];
            const auto & default_value_provider = fetch_request.defaultValueProviderAtIndex(attribute_index);
            auto & fetched_column = *result.fetched_columns[attribute_index];
            fetched_column.reserve(fetched_keys_size);

            if (unlikely(attribute.is_complex_type))
            {
                auto & container = std::get<std::vector<Field>>(attribute.attribute_container);

                for (size_t fetched_key_index = 0; fetched_key_index < fetched_keys.size(); ++fetched_key_index)
                {
                    auto fetched_key = fetched_keys[fetched_key_index];

                    if (unlikely(fetched_key.is_default))
                        fetched_column.insert(default_value_provider.getDefaultValue(fetched_key_index));
                    else
                        fetched_column.insert(container[fetched_key.element_index]);
                }
            }
            else
            {
                auto type_call = [&](const auto & dictionary_attribute_type)
                {
                    using Type = std::decay_t<decltype(dictionary_attribute_type)>;
                    using AttributeType = typename Type::AttributeType;
                    using ValueType = DictionaryValueType<AttributeType>;
                    using ColumnType =
                        std::conditional_t<std::is_same_v<AttributeType, String>, ColumnString,
                            std::conditional_t<IsDecimalNumber<AttributeType>, ColumnDecimal<ValueType>,
                                ColumnVector<AttributeType>>>;

                    auto & container = std::get<PaddedPODArray<ValueType>>(attribute.attribute_container);
                    ColumnType & column_typed = static_cast<ColumnType &>(fetched_column);

                    if constexpr (std::is_same_v<ColumnType, ColumnString>)
                    {
                        for (size_t fetched_key_index = 0; fetched_key_index < fetched_keys.size(); ++fetched_key_index)
                        {
                            auto fetched_key = fetched_keys[fetched_key_index];

                            if (unlikely(fetched_key.is_default))
                                column_typed.insert(default_value_provider.getDefaultValue(fetched_key_index));
                            else
                            {
                                auto item = container[fetched_key.element_index];
                                column_typed.insertData(item.data, item.size);
                            }
                        }
                    }
                    else
                    {
                        for (size_t fetched_key_index = 0; fetched_key_index < fetched_keys.size(); ++fetched_key_index)
                        {
                            auto fetched_key = fetched_keys[fetched_key_index];
                            auto & data = column_typed.getData();

                            if (unlikely(fetched_key.is_default))
                                column_typed.insert(default_value_provider.getDefaultValue(fetched_key_index));
                            else
                            {
                                auto item = container[fetched_key.element_index];
                                data.push_back(item);
                            }
                        }
                    }
                };

                callOnDictionaryAttributeType(attribute.type, type_call);
            }
        }

        return result;
    }

    void insertColumnsForKeysImpl(const PaddedPODArray<KeyType> & keys, Columns columns)
    {
        const auto now = std::chrono::system_clock::now();

        size_t keys_size = keys.size();

        size_t columns_size = columns.size();
        Field column_value;

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys[key_index];

            auto [it, was_inserted] = cache.insert(key, {});

            if (was_inserted)
            {
                auto & cell = it->getMapped();
                cell.is_default = false;

                for (size_t attribute_index = 0; attribute_index < columns_size; ++attribute_index)
                {
                    auto & column = columns[attribute_index];

                    getAttributeContainer(attribute_index, [&](auto & container)
                    {
                        container.emplace_back();
                        cell.element_index = container.size() - 1;

                        using ElementType = std::decay_t<decltype(container[0])>;

                        column->get(key_index, column_value);

                        if constexpr (std::is_same_v<ElementType, Field>)
                            container.back() = column_value;
                        else if constexpr (std::is_same_v<ElementType, StringRef>)
                        {
                            const String & value = column_value.get<String>();
                            StringRef inserted_value = copyStringInArena(StringRef { value.data(), value.size() });
                            container.back() = inserted_value;
                        }
                        else
                            container.back() = column_value.get<ElementType>();
                    });
                }
            }
            else
            {
                auto & cell_key = it->getKey();

                Cell cell;

                size_t existing_index = it->getMapped().element_index;

                cell.element_index = existing_index;
                cell.is_default = false;

                if (cell_key != key)
                {
                    /// In case of complex key we keep it in arena
                    if constexpr (std::is_same_v<KeyType, StringRef>)
                        arena.free(const_cast<char *>(key.data), key.size);
                }

                cache.reinsert(it, key, cell);

                /// Put values into index

                for (size_t attribute_index = 0; attribute_index < columns_size; ++attribute_index)
                {
                    auto & column = columns[attribute_index];

                    getAttributeContainer(attribute_index, [&](auto & container)
                    {
                        using ElementType = std::decay_t<decltype(container[0])>;

                        column->get(key_index, column_value);

                        if constexpr (std::is_same_v<ElementType, Field>)
                            container[existing_index] = column_value;
                        else if constexpr (std::is_same_v<ElementType, StringRef>)
                        {
                            const String & value = column_value.get<String>();
                            StringRef inserted_value = copyStringInArena(StringRef { value.data(), value.size() });
                            container[existing_index] = inserted_value;
                        }
                        else
                            container[existing_index] = column_value.get<ElementType>();
                    });
                }
            }

            setCellDeadline(*it, now);
        }
    }

    void insertDefaultKeysImpl(const PaddedPODArray<KeyType> & keys)
    {
        const auto now = std::chrono::system_clock::now();

        size_t keys_size = keys.size();

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys[key_index];

            Cell value;
            value.is_default = true;

            auto [it, was_inserted] = cache.insert(key, value);

            if (was_inserted)
            {
                auto & cell = it->getMapped();

                for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
                {
                    getAttributeContainer(attribute_index, [&](auto & container)
                    {
                        container.emplace_back();
                        cell.element_index = container.size();
                    });
                }
            }
            else
            {
                value.element_index = it->getMapped().element_index;

                if (it->getKey() != key)
                {
                    /// In case of complex key we keep it in arena
                    if constexpr (std::is_same_v<KeyType, StringRef>)
                        arena.free(const_cast<char *>(key.data), key.size);
                }

                cache.reinsert(it, key, value);
            }

            setCellDeadline(*it, now);
        }
    }

    PaddedPODArray<KeyType> getCachedKeysImpl() const
    {
        PaddedPODArray<KeyType> result;
        result.reserve(cache.size());

        for (auto & node : cache)
        {
            auto & cell = node.getMapped();

            if (cell.is_default)
                continue;

            result.emplace_back(node.getKey());
        }

        return result;
    }

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && func)
    {
        auto & attribute = attributes[attribute_index];
        auto & attribute_type = attribute.type;

        if (unlikely(attribute.is_complex_type))
        {
            auto & container = std::get<std::vector<Field>>(attribute.attribute_container);
            std::forward<GetContainerFunc>(func)(container);
        }
        else
        {
            auto type_call = [&](const auto & dictionary_attribute_type)
            {
                using Type = std::decay_t<decltype(dictionary_attribute_type)>;
                using AttributeType = typename Type::AttributeType;
                using ValueType = DictionaryValueType<AttributeType>;

                auto & container = std::get<PaddedPODArray<ValueType>>(attribute.attribute_container);
                std::forward<GetContainerFunc>(func)(container);
            };

            callOnDictionaryAttributeType(attribute_type, type_call);
        }
    }

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && func) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->template getAttributeContainer(attribute_index, std::forward<GetContainerFunc>(func));
    }

    StringRef copyStringInArena(StringRef value_to_copy)
    {
        size_t value_to_copy_size = value_to_copy.size;
        char * place_for_key = arena.alloc(value_to_copy_size);
        memcpy(reinterpret_cast<void *>(place_for_key), reinterpret_cast<const void *>(value_to_copy.data), value_to_copy_size);
        StringRef updated_value{place_for_key, value_to_copy_size};

        return updated_value;
    }

    void setup(const DictionaryStructure & dictionary_structure)
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
                last_attribute.is_complex_type = dictionary_attribute.is_nullable || dictionary_attribute.is_array;

                if (dictionary_attribute.is_nullable)
                    last_attribute.attribute_container = std::vector<Field>();
                else
                    last_attribute.attribute_container = PaddedPODArray<ValueType>();
            };

            callOnDictionaryAttributeType(attribute_type, type_call);
        }
    }

    struct Cell
    {
        size_t element_index;
        bool is_default;
    };

    CacheDictionaryStorageConfiguration configuration;

    ArenaWithFreeLists arena;

    pcg64 rnd_engine;

    struct Attribute
    {
        AttributeUnderlyingType type;
        bool is_complex_type;

        std::variant<
            PaddedPODArray<UInt8>,
            PaddedPODArray<UInt16>,
            PaddedPODArray<UInt32>,
            PaddedPODArray<UInt64>,
            PaddedPODArray<UInt128>,
            PaddedPODArray<Int8>,
            PaddedPODArray<Int16>,
            PaddedPODArray<Int32>,
            PaddedPODArray<Int64>,
            PaddedPODArray<Decimal32>,
            PaddedPODArray<Decimal64>,
            PaddedPODArray<Decimal128>,
            PaddedPODArray<Float32>,
            PaddedPODArray<Float64>,
            PaddedPODArray<StringRef>,
            std::vector<Field>> attribute_container;
    };

    class CacheStorageCellDisposer
    {
    public:
        CacheDictionaryStorage & storage;

        template <typename Key, typename Value>
        void operator()(const Key & key, const Value &) const
        {
            /// In case of complex key we keep it in arena
            if constexpr (std::is_same_v<Key, StringRef>)
                storage.arena.free(const_cast<char *>(key.data), key.size);
        }
    };

    using SimpleFixedDeadlineHashMap = FixedDeadlineHashMap<UInt64, Cell, CacheStorageCellDisposer>;
    using ComplexFixedDeadlineHashMap = FixedDeadlineHashMap<StringRef, Cell, CacheStorageCellDisposer>;

    using FixedDeadlineHashMap = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::simple,
        SimpleFixedDeadlineHashMap,
        ComplexFixedDeadlineHashMap>;

    using FixedDeadlineHashMapCell = typename FixedDeadlineHashMap::Cell;

    inline void setCellDeadline(FixedDeadlineHashMapCell & cell, TimePoint now)
    {
        if (configuration.lifetime.min_sec == 0 && configuration.lifetime.max_sec == 0)
        {
            /// This maybe not obvious, but when we define is this cell is expired or expired permanently, we add strict_max_lifetime_seconds
            /// to the expiration time. And it overflows pretty well.
            auto deadline = std::chrono::time_point<std::chrono::system_clock>::max() - 2 * std::chrono::seconds(configuration.strict_max_lifetime_seconds);
            cell.setDeadline(deadline);
            return;
        }

        size_t min_sec_lifetime = configuration.lifetime.min_sec;
        size_t max_sec_lifetime = configuration.lifetime.max_sec;

        std::uniform_int_distribution<UInt64> distribution{min_sec_lifetime, max_sec_lifetime};

        auto deadline = now + std::chrono::seconds(distribution(rnd_engine));
        cell.setDeadline(deadline);
    }

    FixedDeadlineHashMap cache;

    std::vector<Attribute> attributes;
};

}
