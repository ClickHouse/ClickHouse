#include "HashedDictionary.h"

#include <Core/Defines.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>

#include <Dictionaries/DictionaryBlockInputStream.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/HierarchyDictionariesUtils.h>

namespace
{

/// NOTE: Trailing return type is explicitly specified for SFINAE.

/// google::sparse_hash_map
template <typename T> auto getKeyFromCell(const T & value) -> decltype(value->first) { return value->first; } // NOLINT
template <typename T> auto getValueFromCell(const T & value) -> decltype(value->second) { return value->second; } // NOLINT

/// HashMap
template <typename T> auto getKeyFromCell(const T & value) -> decltype(value->getKey()) { return value->getKey(); } // NOLINT
template <typename T> auto getValueFromCell(const T & value) -> decltype(value->getMapped()) { return value->getMapped(); } // NOLINT

}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
HashedDictionary<dictionary_key_type, sparse>::HashedDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    bool require_nonempty_,
    BlockPtr previously_loaded_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
    , previously_loaded_block(std::move(previously_loaded_block_))
{
    createAttributes();
    loadData();
    calculateBytesAllocated();
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnPtr HashedDictionary<dictionary_key_type, sparse>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types [[maybe_unused]],
    const ColumnPtr & default_values_column) const
{
    if (dictionary_key_type == DictionaryKeyType::complex)
        dict_struct.validateKeyTypes(key_types);

    ColumnPtr result;

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    const size_t size = extractor.getKeysSize();

    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);
    const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    auto & attribute = attributes[attribute_index];

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;
    if (attribute.is_nullable_set)
    {
        col_null_map_to = ColumnUInt8::create(size, false);
        vec_null_map_to = &col_null_map_to->getData();
    }

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;

        const auto attribute_null_value = std::get<ValueType>(attribute.null_values);
        AttributeType null_value = static_cast<AttributeType>(attribute_null_value);
        DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(std::move(null_value), default_values_column);

        auto column = ColumnProvider::getColumn(dictionary_attribute, size);

        if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();

            getItemsImpl<ValueType>(
                attribute,
                extractor,
                [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
                [&](const size_t row)
                {
                    out->insertDefault();
                    (*vec_null_map_to)[row] = true;
                },
                default_value_extractor);
        }
        else
        {
            auto & out = column->getData();

            getItemsImpl<ValueType>(
                attribute,
                extractor,
                [&](const size_t row, const auto value) { return out[row] = value; },
                [&](const size_t row)
                {
                    out[row] = ValueType();
                    (*vec_null_map_to)[row] = true;
                },
                default_value_extractor);
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    if (attribute.is_nullable_set)
        result = ColumnNullable::create(result, std::move(col_null_map_to));

    return result;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnUInt8::Ptr HashedDictionary<dictionary_key_type, sparse>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    if (dictionary_key_type == DictionaryKeyType::complex)
        dict_struct.validateKeyTypes(key_types);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    size_t keys_size = extractor.getKeysSize();

    auto result = ColumnUInt8::create(keys_size, false);
    auto & out = result->getData();

    if (attributes.empty())
    {
        query_count.fetch_add(keys_size, std::memory_order_relaxed);
        return result;
    }

    const auto & attribute = attributes.front();
    bool is_attribute_nullable = attribute.is_nullable_set.has_value();

    getAttributeContainer(0, [&](const auto & container)
    {
        for (size_t requested_key_index = 0; requested_key_index < keys_size; ++requested_key_index)
        {
            auto requested_key = extractor.extractCurrentKey();

            out[requested_key_index] = container.find(requested_key) != container.end();

            if (is_attribute_nullable && !out[requested_key_index])
                out[requested_key_index] = attribute.is_nullable_set->find(requested_key) != nullptr;

            extractor.rollbackCurrentKey();
        }
    });

    query_count.fetch_add(keys_size, std::memory_order_relaxed);

    return result;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnPtr HashedDictionary<dictionary_key_type, sparse>::getHierarchy(ColumnPtr key_column [[maybe_unused]], const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        const UInt64 null_value = dictionary_attribute.null_value.template get<UInt64>();
        const CollectionType<UInt64> & parent_keys_map = std::get<CollectionType<UInt64>>(hierarchical_attribute.container);

        auto is_key_valid_func = [&](auto & key) { return parent_keys_map.find(key) != parent_keys_map.end(); };

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            auto it = parent_keys_map.find(hierarchy_key);

            if (it != parent_keys_map.end())
                result = getValueFromCell(it);

            return result;
        };

        auto dictionary_hierarchy_array = getKeysHierarchyArray(keys, null_value, is_key_valid_func, get_parent_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);

        return dictionary_hierarchy_array;
    }
    else
        return nullptr;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnUInt8::Ptr HashedDictionary<dictionary_key_type, sparse>::isInHierarchy(
    ColumnPtr key_column [[maybe_unused]],
    ColumnPtr in_key_column [[maybe_unused]],
    const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        PaddedPODArray<UInt64> keys_in_backup_storage;
        const auto & keys_in = getColumnVectorData(this, in_key_column, keys_in_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        const UInt64 null_value = dictionary_attribute.null_value.template get<UInt64>();
        const CollectionType<UInt64> & parent_keys_map = std::get<CollectionType<UInt64>>(hierarchical_attribute.container);

        auto is_key_valid_func = [&](auto & key) { return parent_keys_map.find(key) != parent_keys_map.end(); };

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            auto it = parent_keys_map.find(hierarchy_key);

            if (it != parent_keys_map.end())
                result = getValueFromCell(it);

            return result;
        };

        auto result = getKeysIsInHierarchyColumn(keys, keys_in, null_value, is_key_valid_func, get_parent_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);

        return result;
    }
    else
        return nullptr;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnPtr HashedDictionary<dictionary_key_type, sparse>::getDescendants(
    ColumnPtr key_column [[maybe_unused]],
    const DataTypePtr &,
    size_t level [[maybe_unused]]) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        PaddedPODArray<UInt64> keys_backup;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];
        const CollectionType<UInt64> & parent_keys = std::get<CollectionType<UInt64>>(hierarchical_attribute.container);

        HashMap<UInt64, PaddedPODArray<UInt64>> parent_to_child;

        for (const auto & [key, value] : parent_keys)
            parent_to_child[value].emplace_back(key);

        auto result = getKeysDescendantsArray(keys, parent_to_child, level);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);

        return result;
    }
    else
        return nullptr;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & dictionary_attribute : dict_struct.attributes)
    {
        auto type_call = [&, this](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            auto is_nullable_set = dictionary_attribute.is_nullable ? std::make_optional<NullableSet>() : std::optional<NullableSet>{};
            std::unique_ptr<Arena> string_arena = std::is_same_v<AttributeType, String> ? std::make_unique<Arena>() : nullptr;

            ValueType default_value;

            if constexpr (std::is_same_v<ValueType, StringRef>)
            {
                string_arena = std::make_unique<Arena>();

                const auto & string_null_value = dictionary_attribute.null_value.template get<String>();
                const size_t string_null_value_size = string_null_value.size();

                const char * string_in_arena = string_arena->insert(string_null_value.data(), string_null_value_size);
                default_value = {string_in_arena, string_null_value_size};
            }
            else
                default_value = dictionary_attribute.null_value.template get<NearestFieldType<ValueType>>();

            Attribute attribute{dictionary_attribute.underlying_type, std::move(is_nullable_set), default_value, CollectionType<ValueType>(), std::move(string_arena)};
            attributes.emplace_back(std::move(attribute));
        };

        callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::updateData()
{
    if (!previously_loaded_block || previously_loaded_block->rows() == 0)
    {
        auto stream = source_ptr->loadUpdatedAll();
        stream->readPrefix();

        while (const auto block = stream->read())
        {
            /// We are using this to keep saved data if input stream consists of multiple blocks
            if (!previously_loaded_block)
                previously_loaded_block = std::make_shared<DB::Block>(block.cloneEmpty());

            for (const auto attribute_idx : ext::range(0, attributes.size() + 1))
            {
                const IColumn & update_column = *block.getByPosition(attribute_idx).column.get();
                MutableColumnPtr saved_column = previously_loaded_block->getByPosition(attribute_idx).column->assumeMutable();
                saved_column->insertRangeFrom(update_column, 0, update_column.size());
            }
        }
        stream->readSuffix();
    }
    else
    {
        auto stream = source_ptr->loadUpdatedAll();
        mergeBlockWithStream<dictionary_key_type>(
            dict_struct.getKeysSize(),
            *previously_loaded_block,
            stream);
    }

    if (previously_loaded_block)
    {
        resize(previously_loaded_block->rows());
        blockToAttributes(*previously_loaded_block.get());
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::blockToAttributes(const Block & block [[maybe_unused]])
{
    size_t skip_keys_size_offset = dict_struct.getKeysSize();

    Columns key_columns;
    key_columns.reserve(skip_keys_size_offset);

    /// Split into keys columns and attribute columns
    for (size_t i = 0; i < skip_keys_size_offset; ++i)
        key_columns.emplace_back(block.safeGetByPosition(i).column);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();

    Field column_value_to_insert;

    for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
    {
        const IColumn & attribute_column = *block.safeGetByPosition(skip_keys_size_offset + attribute_index).column;
        auto & attribute = attributes[attribute_index];
        bool attribute_is_nullable = attribute.is_nullable_set.has_value();

        getAttributeContainer(attribute_index, [&](auto & container)
        {
            using ContainerType = std::decay_t<decltype(container)>;
            using AttributeValueType = typename ContainerType::mapped_type;

            for (size_t key_index = 0; key_index < keys_size; ++key_index)
            {
                auto key = keys_extractor.extractCurrentKey();

                auto it = container.find(key);
                bool key_is_nullable_and_already_exists = attribute_is_nullable && attribute.is_nullable_set->find(key) != nullptr;

                if (key_is_nullable_and_already_exists || it != container.end())
                {
                    keys_extractor.rollbackCurrentKey();
                    continue;
                }

                if constexpr (std::is_same_v<KeyType, StringRef>)
                    key = copyKeyInArena(key);

                attribute_column.get(key_index, column_value_to_insert);

                if (attribute.is_nullable_set && column_value_to_insert.isNull())
                {
                    attribute.is_nullable_set->insert(key);
                    keys_extractor.rollbackCurrentKey();
                    continue;
                }

                if constexpr (std::is_same_v<AttributeValueType, StringRef>)
                {
                    String & value_to_insert = column_value_to_insert.get<String>();
                    size_t value_to_insert_size = value_to_insert.size();

                    const char * string_in_arena = attribute.string_arena->insert(value_to_insert.data(), value_to_insert_size);

                    StringRef string_in_arena_reference = StringRef{string_in_arena, value_to_insert_size};
                    container.insert({key, string_in_arena_reference});
                }
                else
                {
                    auto value_to_insert = column_value_to_insert.get<NearestFieldType<AttributeValueType>>();
                    container.insert({key, value_to_insert});
                }

                ++element_count;

                keys_extractor.rollbackCurrentKey();
            }

            keys_extractor.reset();
        });
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::resize(size_t added_rows)
{
    if (unlikely(!added_rows))
        return;

    for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
    {
        getAttributeContainer(attribute_index, [added_rows](auto & attribute_map)
        {
            size_t reserve_size = added_rows + attribute_map.size();

            if constexpr (sparse)
                attribute_map.resize(reserve_size);
            else
                attribute_map.reserve(reserve_size);
        });
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
template <typename AttributeType, typename ValueSetter, typename NullableValueSetter, typename DefaultValueExtractor>
void HashedDictionary<dictionary_key_type, sparse>::getItemsImpl(
    const Attribute & attribute,
    DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
    ValueSetter && set_value [[maybe_unused]],
    NullableValueSetter && set_nullable_value [[maybe_unused]],
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & attribute_container = std::get<CollectionType<AttributeType>>(attribute.container);
    const size_t keys_size = keys_extractor.getKeysSize();

    bool is_attribute_nullable = attribute.is_nullable_set.has_value();

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();

        const auto it = attribute_container.find(key);

        if (it != attribute_container.end())
            set_value(key_index, getValueFromCell(it));
        else
        {
            if (is_attribute_nullable && attribute.is_nullable_set->find(key) != nullptr)
                set_nullable_value(key_index);
            else
                set_value(key_index, default_value_extractor[key_index]);
        }

        keys_extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
StringRef HashedDictionary<dictionary_key_type, sparse>::copyKeyInArena(StringRef key)
{
    size_t key_size = key.size;
    char * place_for_key = complex_key_arena.alloc(key_size);
    memcpy(reinterpret_cast<void *>(place_for_key), reinterpret_cast<const void *>(key.data), key_size);
    StringRef updated_key{place_for_key, key_size};
    return updated_key;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        auto stream = source_ptr->loadAll();

        stream->readPrefix();

        while (const auto block = stream->read())
        {
            resize(block.rows());
            blockToAttributes(block);
        }

        stream->readSuffix();
    }
    else
        updateData();

    if (require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "{}: dictionary source is empty and 'require_nonempty' property is set.",
            full_name);
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (size_t i = 0; i < attributes.size(); ++i)
    {
        getAttributeContainer(i, [&](const auto & container)
        {
            using ContainerType = std::decay_t<decltype(container)>;
            using AttributeValueType = typename ContainerType::mapped_type;

            bytes_allocated += sizeof(container);

            if constexpr (sparse || std::is_same_v<AttributeValueType, Field>)
            {
                /// bucket_count() - Returns table size, that includes empty and deleted
                /// size()         - Returns table size, w/o empty and deleted
                /// and since this is sparsehash, empty cells should not be significant,
                /// and since items cannot be removed from the dictionary, deleted is also not important.
                bytes_allocated += container.size() * (sizeof(KeyType) + sizeof(AttributeValueType));
                bucket_count = container.bucket_count();
            }
            else
            {
                bytes_allocated += container.getBufferSizeInBytes();
                bucket_count = container.getBufferSizeInCells();
            }
        });

        if (attributes[i].string_arena)
            bytes_allocated += attributes[i].string_arena->size();
    }

    bytes_allocated += complex_key_arena.size();
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
BlockInputStreamPtr HashedDictionary<dictionary_key_type, sparse>::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    PaddedPODArray<HashedDictionary::KeyType> keys;

    if (!attributes.empty())
    {
        const auto & attribute = attributes.front();

        getAttributeContainer(0, [&](auto & container)
        {
            keys.reserve(container.size());

            for (const auto & [key, value] : container)
            {
                (void)(value);
                keys.emplace_back(key);
            }

            if (attribute.is_nullable_set)
            {
                const auto & is_nullable_set = *attribute.is_nullable_set;
                keys.reserve(is_nullable_set.size());

                for (auto & node : is_nullable_set)
                    keys.emplace_back(node.getKey());
            }
        });
    }

    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
        return std::make_shared<DictionaryBlockInputStream>(shared_from_this(), max_block_size, std::move(keys), column_names);
    else
        return std::make_shared<DictionaryBlockInputStream>(shared_from_this(), max_block_size, keys, column_names);
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
template <typename GetContainerFunc>
void HashedDictionary<dictionary_key_type, sparse>::getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func)
{
    assert(attribute_index < attributes.size());

    auto & attribute = attributes[attribute_index];

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        auto & attribute_container = std::get<CollectionType<ValueType>>(attribute.container);
        std::forward<GetContainerFunc>(get_container_func)(attribute_container);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
template <typename GetContainerFunc>
void HashedDictionary<dictionary_key_type, sparse>::getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func) const
{
    const_cast<std::decay_t<decltype(*this)> *>(this)->getAttributeContainer(attribute_index, [&](auto & attribute_container)
    {
        std::forward<GetContainerFunc>(get_container_func)(attribute_container);
    });
}

template class HashedDictionary<DictionaryKeyType::simple, true>;
template class HashedDictionary<DictionaryKeyType::simple, false>;
template class HashedDictionary<DictionaryKeyType::complex, true>;
template class HashedDictionary<DictionaryKeyType::complex, false>;

void registerDictionaryHashed(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             DictionaryKeyType dictionary_key_type,
                             bool sparse) -> DictionaryPtr
    {
        if (dictionary_key_type == DictionaryKeyType::simple && dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for simple key hashed dictionary");
        else if (dictionary_key_type == DictionaryKeyType::complex && dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for complex key hashed dictionary");

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: elements .structure.range_min and .structure.range_max should be defined only "
                "for a dictionary of layout 'range_hashed'",
                full_name);

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

        if (dictionary_key_type == DictionaryKeyType::simple)
        {
            if (sparse)
                return std::make_unique<HashedDictionary<DictionaryKeyType::simple, true>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
            else
                return std::make_unique<HashedDictionary<DictionaryKeyType::simple, false>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
        }
        else
        {
            if (sparse)
                return std::make_unique<HashedDictionary<DictionaryKeyType::complex, true>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
            else
                return std::make_unique<HashedDictionary<DictionaryKeyType::complex, false>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
        }
    };

    using namespace std::placeholders;

    factory.registerLayout("hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::simple, /* sparse = */ false); }, false);
    factory.registerLayout("sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::simple, /* sparse = */ true); }, false);
    factory.registerLayout("complex_key_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::complex, /* sparse = */ false); }, true);
    factory.registerLayout("complex_key_sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::complex, /* sparse = */ true); }, true);

}

}
