#include "HashedArrayDictionary.h"

#include <Common/ArenaUtils.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>

#include <Dictionaries/DictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/HierarchyDictionariesUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
}

template <DictionaryKeyType dictionary_key_type>
HashedArrayDictionary<dictionary_key_type>::HashedArrayDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const HashedArrayDictionaryStorageConfiguration & configuration_,
    BlockPtr update_field_loaded_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , configuration(configuration_)
    , update_field_loaded_block(std::move(update_field_loaded_block_))
{
    createAttributes();
    loadData();
    buildHierarchyParentToChildIndexIfNeeded();
    calculateBytesAllocated();
}

template <DictionaryKeyType dictionary_key_type>
ColumnPtr HashedArrayDictionary<dictionary_key_type>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types [[maybe_unused]],
    const ColumnPtr & default_values_column) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    ColumnPtr result;

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    const size_t keys_size = extractor.getKeysSize();

    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);
    const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    auto & attribute = attributes[attribute_index];

    return getAttributeColumn(attribute, dictionary_attribute, keys_size, default_values_column, extractor);
}

template <DictionaryKeyType dictionary_key_type>
Columns HashedArrayDictionary<dictionary_key_type>::getColumns(
    const Strings & attribute_names,
    const DataTypes & result_types,
    const Columns & key_columns,
    const DataTypes & key_types,
    const Columns & default_values_columns) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    const size_t keys_size = extractor.getKeysSize();

    PaddedPODArray<ssize_t> key_index_to_element_index;

    /** Optimization for multiple attributes.
      * For each key save element index in key_index_to_element_index array.
      * Later in type_call for attribute use getItemsImpl specialization with key_index_to_element_index array
      * instead of DictionaryKeyExtractor.
      */
    if (attribute_names.size() > 1)
    {
        const auto & key_attribute_container = key_attribute.container;
        size_t keys_found = 0;

        key_index_to_element_index.resize(keys_size);

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = extractor.extractCurrentKey();

            auto it = key_attribute_container.find(key);
            if (it == key_attribute_container.end())
            {
                key_index_to_element_index[key_index] = -1;
            }
            else
            {
                key_index_to_element_index[key_index] = it->getMapped();
                ++keys_found;
            }

            extractor.rollbackCurrentKey();
        }

        query_count.fetch_add(keys_size, std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);
    }

    size_t attribute_names_size = attribute_names.size();

    Columns result_columns;
    result_columns.reserve(attribute_names_size);

    for (size_t i = 0; i < attribute_names_size; ++i)
    {
        ColumnPtr result_column;

        const auto & attribute_name = attribute_names[i];
        const auto & result_type = result_types[i];
        const auto & default_values_column = default_values_columns[i];

        const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);
        const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
        auto & attribute = attributes[attribute_index];

        if (attribute_names_size > 1)
            result_column = getAttributeColumn(attribute, dictionary_attribute, keys_size, default_values_column, key_index_to_element_index);
        else
            result_column = getAttributeColumn(attribute, dictionary_attribute, keys_size, default_values_column, extractor);

        result_columns.emplace_back(std::move(result_column));
    }

    return result_columns;
}

template <DictionaryKeyType dictionary_key_type>
ColumnUInt8::Ptr HashedArrayDictionary<dictionary_key_type>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    size_t keys_size = extractor.getKeysSize();

    auto result = ColumnUInt8::create(keys_size, false);
    auto & out = result->getData();

    size_t keys_found = 0;

    for (size_t requested_key_index = 0; requested_key_index < keys_size; ++requested_key_index)
    {
        auto requested_key = extractor.extractCurrentKey();

        out[requested_key_index] = key_attribute.container.find(requested_key) != key_attribute.container.end();

        keys_found += out[requested_key_index];
        extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

template <DictionaryKeyType dictionary_key_type>
ColumnPtr HashedArrayDictionary<dictionary_key_type>::getHierarchy(ColumnPtr key_column [[maybe_unused]], const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        std::optional<UInt64> null_value;

        if (!dictionary_attribute.null_value.isNull())
            null_value = dictionary_attribute.null_value.get<UInt64>();

        const auto & key_attribute_container = key_attribute.container;
        const AttributeContainerType<UInt64> & parent_keys_container = std::get<AttributeContainerType<UInt64>>(hierarchical_attribute.container);

        auto is_key_valid_func = [&](auto & key) { return key_attribute_container.find(key) != key_attribute_container.end(); };

        size_t keys_found = 0;

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            auto it = key_attribute_container.find(hierarchy_key);

            if (it == key_attribute_container.end())
                return result;

            size_t key_index = it->getMapped();

            if (unlikely(hierarchical_attribute.is_index_null) && (*hierarchical_attribute.is_index_null)[key_index])
                return result;

            UInt64 parent_key = parent_keys_container[key_index];
            if (null_value && *null_value == parent_key)
                return result;

            result = parent_key;
            keys_found += 1;

            return result;
        };

        auto dictionary_hierarchy_array = getKeysHierarchyArray(keys, is_key_valid_func, get_parent_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

        return dictionary_hierarchy_array;
    }
    else
    {
        return nullptr;
    }
}

template <DictionaryKeyType dictionary_key_type>
ColumnUInt8::Ptr HashedArrayDictionary<dictionary_key_type>::isInHierarchy(
    ColumnPtr key_column [[maybe_unused]],
    ColumnPtr in_key_column [[maybe_unused]],
    const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        PaddedPODArray<UInt64> keys_in_backup_storage;
        const auto & keys_in = getColumnVectorData(this, in_key_column, keys_in_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        std::optional<UInt64> null_value;

        if (!dictionary_attribute.null_value.isNull())
            null_value = dictionary_attribute.null_value.get<UInt64>();

        const auto & key_attribute_container = key_attribute.container;
        const AttributeContainerType<UInt64> & parent_keys_container = std::get<AttributeContainerType<UInt64>>(hierarchical_attribute.container);

        auto is_key_valid_func = [&](auto & key) { return key_attribute_container.find(key) != key_attribute_container.end(); };

        size_t keys_found = 0;

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            auto it = key_attribute_container.find(hierarchy_key);

            if (it == key_attribute_container.end())
                return result;

            size_t key_index = it->getMapped();

            if (unlikely(hierarchical_attribute.is_index_null) && (*hierarchical_attribute.is_index_null)[key_index])
                return result;

            UInt64 parent_key = parent_keys_container[key_index];
            if (null_value && *null_value == parent_key)
                return result;

            result = parent_key;
            keys_found += 1;

            return result;
        };

        auto result = getKeysIsInHierarchyColumn(keys, keys_in, is_key_valid_func, get_parent_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

        return result;
    }
    else
    {
        return nullptr;
    }
}

template <DictionaryKeyType dictionary_key_type>
DictionaryHierarchicalParentToChildIndexPtr HashedArrayDictionary<dictionary_key_type>::getHierarchicalIndex() const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        if (hierarchical_index)
            return hierarchical_index;

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;
        const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];
        const AttributeContainerType<UInt64> & parent_keys_container = std::get<AttributeContainerType<UInt64>>(hierarchical_attribute.container);

        const auto & key_attribute_container = key_attribute.container;

        HashMap<size_t, UInt64> index_to_key;
        index_to_key.reserve(key_attribute.container.size());

        for (auto & [key, value] : key_attribute_container)
            index_to_key[value] = key;

        HashMap<UInt64, PaddedPODArray<UInt64>> parent_to_child;
        parent_to_child.reserve(index_to_key.size());

        size_t parent_keys_container_size = parent_keys_container.size();
        for (size_t i = 0; i < parent_keys_container_size; ++i)
        {
            if (unlikely(hierarchical_attribute.is_index_null) && (*hierarchical_attribute.is_index_null)[i])
                continue;

            const auto * it = index_to_key.find(i);
            if (it == index_to_key.end())
                continue;

            auto child_key = it->getMapped();
            auto parent_key = parent_keys_container[i];
            parent_to_child[parent_key].emplace_back(child_key);
        }

        return std::make_shared<DictionaryHierarchicalParentToChildIndex>(parent_to_child);
    }
    else
    {
        return nullptr;
    }
}

template <DictionaryKeyType dictionary_key_type>
ColumnPtr HashedArrayDictionary<dictionary_key_type>::getDescendants(
    ColumnPtr key_column [[maybe_unused]],
    const DataTypePtr &,
    size_t level [[maybe_unused]],
    DictionaryHierarchicalParentToChildIndexPtr parent_to_child_index [[maybe_unused]]) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        PaddedPODArray<UInt64> keys_backup;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup);

        size_t keys_found = 0;
        auto result = getKeysDescendantsArray(keys, *parent_to_child_index, level, keys_found);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

        return result;
    }
    else
    {
        return nullptr;
    }
}

template <DictionaryKeyType dictionary_key_type>
void HashedArrayDictionary<dictionary_key_type>::createAttributes()
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

            auto is_index_null = dictionary_attribute.is_nullable ? std::make_optional<std::vector<bool>>() : std::optional<std::vector<bool>>{};
            Attribute attribute{dictionary_attribute.underlying_type, AttributeContainerType<ValueType>(), std::move(is_index_null)};
            attributes.emplace_back(std::move(attribute));
        };

        callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);
    }
}

template <DictionaryKeyType dictionary_key_type>
void HashedArrayDictionary<dictionary_key_type>::updateData()
{
    if (!update_field_loaded_block || update_field_loaded_block->rows() == 0)
    {
        QueryPipeline pipeline(source_ptr->loadUpdatedAll());

        PullingPipelineExecutor executor(pipeline);
        Block block;
        while (executor.pull(block))
        {
            /// We are using this to keep saved data if input stream consists of multiple blocks
            if (!update_field_loaded_block)
                update_field_loaded_block = std::make_shared<DB::Block>(block.cloneEmpty());

            for (size_t attribute_index = 0; attribute_index < block.columns(); ++attribute_index)
            {
                const IColumn & update_column = *block.getByPosition(attribute_index).column.get();
                MutableColumnPtr saved_column = update_field_loaded_block->getByPosition(attribute_index).column->assumeMutable();
                saved_column->insertRangeFrom(update_column, 0, update_column.size());
            }
        }
    }
    else
    {
        auto pipe = source_ptr->loadUpdatedAll();
        mergeBlockWithPipe<dictionary_key_type>(
            dict_struct.getKeysSize(),
            *update_field_loaded_block,
            std::move(pipe));
    }

    if (update_field_loaded_block)
    {
        resize(update_field_loaded_block->rows());
        blockToAttributes(*update_field_loaded_block.get());
    }
}

template <DictionaryKeyType dictionary_key_type>
void HashedArrayDictionary<dictionary_key_type>::blockToAttributes(const Block & block [[maybe_unused]])
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

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();

        auto it = key_attribute.container.find(key);

        if (it != key_attribute.container.end())
        {
            keys_extractor.rollbackCurrentKey();
            continue;
        }

        if constexpr (std::is_same_v<KeyType, StringRef>)
            key = copyStringInArena(string_arena, key);

        key_attribute.container.insert({key, element_count});

        for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
        {
            const IColumn & attribute_column = *block.safeGetByPosition(skip_keys_size_offset + attribute_index).column;
            auto & attribute = attributes[attribute_index];
            bool attribute_is_nullable = attribute.is_index_null.has_value();

            attribute_column.get(key_index, column_value_to_insert);

            auto type_call = [&](const auto & dictionary_attribute_type)
            {
                using Type = std::decay_t<decltype(dictionary_attribute_type)>;
                using AttributeType = typename Type::AttributeType;
                using AttributeValueType = DictionaryValueType<AttributeType>;

                auto & attribute_container = std::get<AttributeContainerType<AttributeValueType>>(attribute.container);
                attribute_container.emplace_back();

                if (attribute_is_nullable)
                {
                    attribute.is_index_null->emplace_back();

                    if (column_value_to_insert.isNull())
                    {
                        (*attribute.is_index_null).back() = true;
                        return;
                    }
                }

                if constexpr (std::is_same_v<AttributeValueType, StringRef>)
                {
                    String & value_to_insert = column_value_to_insert.get<String>();
                    StringRef string_in_arena_reference = copyStringInArena(string_arena, value_to_insert);
                    attribute_container.back() = string_in_arena_reference;
                }
                else
                {
                    auto value_to_insert = column_value_to_insert.get<NearestFieldType<AttributeValueType>>();
                    attribute_container.back() = value_to_insert;
                }
            };

            callOnDictionaryAttributeType(attribute.type, type_call);
        }

        ++element_count;
        keys_extractor.rollbackCurrentKey();
    }
}

template <DictionaryKeyType dictionary_key_type>
void HashedArrayDictionary<dictionary_key_type>::resize(size_t added_rows)
{
    if (unlikely(!added_rows))
        return;

    key_attribute.container.reserve(added_rows);
}

template <DictionaryKeyType dictionary_key_type>
template <typename KeysProvider>
ColumnPtr HashedArrayDictionary<dictionary_key_type>::getAttributeColumn(
    const Attribute & attribute,
    const DictionaryAttribute & dictionary_attribute,
    size_t keys_size,
    ColumnPtr default_values_column,
    KeysProvider && keys_object) const
{
    ColumnPtr result;

    bool is_attribute_nullable = attribute.is_index_null.has_value();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;
    if (attribute.is_index_null)
    {
        col_null_map_to = ColumnUInt8::create(keys_size, false);
        vec_null_map_to = &col_null_map_to->getData();
    }

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;

        DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(dictionary_attribute.null_value, default_values_column);

        auto column = ColumnProvider::getColumn(dictionary_attribute, keys_size);

        if constexpr (std::is_same_v<ValueType, Array>)
        {
            auto * out = column.get();

            getItemsImpl<ValueType, false>(
                attribute,
                keys_object,
                [&](const size_t, const Array & value, bool) { out->insert(value); },
                default_value_extractor);
        }
        else if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    keys_object,
                    [&](size_t row, StringRef value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out->insertData(value.data, value.size);
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    keys_object,
                    [&](size_t, StringRef value, bool) { out->insertData(value.data, value.size); },
                    default_value_extractor);
        }
        else
        {
            auto & out = column->getData();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    keys_object,
                    [&](size_t row, const auto value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out[row] = value;
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    keys_object,
                    [&](size_t row, const auto value, bool) { out[row] = value; },
                    default_value_extractor);
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    if (is_attribute_nullable)
        result = ColumnNullable::create(result, std::move(col_null_map_to));

    return result;
}

template <DictionaryKeyType dictionary_key_type>
template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void HashedArrayDictionary<dictionary_key_type>::getItemsImpl(
    const Attribute & attribute,
    DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
    ValueSetter && set_value [[maybe_unused]],
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & key_attribute_container = key_attribute.container;
    const auto & attribute_container = std::get<AttributeContainerType<AttributeType>>(attribute.container);
    const size_t keys_size = keys_extractor.getKeysSize();

    size_t keys_found = 0;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();

        const auto it = key_attribute_container.find(key);

        if (it != key_attribute_container.end())
        {
            size_t element_index = it->getMapped();

            const auto & element = attribute_container[element_index];

            if constexpr (is_nullable)
                set_value(key_index, element, (*attribute.is_index_null)[element_index]);
            else
                set_value(key_index, element, false);

            ++keys_found;
        }
        else
        {
            if constexpr (is_nullable)
                set_value(key_index, default_value_extractor[key_index], default_value_extractor.isNullAt(key_index));
            else
                set_value(key_index, default_value_extractor[key_index], false);
        }

        keys_extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type>
template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void HashedArrayDictionary<dictionary_key_type>::getItemsImpl(
    const Attribute & attribute,
    const PaddedPODArray<ssize_t> & key_index_to_element_index,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & attribute_container = std::get<AttributeContainerType<AttributeType>>(attribute.container);
    const size_t keys_size = key_index_to_element_index.size();

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        bool key_exists = key_index_to_element_index[key_index] != -1;

        if (key_exists)
        {
            size_t element_index = static_cast<size_t>(key_index_to_element_index[key_index]);
            const auto & element = attribute_container[element_index];

            if constexpr (is_nullable)
                set_value(key_index, element, (*attribute.is_index_null)[element_index]);
            else
                set_value(key_index, element, false);
        }
        else
        {
            if constexpr (is_nullable)
                set_value(key_index, default_value_extractor[key_index], default_value_extractor.isNullAt(key_index));
            else
                set_value(key_index, default_value_extractor[key_index], false);
        }
    }
}

template <DictionaryKeyType dictionary_key_type>
void HashedArrayDictionary<dictionary_key_type>::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        QueryPipeline pipeline;
        pipeline = QueryPipeline(source_ptr->loadAll());

        PullingPipelineExecutor executor(pipeline);
        Block block;
        while (executor.pull(block))
        {
            resize(block.rows());
            blockToAttributes(block);
        }
    }
    else
    {
        updateData();
    }

    if (configuration.require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "{}: dictionary source is empty and 'require_nonempty' property is set.",
            getFullName());
}

template <DictionaryKeyType dictionary_key_type>
void HashedArrayDictionary<dictionary_key_type>::buildHierarchyParentToChildIndexIfNeeded()
{
    if (!dict_struct.hierarchical_attribute_index)
        return;

    if (dict_struct.attributes[*dict_struct.hierarchical_attribute_index].bidirectional)
        hierarchical_index = getHierarchicalIndex();
}

template <DictionaryKeyType dictionary_key_type>
void HashedArrayDictionary<dictionary_key_type>::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    bytes_allocated += key_attribute.container.size();

    for (auto & attribute : attributes)
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            const auto & container = std::get<AttributeContainerType<ValueType>>(attribute.container);
            bytes_allocated += sizeof(AttributeContainerType<ValueType>);

            if constexpr (std::is_same_v<ValueType, Array>)
            {
                /// It is not accurate calculations
                bytes_allocated += sizeof(Array) * container.size();
            }
            else
            {
                bytes_allocated += container.allocated_bytes();
            }

            bucket_count = container.capacity();
        };

        callOnDictionaryAttributeType(attribute.type, type_call);

        if (attribute.is_index_null.has_value())
            bytes_allocated += (*attribute.is_index_null).size();
    }

    if (update_field_loaded_block)
        bytes_allocated += update_field_loaded_block->allocatedBytes();

    if (hierarchical_index)
    {
        hierarchical_index_bytes_allocated = hierarchical_index->getSizeInBytes();
        bytes_allocated += hierarchical_index_bytes_allocated;
    }

    bytes_allocated += string_arena.size();
}

template <DictionaryKeyType dictionary_key_type>
Pipe HashedArrayDictionary<dictionary_key_type>::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
{
    PaddedPODArray<HashedArrayDictionary::KeyType> keys;
    keys.reserve(key_attribute.container.size());

    for (auto & [key, _] : key_attribute.container)
        keys.emplace_back(key);

    ColumnsWithTypeAndName key_columns;

    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        auto keys_column = getColumnFromPODArray(std::move(keys));
        key_columns = {ColumnWithTypeAndName(std::move(keys_column), std::make_shared<DataTypeUInt64>(), dict_struct.id->name)};
    }
    else
    {
        key_columns = deserializeColumnsWithTypeAndNameFromKeys(dict_struct, keys, 0, keys.size());
    }

    std::shared_ptr<const IDictionary> dictionary = shared_from_this();
    auto coordinator = std::make_shared<DictionarySourceCoordinator>(dictionary, column_names, std::move(key_columns), max_block_size);
    auto result = coordinator->read(num_streams);

    return result;
}

template class HashedArrayDictionary<DictionaryKeyType::Simple>;
template class HashedArrayDictionary<DictionaryKeyType::Complex>;

void registerDictionaryArrayHashed(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             DictionaryKeyType dictionary_key_type) -> DictionaryPtr
    {
        if (dictionary_key_type == DictionaryKeyType::Simple && dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for simple key hashed array dictionary");
        else if (dictionary_key_type == DictionaryKeyType::Complex && dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for complex key hashed array dictionary");

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: elements .structure.range_min and .structure.range_max should be defined only "
                "for a dictionary of layout 'range_hashed'",
                full_name);

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

        HashedArrayDictionaryStorageConfiguration configuration{require_nonempty, dict_lifetime};

        if (dictionary_key_type == DictionaryKeyType::Simple)
            return std::make_unique<HashedArrayDictionary<DictionaryKeyType::Simple>>(dict_id, dict_struct, std::move(source_ptr), configuration);
        else
            return std::make_unique<HashedArrayDictionary<DictionaryKeyType::Complex>>(dict_id, dict_struct, std::move(source_ptr), configuration);
    };

    using namespace std::placeholders;

    factory.registerLayout("hashed_array",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Simple); }, false);
    factory.registerLayout("complex_key_hashed_array",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Complex); }, true);
}

}
