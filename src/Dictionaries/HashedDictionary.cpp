#include "HashedDictionary.h"

#include <Common/ArenaUtils.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>

#include <Processors/Sources/SourceWithProgress.h>

#include <Dictionaries//DictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/HierarchyDictionariesUtils.h>
#include <Common/logger_useful.h>

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
    const HashedDictionaryStorageConfiguration & configuration_,
    BlockPtr update_field_loaded_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , configuration(configuration_)
    , update_field_loaded_block(std::move(update_field_loaded_block_))
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
    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    ColumnPtr result;

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    const size_t size = extractor.getKeysSize();

    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);
    const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    auto & attribute = attributes[attribute_index];

    bool is_attribute_nullable = attribute.is_nullable_set.has_value();

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

        DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(dictionary_attribute.null_value, default_values_column);

        auto column = ColumnProvider::getColumn(dictionary_attribute, size);

        if constexpr (std::is_same_v<ValueType, Array>)
        {
            auto * out = column.get();

            getItemsImpl<ValueType, false>(
                attribute,
                extractor,
                [&](const size_t, const Array & value, bool) { out->insert(value); },
                default_value_extractor);
        }
        else if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    extractor,
                    [&](size_t row, const StringRef value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out->insertData(value.data, value.size);
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    extractor,
                    [&](size_t, const StringRef value, bool) { out->insertData(value.data, value.size); },
                    default_value_extractor);
        }
        else
        {
            auto & out = column->getData();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    extractor,
                    [&](size_t row, const auto value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out[row] = value;
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    extractor,
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

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnUInt8::Ptr HashedDictionary<dictionary_key_type, sparse>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    size_t keys_size = extractor.getKeysSize();

    auto result = ColumnUInt8::create(keys_size, false);
    auto & out = result->getData();

    size_t keys_found = 0;

    if (unlikely(attributes.empty()))
    {
        for (size_t requested_key_index = 0; requested_key_index < keys_size; ++requested_key_index)
        {
            auto requested_key = extractor.extractCurrentKey();
            out[requested_key_index] = no_attributes_container.find(requested_key) != no_attributes_container.end();
            keys_found += out[requested_key_index];
            extractor.rollbackCurrentKey();
        }

        query_count.fetch_add(keys_size, std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);
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

            keys_found += out[requested_key_index];

            if (is_attribute_nullable && !out[requested_key_index])
                out[requested_key_index] = attribute.is_nullable_set->find(requested_key) != nullptr;

            extractor.rollbackCurrentKey();
        }
    });

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnPtr HashedDictionary<dictionary_key_type, sparse>::getHierarchy(ColumnPtr key_column [[maybe_unused]], const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        const UInt64 null_value = dictionary_attribute.null_value.template get<UInt64>();
        const CollectionType<UInt64> & parent_keys_map = std::get<CollectionType<UInt64>>(hierarchical_attribute.container);

        auto is_key_valid_func = [&](auto & key) { return parent_keys_map.find(key) != parent_keys_map.end(); };

        size_t keys_found = 0;

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            auto it = parent_keys_map.find(hierarchy_key);

            if (it != parent_keys_map.end())
                result = getValueFromCell(it);

            keys_found += result.has_value();

            return result;
        };

        auto dictionary_hierarchy_array = getKeysHierarchyArray(keys, null_value, is_key_valid_func, get_parent_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

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
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
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

        size_t keys_found = 0;

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            auto it = parent_keys_map.find(hierarchy_key);

            if (it != parent_keys_map.end())
                result = getValueFromCell(it);

            keys_found += result.has_value();

            return result;
        };

        auto result = getKeysIsInHierarchyColumn(keys, keys_in, null_value, is_key_valid_func, get_parent_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

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
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        PaddedPODArray<UInt64> keys_backup;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];
        const CollectionType<UInt64> & parent_keys = std::get<CollectionType<UInt64>>(hierarchical_attribute.container);

        HashMap<UInt64, PaddedPODArray<UInt64>> parent_to_child;

        for (const auto & [key, value] : parent_keys)
            parent_to_child[value].emplace_back(key);

        size_t keys_found;
        auto result = getKeysDescendantsArray(keys, parent_to_child, level, keys_found);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

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
            Attribute attribute{dictionary_attribute.underlying_type, std::move(is_nullable_set), CollectionType<ValueType>()};
            attributes.emplace_back(std::move(attribute));
        };

        callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::updateData()
{
    /// NOTE: updateData() does not preallocation since it may increase memory usage.

    if (!update_field_loaded_block || update_field_loaded_block->rows() == 0)
    {
        QueryPipeline pipeline(source_ptr->loadUpdatedAll());

        PullingPipelineExecutor executor(pipeline);
        Block block;
        while (executor.pull(block))
        {
            convertToFullIfSparse(block);

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

    size_t attributes_size = attributes.size();

    if (unlikely(attributes_size == 0))
    {
        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys_extractor.extractCurrentKey();

            if constexpr (std::is_same_v<KeyType, StringRef>)
                key = copyStringInArena(string_arena, key);

            no_attributes_container.insert(key);
            keys_extractor.rollbackCurrentKey();
        }

        return;
    }

    for (size_t attribute_index = 0; attribute_index < attributes_size; ++attribute_index)
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
                    key = copyStringInArena(string_arena, key);

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
                    StringRef arena_value = copyStringInArena(string_arena, value_to_insert);
                    container.insert({key, arena_value});
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

    size_t attributes_size = attributes.size();

    if (unlikely(attributes_size == 0))
    {
        size_t reserve_size = added_rows + no_attributes_container.size();

        if constexpr (sparse)
            no_attributes_container.resize(reserve_size);
        else
            no_attributes_container.reserve(reserve_size);

        return;
    }

    for (size_t attribute_index = 0; attribute_index < attributes_size; ++attribute_index)
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
template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void HashedDictionary<dictionary_key_type, sparse>::getItemsImpl(
    const Attribute & attribute,
    DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
    ValueSetter && set_value [[maybe_unused]],
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & attribute_container = std::get<CollectionType<AttributeType>>(attribute.container);
    const size_t keys_size = keys_extractor.getKeysSize();

    size_t keys_found = 0;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();

        const auto it = attribute_container.find(key);

        if (it != attribute_container.end())
        {
            set_value(key_index, getValueFromCell(it), false);
            ++keys_found;
        }
        else
        {
            if constexpr (is_nullable)
            {
                bool is_value_nullable = (attribute.is_nullable_set->find(key) != nullptr) || default_value_extractor.isNullAt(key_index);
                set_value(key_index, default_value_extractor[key_index], is_value_nullable);
            }
            else
                set_value(key_index, default_value_extractor[key_index], false);
        }

        keys_extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        std::atomic<size_t> new_size = 0;

        QueryPipeline pipeline;
        if (configuration.preallocate)
            pipeline = QueryPipeline(source_ptr->loadAllWithSizeHint(&new_size));
        else
            pipeline = QueryPipeline(source_ptr->loadAll());

        PullingPipelineExecutor executor(pipeline);
        Block block;
        while (executor.pull(block))
        {
            if (configuration.preallocate && new_size)
            {
                size_t current_new_size = new_size.exchange(0);
                if (current_new_size)
                {
                    LOG_TRACE(&Poco::Logger::get("HashedDictionary"), "Preallocated {} elements", current_new_size);
                    resize(current_new_size);
                }
            }
            else
            {
                resize(block.rows());
            }

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

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::calculateBytesAllocated()
{
    size_t attributes_size = attributes.size();
    bytes_allocated += attributes_size * sizeof(attributes.front());

    for (size_t i = 0; i < attributes_size; ++i)
    {
        getAttributeContainer(i, [&](const auto & container)
        {
            using ContainerType = std::decay_t<decltype(container)>;
            using AttributeValueType = typename ContainerType::mapped_type;

            bytes_allocated += sizeof(container);

            if constexpr (sparse || std::is_same_v<AttributeValueType, Field>)
            {
                /// bucket_count() - Returns table size, that includes empty and deleted
                /// size()         - Returns table size, without empty and deleted
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

        bytes_allocated += sizeof(attributes[i].is_nullable_set);

        if (attributes[i].is_nullable_set.has_value())
            bytes_allocated = attributes[i].is_nullable_set->getBufferSizeInBytes();
    }

    if (unlikely(attributes_size == 0))
    {
        bytes_allocated += sizeof(no_attributes_container);

        if constexpr (sparse)
        {
            bytes_allocated += no_attributes_container.size() * (sizeof(KeyType));
            bucket_count = no_attributes_container.bucket_count();
        }
        else
        {
            bytes_allocated += no_attributes_container.getBufferSizeInBytes();
            bucket_count = no_attributes_container.getBufferSizeInCells();
        }
    }

    bytes_allocated += string_arena.size();

    if (update_field_loaded_block)
        bytes_allocated += update_field_loaded_block->allocatedBytes();
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
Pipe HashedDictionary<dictionary_key_type, sparse>::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
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
    else
    {
        keys.reserve(no_attributes_container.size());

        for (const auto & key : no_attributes_container)
        {
            if constexpr (sparse)
                keys.emplace_back(key);
            else
                keys.emplace_back(key.getKey());
        }
    }

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

template class HashedDictionary<DictionaryKeyType::Simple, true>;
template class HashedDictionary<DictionaryKeyType::Simple, false>;
template class HashedDictionary<DictionaryKeyType::Complex, true>;
template class HashedDictionary<DictionaryKeyType::Complex, false>;

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
        if (dictionary_key_type == DictionaryKeyType::Simple && dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for simple key hashed dictionary");
        else if (dictionary_key_type == DictionaryKeyType::Complex && dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for complex key hashed dictionary");

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: elements .structure.range_min and .structure.range_max should be defined only "
                "for a dictionary of layout 'range_hashed'",
                full_name);

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

        std::string dictionary_layout_name;

        if (dictionary_key_type == DictionaryKeyType::Simple)
            dictionary_layout_name = "hashed";
        else
            dictionary_layout_name = "complex_key_hashed";

        if (sparse)
            dictionary_layout_name = "sparse_" + dictionary_layout_name;

        const std::string dictionary_layout_prefix = ".layout." + dictionary_layout_name;
        const bool preallocate = config.getBool(config_prefix + dictionary_layout_prefix + ".preallocate", false);

        HashedDictionaryStorageConfiguration configuration{preallocate, require_nonempty, dict_lifetime};

        if (dictionary_key_type == DictionaryKeyType::Simple)
        {
            if (sparse)
                return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, true>>(dict_id, dict_struct, std::move(source_ptr), configuration);
            else
                return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, false>>(dict_id, dict_struct, std::move(source_ptr), configuration);
        }
        else
        {
            if (sparse)
                return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, true>>(dict_id, dict_struct, std::move(source_ptr), configuration);
            else
                return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, false>>(dict_id, dict_struct, std::move(source_ptr), configuration);
        }
    };

    using namespace std::placeholders;

    factory.registerLayout("hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Simple, /* sparse = */ false); }, false);
    factory.registerLayout("sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Simple, /* sparse = */ true); }, false);
    factory.registerLayout("complex_key_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Complex, /* sparse = */ false); }, true);
    factory.registerLayout("complex_key_sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Complex, /* sparse = */ true); }, true);

}

}
