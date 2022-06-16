#include "FlatDictionary.h"

#include <Core/Defines.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/ArenaUtils.h>

#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Dictionaries/DictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/HierarchyDictionariesUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
}

FlatDictionary::FlatDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    Configuration configuration_,
    BlockPtr update_field_loaded_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , configuration(configuration_)
    , loaded_keys(configuration.initial_array_size, false)
    , update_field_loaded_block(std::move(update_field_loaded_block_))
{
    createAttributes();
    loadData();
    buildHierarchyParentToChildIndexIfNeeded();
    calculateBytesAllocated();
}

ColumnPtr FlatDictionary::getColumn(
        const std::string & attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes &,
        const ColumnPtr & default_values_column) const
{
    ColumnPtr result;

    PaddedPODArray<UInt64> backup_storage;
    const auto & ids = getColumnVectorData(this, key_columns.front(), backup_storage);

    auto size = ids.size();

    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);

    size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    const auto & attribute = attributes[attribute_index];

    bool is_attribute_nullable = attribute.is_nullable_set.has_value();
    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;

    if (is_attribute_nullable)
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
                ids,
                [&](size_t, const Array & value, bool) { out->insert(value); },
                default_value_extractor);
        }
        else if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    ids,
                    [&](size_t row, const StringRef value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out->insertData(value.data, value.size);
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    ids,
                    [&](size_t, const StringRef value, bool) { out->insertData(value.data, value.size); },
                    default_value_extractor);
        }
        else
        {
            auto & out = column->getData();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    ids,
                    [&](size_t row, const auto value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out[row] = value;
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    ids,
                    [&](size_t row, const auto value, bool) { out[row] = value; },
                    default_value_extractor);
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    if (attribute.is_nullable_set)
        result = ColumnNullable::create(result, std::move(col_null_map_to));

    return result;
}

ColumnUInt8::Ptr FlatDictionary::hasKeys(const Columns & key_columns, const DataTypes &) const
{
    PaddedPODArray<UInt64> backup_storage;
    const auto & keys = getColumnVectorData(this, key_columns.front(), backup_storage);
    size_t keys_size = keys.size();

    auto result = ColumnUInt8::create(keys_size);
    auto & out = result->getData();

    size_t keys_found = 0;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        const auto key = keys[key_index];
        out[key_index] = key < loaded_keys.size() && loaded_keys[key];
        keys_found += out[key_index];
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

ColumnPtr FlatDictionary::getHierarchy(ColumnPtr key_column, const DataTypePtr &) const
{
    PaddedPODArray<UInt64> keys_backup_storage;
    const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

    size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;
    const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
    const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

    std::optional<UInt64> null_value;

    if (!dictionary_attribute.null_value.isNull())
        null_value = dictionary_attribute.null_value.get<UInt64>();

    const ContainerType<UInt64> & parent_keys = std::get<ContainerType<UInt64>>(hierarchical_attribute.container);

    auto is_key_valid_func = [&, this](auto & key) { return key < loaded_keys.size() && loaded_keys[key]; };

    size_t keys_found = 0;

    auto get_parent_key_func = [&, this](auto & hierarchy_key)
    {
        std::optional<UInt64> result;

        bool is_key_valid = hierarchy_key < loaded_keys.size() && loaded_keys[hierarchy_key];

        if (!is_key_valid)
            return result;

        if (unlikely(hierarchical_attribute.is_nullable_set) && hierarchical_attribute.is_nullable_set->find(hierarchy_key))
            return result;

        UInt64 parent_key = parent_keys[hierarchy_key];
        if (null_value && *null_value == parent_key)
            return result;

        result = parent_key;
        keys_found += 1;
        return result;
    };

    auto dictionary_hierarchy_array = getKeysHierarchyArray(keys, is_key_valid_func, get_parent_key_func);

    query_count.fetch_add(keys.size(), std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return dictionary_hierarchy_array;
}

ColumnUInt8::Ptr FlatDictionary::isInHierarchy(
    ColumnPtr key_column,
    ColumnPtr in_key_column,
    const DataTypePtr &) const
{
    PaddedPODArray<UInt64> keys_backup_storage;
    const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

    PaddedPODArray<UInt64> keys_in_backup_storage;
    const auto & keys_in = getColumnVectorData(this, in_key_column, keys_in_backup_storage);

    size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;
    const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
    const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

    std::optional<UInt64> null_value;

    if (!dictionary_attribute.null_value.isNull())
        null_value = dictionary_attribute.null_value.get<UInt64>();

    const ContainerType<UInt64> & parent_keys = std::get<ContainerType<UInt64>>(hierarchical_attribute.container);

    auto is_key_valid_func = [&, this](auto & key) { return key < loaded_keys.size() && loaded_keys[key]; };

    size_t keys_found = 0;

    auto get_parent_key_func = [&, this](auto & hierarchy_key)
    {
        std::optional<UInt64> result;

        bool is_key_valid = hierarchy_key < loaded_keys.size() && loaded_keys[hierarchy_key];

        if (!is_key_valid)
            return result;

        if (unlikely(hierarchical_attribute.is_nullable_set) && hierarchical_attribute.is_nullable_set->find(hierarchy_key))
            return result;

        UInt64 parent_key = parent_keys[hierarchy_key];
        if (null_value && *null_value == parent_key)
            return result;

        result = parent_keys[hierarchy_key];
        keys_found += 1;
        return result;
    };

    auto result = getKeysIsInHierarchyColumn(keys, keys_in, is_key_valid_func, get_parent_key_func);

    query_count.fetch_add(keys.size(), std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

DictionaryHierarchyParentToChildIndexPtr FlatDictionary::getHierarchicalIndex() const
{
    if (hierarhical_index)
        return hierarhical_index;

    size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;
    const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];
    const ContainerType<UInt64> & parent_keys = std::get<ContainerType<UInt64>>(hierarchical_attribute.container);

    HashMap<UInt64, PaddedPODArray<UInt64>> parent_to_child;
    parent_to_child.reserve(element_count);

    UInt64 child_keys_size = static_cast<UInt64>(parent_keys.size());

    for (UInt64 child_key = 0; child_key < child_keys_size; ++child_key)
    {
        if (!loaded_keys[child_key])
            continue;

        if (unlikely(hierarchical_attribute.is_nullable_set) && hierarchical_attribute.is_nullable_set->find(child_key))
            continue;

        auto parent_key = parent_keys[child_key];
        parent_to_child[parent_key].emplace_back(child_key);
    }

    return std::make_shared<DictionaryHierarchicalParentToChildIndex>(parent_to_child);
}

ColumnPtr FlatDictionary::getDescendants(
    ColumnPtr key_column,
    const DataTypePtr &,
    size_t level,
    DictionaryHierarchicalParentToChildIndexPtr parent_to_child_index) const
{
    PaddedPODArray<UInt64> keys_backup;
    const auto & keys = getColumnVectorData(this, key_column, keys_backup);

    size_t keys_found;
    auto result = getKeysDescendantsArray(keys, *parent_to_child_index, level, keys_found);

    query_count.fetch_add(keys.size(), std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

void FlatDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
        attributes.push_back(createAttribute(attribute));
}

void FlatDictionary::blockToAttributes(const Block & block)
{
    const auto keys_column = block.safeGetByPosition(0).column;

    DictionaryKeysArenaHolder<DictionaryKeyType::Simple> arena_holder;
    DictionaryKeysExtractor<DictionaryKeyType::Simple> keys_extractor({ keys_column }, arena_holder.getComplexKeyArena());
    size_t keys_size = keys_extractor.getKeysSize();

    static constexpr size_t key_offset = 1;

    size_t attributes_size = attributes.size();

    if (unlikely(attributes_size == 0))
    {
        for (size_t i = 0; i < keys_size; ++i)
        {
            auto key = keys_extractor.extractCurrentKey();

            if (unlikely(key >= configuration.max_array_size))
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "{}: identifier should be less than {}",
                    getFullName(),
                    toString(configuration.max_array_size));

            if (key >= loaded_keys.size())
            {
                const size_t elements_count = key + 1;
                loaded_keys.resize(elements_count, false);
            }

            loaded_keys[key] = true;

            keys_extractor.rollbackCurrentKey();
        }

        return;
    }

    for (size_t attribute_index = 0; attribute_index < attributes_size; ++attribute_index)
    {
        const IColumn & attribute_column = *block.safeGetByPosition(attribute_index + key_offset).column;
        Attribute & attribute = attributes[attribute_index];

        for (size_t i = 0; i < keys_size; ++i)
        {
            auto key = keys_extractor.extractCurrentKey();

            setAttributeValue(attribute, key, attribute_column[i]);
            keys_extractor.rollbackCurrentKey();
        }

        keys_extractor.reset();
    }
}

void FlatDictionary::updateData()
{
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

            for (size_t column_index = 0; column_index < block.columns(); ++column_index)
            {
                const IColumn & update_column = *block.getByPosition(column_index).column.get();
                MutableColumnPtr saved_column = update_field_loaded_block->getByPosition(column_index).column->assumeMutable();
                saved_column->insertRangeFrom(update_column, 0, update_column.size());
            }
        }
    }
    else
    {
        auto pipeline(source_ptr->loadUpdatedAll());
        mergeBlockWithPipe<DictionaryKeyType::Simple>(
            dict_struct.getKeysSize(),
            *update_field_loaded_block,
            std::move(pipeline));
    }

    if (update_field_loaded_block)
        blockToAttributes(*update_field_loaded_block.get());
}

void FlatDictionary::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        QueryPipeline pipeline(source_ptr->loadAll());
        PullingPipelineExecutor executor(pipeline);

        Block block;
        while (executor.pull(block))
            blockToAttributes(block);
    }
    else
        updateData();

    element_count = 0;

    size_t loaded_keys_size = loaded_keys.size();
    for (size_t i = 0; i < loaded_keys_size; ++i)
        element_count += loaded_keys[i];

    if (configuration.require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY, "{}: dictionary source is empty and 'require_nonempty' property is set.", getFullName());
}

void FlatDictionary::buildHierarchyParentToChildIndexIfNeeded()
{
    if (!dict_struct.hierarchical_attribute_index)
        return;

    if (dict_struct.attributes[*dict_struct.hierarchical_attribute_index].bidirectional)
        hierarhical_index = getHierarchicalIndex();
}

void FlatDictionary::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (const auto & attribute : attributes)
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            const auto & container = std::get<ContainerType<ValueType>>(attribute.container);
            bytes_allocated += sizeof(ContainerType<ValueType>);

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

        bytes_allocated += sizeof(attribute.is_nullable_set);

        if (attribute.is_nullable_set.has_value())
            bytes_allocated = attribute.is_nullable_set->getBufferSizeInBytes();
    }

    if (update_field_loaded_block)
        bytes_allocated += update_field_loaded_block->allocatedBytes();

    if (hierarhical_index)
    {
        hierarchical_index_bytes_allocated = hierarhical_index->getSizeInBytes();
        bytes_allocated += hierarchical_index_bytes_allocated;
    }

    bytes_allocated += string_arena.size();
}

FlatDictionary::Attribute FlatDictionary::createAttribute(const DictionaryAttribute & dictionary_attribute)
{
    auto is_nullable_set = dictionary_attribute.is_nullable ? std::make_optional<NullableSet>() : std::optional<NullableSet>{};
    Attribute attribute{dictionary_attribute.underlying_type, std::move(is_nullable_set), {}};

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        attribute.container.emplace<ContainerType<ValueType>>(configuration.initial_array_size, ValueType());
    };

    callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);

    return attribute;
}

template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void FlatDictionary::getItemsImpl(
    const Attribute & attribute,
    const PaddedPODArray<UInt64> & keys,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & container = std::get<ContainerType<AttributeType>>(attribute.container);
    const auto rows = keys.size();

    size_t keys_found = 0;

    for (size_t row = 0; row < rows; ++row)
    {
        const auto key = keys[row];

        if (key < loaded_keys.size() && loaded_keys[key])
        {
            if constexpr (is_nullable)
                set_value(row, container[key], attribute.is_nullable_set->find(key) != nullptr);
            else
                set_value(row, container[key], false);

            ++keys_found;
        }
        else
        {
            if constexpr (is_nullable)
                set_value(row, default_value_extractor[row], default_value_extractor.isNullAt(row));
            else
                set_value(row, default_value_extractor[row], false);
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <typename T>
void FlatDictionary::resize(Attribute & attribute, UInt64 key)
{
    if (key >= configuration.max_array_size)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "{}: identifier should be less than {}",
            getFullName(),
            toString(configuration.max_array_size));

    auto & container = std::get<ContainerType<T>>(attribute.container);

    if (key >= container.size())
    {
        const size_t elements_count = key + 1; //id=0 -> elements_count=1
        loaded_keys.resize(elements_count, false);

        if constexpr (std::is_same_v<T, Array>)
            container.resize(elements_count, T{});
        else
            container.resize_fill(elements_count, T{});
    }
}

void FlatDictionary::setAttributeValue(Attribute & attribute, const UInt64 key, const Field & value)
{
    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        resize<ValueType>(attribute, key);

        if (attribute.is_nullable_set && value.isNull())
        {
            attribute.is_nullable_set->insert(key);
            loaded_keys[key] = true;
            return;
        }

        auto & attribute_value = value.get<AttributeType>();

        auto & container = std::get<ContainerType<ValueType>>(attribute.container);
        loaded_keys[key] = true;

        if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto arena_value = copyStringInArena(string_arena, attribute_value);
            container[key] = arena_value;
        }
        else
        {
            container[key] = attribute_value;
        }
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

Pipe FlatDictionary::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
{
    const auto keys_count = loaded_keys.size();

    PaddedPODArray<UInt64> keys;
    keys.reserve(keys_count);

    for (size_t key_index = 0; key_index < keys_count; ++key_index)
        if (loaded_keys[key_index])
            keys.push_back(key_index);

    auto keys_column = getColumnFromPODArray(std::move(keys));
    ColumnsWithTypeAndName key_columns = {ColumnWithTypeAndName(keys_column, std::make_shared<DataTypeUInt64>(), dict_struct.id->name)};

    std::shared_ptr<const IDictionary> dictionary = shared_from_this();
    auto coordinator =std::make_shared<DictionarySourceCoordinator>(dictionary, column_names, std::move(key_columns), max_block_size);
    auto result = coordinator->read(num_streams);

    return result;
}

void registerDictionaryFlat(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                            const DictionaryStructure & dict_struct,
                            const Poco::Util::AbstractConfiguration & config,
                            const std::string & config_prefix,
                            DictionarySourcePtr source_ptr,
                            ContextPtr /* global_context */,
                            bool /* created_from_ddl */) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout 'flat'");

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "{}: elements .structure.range_min and .structure.range_max should be defined only "
                            "for a dictionary of layout 'range_hashed'",
                            full_name);

        static constexpr size_t default_initial_array_size = 1024;
        static constexpr size_t default_max_array_size = 500000;

        String dictionary_layout_prefix = config_prefix + ".layout" + ".flat";
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        FlatDictionary::Configuration configuration
        {
            .initial_array_size = config.getUInt64(dictionary_layout_prefix + ".initial_array_size", default_initial_array_size),
            .max_array_size = config.getUInt64(dictionary_layout_prefix + ".max_array_size", default_max_array_size),
            .require_nonempty = config.getBool(config_prefix + ".require_nonempty", false),
            .dict_lifetime = dict_lifetime
        };

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        return std::make_unique<FlatDictionary>(dict_id, dict_struct, std::move(source_ptr), configuration);
    };

    factory.registerLayout("flat", create_layout, false);
}


}
