#include "FlatDictionary.h"

#include <Core/Defines.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>

#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>

#include <Dictionaries/DictionaryBlockInputStream.h>
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
    const DictionaryLifetime dict_lifetime_,
    Configuration configuration_,
    BlockPtr update_field_loaded_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , configuration(configuration_)
    , loaded_keys(configuration.initial_array_size, false)
    , update_field_loaded_block(std::move(update_field_loaded_block_))
{
    createAttributes();
    loadData();
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
        result = ColumnNullable::create(std::move(result), std::move(col_null_map_to));

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

    const UInt64 null_value = dictionary_attribute.null_value.get<UInt64>();
    const ContainerType<UInt64> & parent_keys = std::get<ContainerType<UInt64>>(hierarchical_attribute.container);

    auto is_key_valid_func = [&, this](auto & key) { return key < loaded_keys.size() && loaded_keys[key]; };

    size_t keys_found = 0;

    auto get_parent_key_func = [&, this](auto & hierarchy_key)
    {
        bool is_key_valid = hierarchy_key < loaded_keys.size() && loaded_keys[hierarchy_key];
        std::optional<UInt64> result = is_key_valid ? std::make_optional(parent_keys[hierarchy_key]) : std::nullopt;
        keys_found += result.has_value();
        return result;
    };

    auto dictionary_hierarchy_array = getKeysHierarchyArray(keys, null_value, is_key_valid_func, get_parent_key_func);

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

    const UInt64 null_value = dictionary_attribute.null_value.get<UInt64>();
    const ContainerType<UInt64> & parent_keys = std::get<ContainerType<UInt64>>(hierarchical_attribute.container);

    auto is_key_valid_func = [&, this](auto & key) { return key < loaded_keys.size() && loaded_keys[key]; };

    size_t keys_found = 0;

    auto get_parent_key_func = [&, this](auto & hierarchy_key)
    {
        bool is_key_valid = hierarchy_key < loaded_keys.size() && loaded_keys[hierarchy_key];
        std::optional<UInt64> result = is_key_valid ? std::make_optional(parent_keys[hierarchy_key]) : std::nullopt;
        keys_found += result.has_value();
        return result;
    };

    auto result = getKeysIsInHierarchyColumn(keys, keys_in, null_value, is_key_valid_func, get_parent_key_func);

    query_count.fetch_add(keys.size(), std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

ColumnPtr FlatDictionary::getDescendants(
    ColumnPtr key_column,
    const DataTypePtr &,
    size_t level) const
{
    PaddedPODArray<UInt64> keys_backup;
    const auto & keys = getColumnVectorData(this, key_column, keys_backup);

    size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;
    const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];
    const ContainerType<UInt64> & parent_keys = std::get<ContainerType<UInt64>>(hierarchical_attribute.container);

    HashMap<UInt64, PaddedPODArray<UInt64>> parent_to_child;

    for (size_t i = 0; i < parent_keys.size(); ++i)
    {
        auto parent_key = parent_keys[i];

        if (loaded_keys[i])
            parent_to_child[parent_key].emplace_back(static_cast<UInt64>(i));
    }

    size_t keys_found;
    auto result = getKeysDescendantsArray(keys, parent_to_child, level, keys_found);

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

    DictionaryKeysArenaHolder<DictionaryKeyType::simple> arena_holder;
    DictionaryKeysExtractor<DictionaryKeyType::simple> keys_extractor({ keys_column }, arena_holder.getComplexKeyArena());
    auto keys = keys_extractor.extractAllKeys();

    HashSet<UInt64> already_processed_keys;

    size_t key_offset = 1;
    for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
    {
        const IColumn & attribute_column = *block.safeGetByPosition(attribute_index + key_offset).column;
        Attribute & attribute = attributes[attribute_index];

        for (size_t i = 0; i < keys.size(); ++i)
        {
            auto key = keys[i];

            if (already_processed_keys.find(key) != nullptr)
                continue;

            already_processed_keys.insert(key);

            setAttributeValue(attribute, key, attribute_column[i]);
            ++element_count;
        }

        already_processed_keys.clear();
    }
}

void FlatDictionary::updateData()
{
    if (!update_field_loaded_block || update_field_loaded_block->rows() == 0)
    {
        auto stream = source_ptr->loadUpdatedAll();
        stream->readPrefix();

        while (const auto block = stream->read())
        {
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
        stream->readSuffix();
    }
    else
    {
        auto stream = source_ptr->loadUpdatedAll();
        mergeBlockWithStream<DictionaryKeyType::simple>(
            dict_struct.getKeysSize(),
            *update_field_loaded_block,
            stream);
    }

    if (update_field_loaded_block)
        blockToAttributes(*update_field_loaded_block.get());
}

void FlatDictionary::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        auto stream = source_ptr->loadAll();
        stream->readPrefix();

        while (const auto block = stream->read())
            blockToAttributes(block);

        stream->readSuffix();
    }
    else
        updateData();

    if (configuration.require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY, "{}: dictionary source is empty and 'require_nonempty' property is set.", full_name);
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

            if constexpr (std::is_same_v<ValueType, StringRef>)
                bytes_allocated += sizeof(Arena) + attribute.string_arena->size();
        };

        callOnDictionaryAttributeType(attribute.type, type_call);

        bytes_allocated += sizeof(attribute.is_nullable_set);

        if (attribute.is_nullable_set.has_value())
            bytes_allocated = attribute.is_nullable_set->getBufferSizeInBytes();
    }

    if (update_field_loaded_block)
        bytes_allocated += update_field_loaded_block->allocatedBytes();
}

FlatDictionary::Attribute FlatDictionary::createAttribute(const DictionaryAttribute & dictionary_attribute)
{
    auto is_nullable_set = dictionary_attribute.is_nullable ? std::make_optional<NullableSet>() : std::optional<NullableSet>{};
    Attribute attribute{dictionary_attribute.underlying_type, std::move(is_nullable_set), {}, {}};

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        if constexpr (std::is_same_v<ValueType, StringRef>)
            attribute.string_arena = std::make_unique<Arena>();

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
            full_name,
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

template <typename T>
void FlatDictionary::setAttributeValueImpl(Attribute & attribute, UInt64 key, const T & value)
{
    auto & array = std::get<ContainerType<T>>(attribute.container);
    array[key] = value;
    loaded_keys[key] = true;
}

template <>
void FlatDictionary::setAttributeValueImpl<String>(Attribute & attribute, UInt64 key, const String & value)
{
    const auto * string_in_arena = attribute.string_arena->insert(value.data(), value.size());
    setAttributeValueImpl(attribute, key, StringRef{string_in_arena, value.size()});
}

void FlatDictionary::setAttributeValue(Attribute & attribute, const UInt64 key, const Field & value)
{
    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        resize<ValueType>(attribute, key);

        if (attribute.is_nullable_set)
        {
            if (value.isNull())
            {
                attribute.is_nullable_set->insert(key);
                loaded_keys[key] = true;
                return;
            }
        }

        setAttributeValueImpl<AttributeType>(attribute, key, value.get<AttributeType>());
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

BlockInputStreamPtr FlatDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    const auto keys_count = loaded_keys.size();

    PaddedPODArray<UInt64> keys;
    keys.reserve(keys_count);

    for (size_t key_index = 0; key_index < keys_count; ++key_index)
        if (loaded_keys[key_index])
            keys.push_back(key_index);

    return std::make_shared<DictionaryBlockInputStream>(shared_from_this(), max_block_size, std::move(keys), column_names);
}

void registerDictionaryFlat(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                            const DictionaryStructure & dict_struct,
                            const Poco::Util::AbstractConfiguration & config,
                            const std::string & config_prefix,
                            DictionarySourcePtr source_ptr,
                            ContextPtr /* context */,
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

        FlatDictionary::Configuration configuration
        {
            .initial_array_size = config.getUInt64(dictionary_layout_prefix + ".initial_array_size", default_initial_array_size),
            .max_array_size = config.getUInt64(dictionary_layout_prefix + ".max_array_size", default_max_array_size),
            .require_nonempty = config.getBool(config_prefix + ".require_nonempty", false)
        };

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        return std::make_unique<FlatDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, std::move(configuration));
    };

    factory.registerLayout("flat", create_layout, false);
}


}
