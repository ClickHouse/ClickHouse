#include <Dictionaries/RangeHashedDictionary.h>

#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeEnum.h>

#include <Columns/ColumnNullable.h>

#include <Functions/FunctionHelpers.h>
#include <Interpreters/castColumn.h>

#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySource.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
}


template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::RangeHashedDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    RangeHashedDictionaryConfiguration configuration_,
    BlockPtr update_field_loaded_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , dict_lifetime(dict_lifetime_)
    , configuration(configuration_)
    , update_field_loaded_block(std::move(update_field_loaded_block_))
{
    createAttributes();
    loadData();
    calculateBytesAllocated();
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
ColumnPtr RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    const ColumnPtr & default_values_column) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
    {
        auto key_types_copy = key_types;
        key_types_copy.pop_back();
        dict_struct.validateKeyTypes(key_types_copy);
    }

    ColumnPtr result;

    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);
    const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    const auto & attribute = attributes[attribute_index];

    /// Cast second column to storage type
    Columns modified_key_columns = key_columns;
    auto range_storage_column = key_columns.back();
    ColumnWithTypeAndName column_to_cast = {range_storage_column->convertToFullColumnIfConst(), key_types.back(), ""};
    modified_key_columns.back() = castColumnAccurate(column_to_cast, dict_struct.range_min->type);

    size_t keys_size = key_columns.front()->size();
    bool is_attribute_nullable = attribute.is_value_nullable.has_value();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;
    if (is_attribute_nullable)
    {
        col_null_map_to = ColumnUInt8::create(keys_size, false);
        vec_null_map_to = &col_null_map_to->getData();
    }

    auto type_call = [&](const auto &dictionary_attribute_type)
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
                modified_key_columns,
                [&](size_t, const Array & value, bool)
                {
                    out->insert(value);
                },
                default_value_extractor);
        }
        else if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    modified_key_columns,
                    [&](size_t row, const StringRef value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out->insertData(value.data, value.size);
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    modified_key_columns,
                    [&](size_t, const StringRef value, bool)
                    {
                        out->insertData(value.data, value.size);
                    },
                    default_value_extractor);
        }
        else
        {
            auto & out = column->getData();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    modified_key_columns,
                    [&](size_t row, const auto value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out[row] = value;
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    modified_key_columns,
                    [&](size_t row, const auto value, bool)
                    {
                        out[row] = value;
                    },
                    default_value_extractor);
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    if (is_attribute_nullable)
        result = ColumnNullable::create(std::move(result), std::move(col_null_map_to));

    return result;
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
ColumnUInt8::Ptr RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
    {
        auto key_types_copy = key_types;
        key_types_copy.pop_back();
        dict_struct.validateKeyTypes(key_types_copy);
    }

    auto range_storage_column = key_columns.back();
    ColumnWithTypeAndName column_to_cast = {range_storage_column->convertToFullColumnIfConst(), key_types.back(), ""};
    auto range_column_updated = castColumnAccurate(column_to_cast, dict_struct.range_min->type);

    const auto & range_column = assert_cast<const RangeColumnType &>(*range_column_updated);
    const auto & range_column_data = range_column.getData();

    auto key_columns_copy = key_columns;
    key_columns_copy.pop_back();
    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns_copy, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();

    auto & container = key_attribute.container;

    auto result = ColumnUInt8::create(keys_size);
    auto & out = result->getData();
    size_t keys_found = 0;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        const auto key = keys_extractor.extractCurrentKey();
        const auto it = container.find(key);

        if (it)
        {
            const auto date = range_column_data[key_index];
            const auto & interval_tree = it->getMapped();
            out[key_index] = interval_tree.has(date);
            keys_found += out[key_index];
        }
        else
        {
            out[key_index] = false;
        }

        keys_extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
void RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attributes.push_back(createAttribute(attribute));

        if (attribute.hierarchical)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hierarchical attributes not supported by {} dictionary.",
                            getDictionaryID().getNameForLogs());
    }
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
void RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        QueryPipeline pipeline(source_ptr->loadAll());

        PullingPipelineExecutor executor(pipeline);
        Block block;
        while (executor.pull(block))
        {
            blockToAttributes(block);
        }
    }
    else
    {
        updateData();
    }

    auto & key_attribute_container = key_attribute.container;

    for (auto & [_, intervals] : key_attribute_container)
        intervals.build();

    if (configuration.require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "{}: dictionary source is empty and 'require_nonempty' property is set.");
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
void RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::calculateBytesAllocated()
{
    bucket_count = key_attribute.container.getBufferSizeInCells();

    bytes_allocated += key_attribute.container.getBufferSizeInBytes();

    for (auto & [_, intervals] : key_attribute.container)
        bytes_allocated += intervals.getSizeInBytes();

    bytes_allocated += attributes.size() * sizeof(attributes.front());
    for (const auto & attribute : attributes)
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            const auto & container = std::get<AttributeContainerType<ValueType>>(attribute.container);

            bytes_allocated += container.size() * sizeof(ValueType);

            if (attribute.is_value_nullable)
                bytes_allocated += (*attribute.is_value_nullable).size() * sizeof(bool);
        };

        callOnDictionaryAttributeType(attribute.type, type_call);
    }

    if (update_field_loaded_block)
        bytes_allocated += update_field_loaded_block->allocatedBytes();

    bytes_allocated += string_arena.size();
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
typename RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::Attribute RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::createAttribute(const DictionaryAttribute & dictionary_attribute)
{
    std::optional<std::vector<bool>> is_value_nullable;

    if (dictionary_attribute.is_nullable)
        is_value_nullable.emplace(std::vector<bool>());

    Attribute attribute{dictionary_attribute.underlying_type, {}, std::move(is_value_nullable)};

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        attribute.container = AttributeContainerType<ValueType>();
    };

    callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);

    return attribute;
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::getItemsImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & container = std::get<AttributeContainerType<AttributeType>>(attribute.container);

    size_t keys_found = 0;

    const auto & range_column = assert_cast<const RangeColumnType &>(*key_columns.back());
    const auto & range_column_data = range_column.getData();

    auto key_columns_copy = key_columns;
    key_columns_copy.pop_back();

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns_copy, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();
        const auto it = key_attribute.container.find(key);

        if (it)
        {
            const auto date = range_column_data[key_index];
            const auto & interval_tree = it->getMapped();

            size_t min_value_index = 0;
            std::optional<RangeInterval> min_range;

            interval_tree.find(date, [&](auto & interval, auto & value_index)
            {
                if (min_range && interval < *min_range)
                {
                    min_range = interval;
                    min_value_index = value_index;
                }
                else
                {
                    min_range = interval;
                    min_value_index = value_index;
                }

                return true;
            });

            if (min_range.has_value())
            {
                ++keys_found;

                if constexpr (is_nullable)
                {
                    AttributeType value = container[min_value_index];
                    bool is_null = (*attribute.is_value_nullable)[min_value_index];

                    if (!is_null)
                        set_value(key_index, value, false);
                    else
                        set_value(key_index, default_value_extractor[key_index], true);
                }
                else
                {
                    AttributeType value = container[min_value_index];
                    set_value(key_index, value, false);
                }

                keys_extractor.rollbackCurrentKey();
                continue;
            }
        }

        if constexpr (is_nullable)
            set_value(key_index, default_value_extractor[key_index], default_value_extractor.isNullAt(key_index));
        else
            set_value(key_index, default_value_extractor[key_index], false);

        keys_extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
void RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::updateData()
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
        static constexpr size_t range_columns_size = 2;

        auto pipe = source_ptr->loadUpdatedAll();
        mergeBlockWithPipe<dictionary_key_type>(
            dict_struct.getKeysSize() + range_columns_size,
            *update_field_loaded_block,
            std::move(pipe));
    }

    if (update_field_loaded_block)
    {
        blockToAttributes(*update_field_loaded_block.get());
    }
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
void RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::blockToAttributes(const Block & block)
{
    size_t attributes_size = attributes.size();
    size_t dictionary_keys_size = dict_struct.getKeysSize();

    static constexpr size_t ranges_size = 2;

    size_t block_columns = block.columns();
    size_t range_dictionary_attributes_size = attributes_size + dictionary_keys_size + ranges_size;

    if (range_dictionary_attributes_size != block.columns())
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Block size mismatch. Actual {}. Expected {}",
            block_columns,
            range_dictionary_attributes_size);
    }

    Columns key_columns;
    key_columns.reserve(dictionary_keys_size);

    /// Split into keys columns and attribute columns
    for (size_t i = 0; i < dictionary_keys_size; ++i)
        key_columns.emplace_back(block.getByPosition(i).column);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();

    size_t block_attributes_skip_offset = dictionary_keys_size;

    const auto * min_range_column = block.getByPosition(block_attributes_skip_offset).column.get();
    const auto * max_range_column = block.getByPosition(block_attributes_skip_offset + 1).column.get();

    const NullMap * min_range_null_map = nullptr;
    const NullMap * max_range_null_map = nullptr;

    if (const auto * min_range_column_nullable = checkAndGetColumn<ColumnNullable>(min_range_column))
    {
        min_range_column = &min_range_column_nullable->getNestedColumn();
        min_range_null_map = &min_range_column_nullable->getNullMapColumn().getData();
    }

    if (const auto * max_range_column_nullable = checkAndGetColumn<ColumnNullable>(max_range_column))
    {
        max_range_column = &max_range_column_nullable->getNestedColumn();
        max_range_null_map = &max_range_column_nullable->getNullMapColumn().getData();
    }

    const auto * min_range_column_typed = typeid_cast<const RangeColumnType *>(min_range_column);
    if (!min_range_column_typed)
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Dictionary {} range min should be equal to {}",
            getFullName(),
            dict_struct.range_min->type->getName());

    const auto * max_range_column_typed = typeid_cast<const RangeColumnType *>(max_range_column);
    if (!max_range_column_typed)
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Dictionary {} range max should be equal to {}",
            getFullName(),
            dict_struct.range_max->type->getName());

    const auto & min_range_column_data = min_range_column_typed->getData();
    const auto & max_range_column_data = max_range_column_typed->getData();

    block_attributes_skip_offset += 2;

    Field column_value;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();

        RangeStorageType lower_bound = min_range_column_data[key_index];
        RangeStorageType upper_bound = max_range_column_data[key_index];

        bool invalid_range = false;

        if (unlikely(min_range_null_map && (*min_range_null_map)[key_index]))
        {
            lower_bound = std::numeric_limits<RangeStorageType>::min();
            invalid_range = true;
        }

        if (unlikely(max_range_null_map && (*max_range_null_map)[key_index]))
        {
            upper_bound = std::numeric_limits<RangeStorageType>::max();
            invalid_range = true;
        }

        if (unlikely(!configuration.convert_null_range_bound_to_open && invalid_range))
        {
            keys_extractor.rollbackCurrentKey();
            continue;
        }

        if constexpr (std::is_same_v<KeyType, StringRef>)
            key = copyStringInArena(string_arena, key);

        if (likely(lower_bound <= upper_bound))
        {
            for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
            {
                const auto & attribute_column = *block.getByPosition(attribute_index + block_attributes_skip_offset).column;
                auto & attribute = attributes[attribute_index];
                attribute_column.get(key_index, column_value);

                setAttributeValue(attribute, column_value);
            }

            auto interval = RangeInterval(lower_bound, upper_bound);
            auto it = key_attribute.container.find(key);

            if (it)
            {
                auto & intervals = it->getMapped();
                intervals.emplace(interval, element_count);
            }
            else
            {
                IntervalMap intervals;
                intervals.emplace(interval, element_count);
                key_attribute.container.insert({key, std::move(intervals)});
            }

            ++element_count;
        }

        keys_extractor.rollbackCurrentKey();
    }
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
template <typename T>
void RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::setAttributeValueImpl(Attribute & attribute, const Field & value)
{
    using ValueType = DictionaryValueType<T>;

    auto & container = std::get<AttributeContainerType<ValueType>>(attribute.container);
    container.emplace_back();

    if (unlikely(attribute.is_value_nullable.has_value()))
    {
        bool value_is_null = value.isNull();
        attribute.is_value_nullable->emplace_back(value_is_null);

        if (unlikely(value_is_null))
            return;
    }

    ValueType value_to_insert;

    if constexpr (std::is_same_v<T, String>)
    {
        const auto & string = value.get<String>();
        StringRef string_ref = copyStringInArena(string_arena, string);
        value_to_insert = string_ref;
    }
    else
    {
        value_to_insert = value.get<ValueType>();
    }

    container.back() = value_to_insert;
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
void RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::setAttributeValue(Attribute & attribute, const Field & value)
{
    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        setAttributeValueImpl<AttributeType>(attribute, value);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

template <DictionaryKeyType dictionary_key_type, typename RangeStorageDataType>
Pipe RangeHashedDictionary<dictionary_key_type, RangeStorageDataType>::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
{
    auto range_key_column = dict_struct.range_min->type->createColumn();
    auto range_min_column = dict_struct.range_min->type->createColumn();

    auto * range_key_column_typed = typeid_cast<RangeColumnType *>(range_key_column.get());
    auto * range_min_column_typed = typeid_cast<RangeColumnType *>(range_min_column.get());
    if (!range_min_column_typed || !range_key_column_typed)
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Dictionary {} range min should be equal to {}",
            getFullName(),
            dict_struct.range_min->type->getName());

    auto range_max_column = dict_struct.range_max->type->createColumn();

    auto * range_max_column_typed = typeid_cast<RangeColumnType *>(range_max_column.get());
    if (!range_max_column_typed)
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Dictionary {} range max should be equal to {}",
            getFullName(),
            dict_struct.range_max->type->getName());

    PaddedPODArray<KeyType> keys;
    auto & range_key_column_data = range_key_column_typed->getData();
    auto & range_min_column_data = range_min_column_typed->getData();
    auto & range_max_column_data = range_max_column_typed->getData();

    const auto & container = key_attribute.container;

    size_t container_size = container.size();

    keys.reserve(container_size);
    range_key_column_data.reserve(container_size);
    range_min_column_data.reserve(container_size);
    range_max_column_data.reserve(container_size);

    for (const auto & key : container)
    {
        for (const auto & [interval, _] : key.getMapped())
        {
            keys.push_back(key.getKey());
            range_key_column_data.push_back(interval.left);
            range_min_column_data.push_back(interval.left);
            range_max_column_data.push_back(interval.right);
        }
    }

    auto range_min_column_with_type = ColumnWithTypeAndName{std::move(range_min_column), dict_struct.range_min->type, dict_struct.range_min->name};
    auto range_max_column_with_type = ColumnWithTypeAndName{std::move(range_max_column), dict_struct.range_max->type, dict_struct.range_max->name};

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

    key_columns.emplace_back(ColumnWithTypeAndName{std::move(range_key_column), dict_struct.range_min->type, ""});

    ColumnsWithTypeAndName data_columns = {std::move(range_min_column_with_type), std::move(range_max_column_with_type)};

    std::shared_ptr<const IDictionary> dictionary = shared_from_this();
    auto coordinator = DictionarySourceCoordinator::create(dictionary, column_names, std::move(key_columns), std::move(data_columns), max_block_size);
    auto result = coordinator->read(num_streams);

    return result;
}

template <DictionaryKeyType dictionary_key_type>
static DictionaryPtr createRangeHashedDictionary(const std::string & full_name,
                            const DictionaryStructure & dict_struct,
                            const Poco::Util::AbstractConfiguration & config,
                            const std::string & config_prefix,
                            DictionarySourcePtr source_ptr)
{
    if (dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout 'range_hashed'");

    if (!dict_struct.range_min || !dict_struct.range_max)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout 'range_hashed' requires .structure.range_min and .structure.range_max",
            full_name);

    const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
    const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
    const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
    const bool convert_null_range_bound_to_open = config.getBool(config_prefix + ".convert_null_range_bound_to_open", true);

    RangeHashedDictionaryConfiguration configuration
    {
        .convert_null_range_bound_to_open = convert_null_range_bound_to_open,
        .require_nonempty = require_nonempty
    };

    TypeIndex type_index = dict_struct.range_min->type->getTypeId();

    DictionaryPtr result;

    auto call = [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using DataType = typename Types::LeftType;

        if constexpr (IsDataTypeDecimalOrNumber<DataType> || IsDataTypeDateOrDateTime<DataType> || IsDataTypeEnum<DataType>)
        {
            result = std::make_unique<RangeHashedDictionary<DictionaryKeyType::Simple, DataType>>(
                dict_id,
                dict_struct,
                std::move(source_ptr),
                dict_lifetime,
                configuration);

            return true;
        }

        return false;
    };

    if (!callOnIndexAndDataType<void>(type_index, call))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Dictionary structure type of 'range_min' and 'range_max' should be an Integer, Float, Decimal, Date, Date32, DateTime DateTime64, or Enum."
            " Actual 'range_min' and 'range_max' type is {}",
            dict_struct.range_min->type->getName());
    }

    return result;
}

void registerDictionaryRangeHashed(DictionaryFactory & factory)
{
    auto create_layout_simple = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr /* global_context */,
                             bool /*created_from_ddl*/) -> DictionaryPtr
    {
        return createRangeHashedDictionary<DictionaryKeyType::Simple>(full_name, dict_struct, config, config_prefix, std::move(source_ptr));
    };

    factory.registerLayout("range_hashed", create_layout_simple, false);

    auto create_layout_complex = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr /* context */,
                             bool /*created_from_ddl*/) -> DictionaryPtr
    {
        return createRangeHashedDictionary<DictionaryKeyType::Complex>(full_name, dict_struct, config, config_prefix, std::move(source_ptr));
    };

    factory.registerLayout("complex_key_range_hashed", create_layout_complex, true);
}

}
