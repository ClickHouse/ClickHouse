#include <Dictionaries/RangeHashedDictionary.h>

#include <Common/ArenaUtils.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

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
    extern const int TYPE_MISMATCH;
}

namespace
{
    template <typename F>
    void callOnRangeType(const DataTypePtr & range_type, F && func)
    {
        auto call = [&](const auto & types)
        {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::LeftType;

            if constexpr (IsDataTypeDecimalOrNumber<DataType> || IsDataTypeDateOrDateTime<DataType> || IsDataTypeEnum<DataType>)
            {
                using ColumnType = typename DataType::ColumnType;
                func(TypePair<ColumnType, void>());
                return true;
            }

            return false;
        };

        auto type_index = range_type->getTypeId();
        if (!callOnIndexAndDataType<void>(type_index, call))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Dictionary structure type of 'range_min' and 'range_max' should be an Integer, Float, Decimal, Date, Date32, DateTime DateTime64, or Enum."
                " Actual 'range_min' and 'range_max' type is {}",
                range_type->getName());
        }
    }
}

template <DictionaryKeyType dictionary_key_type>
RangeHashedDictionary<dictionary_key_type>::RangeHashedDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    DictionaryLifetime dict_lifetime_,
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

template <DictionaryKeyType dictionary_key_type>
ColumnPtr RangeHashedDictionary<dictionary_key_type>::getColumn(
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

    /// Cast range column to storage type
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
        result = ColumnNullable::create(result, std::move(col_null_map_to));

    return result;
}

template <DictionaryKeyType dictionary_key_type>
ColumnPtr RangeHashedDictionary<dictionary_key_type>::getColumnInternal(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const PaddedPODArray<UInt64> & key_to_index) const
{
    ColumnPtr result;

    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);
    const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    const auto & attribute = attributes[attribute_index];

    size_t keys_size = key_to_index.size();
    bool is_attribute_nullable = attribute.is_value_nullable.has_value();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;
    if (is_attribute_nullable)
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

        auto column = ColumnProvider::getColumn(dictionary_attribute, keys_size);

        if constexpr (std::is_same_v<ValueType, Array>)
        {
            auto * out = column.get();

            getItemsInternalImpl<ValueType, false>(
                attribute,
                key_to_index,
                [&](size_t, const Array & value, bool)
                {
                    out->insert(value);
                });
        }
        else if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();

            if (is_attribute_nullable)
                getItemsInternalImpl<ValueType, true>(
                    attribute,
                    key_to_index,
                    [&](size_t row, const StringRef value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out->insertData(value.data, value.size);
                    });
            else
                getItemsInternalImpl<ValueType, false>(
                    attribute,
                    key_to_index,
                    [&](size_t, const StringRef value, bool)
                    {
                        out->insertData(value.data, value.size);
                    });
        }
        else
        {
            auto & out = column->getData();

            if (is_attribute_nullable)
                getItemsInternalImpl<ValueType, true>(
                    attribute,
                    key_to_index,
                    [&](size_t row, const auto value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out[row] = value;
                    });
            else
                getItemsInternalImpl<ValueType, false>(
                    attribute,
                    key_to_index,
                    [&](size_t row, const auto value, bool)
                    {
                        out[row] = value;
                    });
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    if (is_attribute_nullable)
        result = ColumnNullable::create(result, std::move(col_null_map_to));

    return result;
}

template <DictionaryKeyType dictionary_key_type>
ColumnUInt8::Ptr RangeHashedDictionary<dictionary_key_type>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
    {
        auto key_types_copy = key_types;
        key_types_copy.pop_back();
        dict_struct.validateKeyTypes(key_types_copy);
    }

    /// Cast range column to storage type
    auto range_storage_column = key_columns.back();
    ColumnWithTypeAndName column_to_cast = {range_storage_column->convertToFullColumnIfConst(), key_types.back(), ""};
    auto range_column_updated = castColumnAccurate(column_to_cast, dict_struct.range_min->type);
    auto key_columns_copy = key_columns;
    key_columns_copy.pop_back();

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns_copy, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();

    auto result = ColumnUInt8::create(keys_size);
    auto & out = result->getData();
    size_t keys_found = 0;

    callOnRangeType(dict_struct.range_min->type, [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using RangeColumnType = typename Types::LeftType;
        using RangeStorageType = typename RangeColumnType::ValueType;

        const auto * range_column_typed = typeid_cast<const RangeColumnType *>(range_column_updated.get());
        if (!range_column_typed)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Dictionary {} range column type should be equal to {}",
                getFullName(),
                dict_struct.range_min->type->getName());
        const auto & range_column_data = range_column_typed->getData();

        const auto & key_attribute_container = std::get<KeyAttributeContainerType<RangeStorageType>>(key_attribute.container);

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            const auto key = keys_extractor.extractCurrentKey();
            const auto it = key_attribute_container.find(key);

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
    });

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::createAttributes()
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

    callOnRangeType(dict_struct.range_min->type, [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using RangeColumnType = typename Types::LeftType;
        using RangeStorageType = typename RangeColumnType::ValueType;

        key_attribute.container = KeyAttributeContainerType<RangeStorageType>();
        key_attribute.invalid_intervals_container = InvalidIntervalsContainerType<RangeStorageType>();
    });
}

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::loadData()
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

    callOnRangeType(dict_struct.range_min->type, [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using RangeColumnType = typename Types::LeftType;
        using RangeStorageType = typename RangeColumnType::ValueType;

        auto & key_attribute_container = std::get<KeyAttributeContainerType<RangeStorageType>>(key_attribute.container);

        for (auto & [_, intervals] : key_attribute_container)
            intervals.build();
    });

    if (configuration.require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "{}: dictionary source is empty and 'require_nonempty' property is set.");
}

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::calculateBytesAllocated()
{
    callOnRangeType(dict_struct.range_min->type, [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using RangeColumnType = typename Types::LeftType;
        using RangeStorageType = typename RangeColumnType::ValueType;

        auto & key_attribute_container = std::get<KeyAttributeContainerType<RangeStorageType>>(key_attribute.container);

        bucket_count = key_attribute_container.getBufferSizeInCells();
        bytes_allocated += key_attribute_container.getBufferSizeInBytes();

        for (auto & [_, intervals] : key_attribute_container)
            bytes_allocated += intervals.getSizeInBytes();
    });

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

template <DictionaryKeyType dictionary_key_type>
typename RangeHashedDictionary<dictionary_key_type>::Attribute RangeHashedDictionary<dictionary_key_type>::createAttribute(const DictionaryAttribute & dictionary_attribute)
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

template <DictionaryKeyType dictionary_key_type>
template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void RangeHashedDictionary<dictionary_key_type>::getItemsImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & attribute_container = std::get<AttributeContainerType<AttributeType>>(attribute.container);

    size_t keys_found = 0;

    auto range_column = key_columns.back();
    auto key_columns_copy = key_columns;
    key_columns_copy.pop_back();

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns_copy, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();

    callOnRangeType(dict_struct.range_min->type, [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using RangeColumnType = typename Types::LeftType;
        using RangeStorageType = typename RangeColumnType::ValueType;
        using RangeInterval = Interval<RangeStorageType>;

        const auto * range_column_typed = typeid_cast<const RangeColumnType *>(range_column.get());
        if (!range_column_typed)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Dictionary {} range column type should be equal to {}",
                getFullName(),
                dict_struct.range_min->type->getName());

        const auto & range_column_data = range_column_typed->getData();

        const auto & key_attribute_container = std::get<KeyAttributeContainerType<RangeStorageType>>(key_attribute.container);

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys_extractor.extractCurrentKey();
            const auto it = key_attribute_container.find(key);

            if (it)
            {
                const auto date = range_column_data[key_index];
                const auto & interval_tree = it->getMapped();

                size_t value_index = 0;
                std::optional<RangeInterval> range;

                interval_tree.find(date, [&](auto & interval, auto & interval_value_index)
                {
                    if (range)
                    {
                        if (likely(configuration.lookup_strategy == RangeHashedDictionaryLookupStrategy::min) && interval < *range)
                        {
                            range = interval;
                            value_index = interval_value_index;
                        }
                        else if (configuration.lookup_strategy == RangeHashedDictionaryLookupStrategy::max && interval > * range)
                        {
                            range = interval;
                            value_index = interval_value_index;
                        }
                    }
                    else
                    {
                        range = interval;
                        value_index = interval_value_index;
                    }

                    return true;
                });

                if (range.has_value())
                {
                    ++keys_found;

                    AttributeType value = attribute_container[value_index];

                    if constexpr (is_nullable)
                    {
                        bool is_null = (*attribute.is_value_nullable)[value_index];

                        if (!is_null)
                            set_value(key_index, value, false);
                        else
                            set_value(key_index, default_value_extractor[key_index], true);
                    }
                    else
                    {
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
    });

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type>
template <typename AttributeType, bool is_nullable, typename ValueSetter>
void RangeHashedDictionary<dictionary_key_type>::getItemsInternalImpl(
        const Attribute & attribute,
        const PaddedPODArray<UInt64> & key_to_index,
        ValueSetter && set_value) const
{
    size_t keys_size = key_to_index.size();

    const auto & container = std::get<AttributeContainerType<AttributeType>>(attribute.container);
    size_t container_size = container.size();

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        UInt64 container_index = key_to_index[key_index];

        if (unlikely(container_index >= container_size))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Dictionary {} expected attribute container index {} must be less than attribute container size {}",
                getFullName(),
                container_index,
                container_size
            );
        }

        AttributeType value = container[container_index];

        if constexpr (is_nullable)
        {
            bool is_null = (*attribute.is_value_nullable)[container_index];

            if (!is_null)
                set_value(key_index, value, false);
            else
                set_value(key_index, value, true);
        }
        else
        {
            set_value(key_index, value, false);
        }
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_size, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::updateData()
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

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::blockToAttributes(const Block & block)
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

    callOnRangeType(dict_struct.range_min->type, [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using RangeColumnType = typename Types::LeftType;
        using RangeStorageType = typename RangeColumnType::ValueType;

        const auto * min_range_column_typed = typeid_cast<const RangeColumnType *>(min_range_column);
        if (!min_range_column_typed)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Dictionary {} range min column type should be equal to {}",
                getFullName(),
                dict_struct.range_min->type->getName());

        const auto * max_range_column_typed = typeid_cast<const RangeColumnType *>(max_range_column);
        if (!max_range_column_typed)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Dictionary {} range max column type should be equal to {}",
                getFullName(),
                dict_struct.range_max->type->getName());

        const auto & min_range_column_data = min_range_column_typed->getData();
        const auto & max_range_column_data = max_range_column_typed->getData();

        auto & key_attribute_container = std::get<KeyAttributeContainerType<RangeStorageType>>(key_attribute.container);
        auto & invalid_intervals_container = std::get<InvalidIntervalsContainerType<RangeStorageType>>(key_attribute.invalid_intervals_container);

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

            for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
            {
                const auto & attribute_column = *block.getByPosition(attribute_index + block_attributes_skip_offset).column;
                auto & attribute = attributes[attribute_index];
                attribute_column.get(key_index, column_value);

                setAttributeValue(attribute, column_value);
            }

            auto interval = Interval<RangeStorageType>(lower_bound, upper_bound);
            auto it = key_attribute_container.find(key);

            bool emplaced_in_interval_tree = false;

            if (it)
            {
                auto & intervals = it->getMapped();
                emplaced_in_interval_tree = intervals.emplace(interval, element_count);
            }
            else
            {
                IntervalMap<RangeStorageType> intervals;
                emplaced_in_interval_tree = intervals.emplace(interval, element_count);
                key_attribute_container.insert({key, std::move(intervals)});
            }

            if (unlikely(!emplaced_in_interval_tree))
            {
                InvalidIntervalWithKey<RangeStorageType> invalid_interval{key, interval, element_count};
                invalid_intervals_container.emplace_back(invalid_interval);
            }

            ++element_count;
            keys_extractor.rollbackCurrentKey();
        }
    });
}

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::setAttributeValue(Attribute & attribute, const Field & value)
{
    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

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

        if constexpr (std::is_same_v<AttributeType, String>)
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
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

template <DictionaryKeyType dictionary_key_type>
Pipe RangeHashedDictionary<dictionary_key_type>::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
{
    auto key_to_index_column = ColumnUInt64::create();
    auto range_min_column = dict_struct.range_min->type->createColumn();
    auto range_max_column = dict_struct.range_max->type->createColumn();

    PaddedPODArray<KeyType> keys;

    callOnRangeType(dict_struct.range_min->type, [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using RangeColumnType = typename Types::LeftType;
        using RangeStorageType = typename RangeColumnType::ValueType;

        auto * range_min_column_typed = typeid_cast<RangeColumnType *>(range_min_column.get());
        if (!range_min_column_typed)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Dictionary {} range min column type should be equal to {}",
                getFullName(),
                dict_struct.range_min->type->getName());

        auto * range_max_column_typed = typeid_cast<RangeColumnType *>(range_max_column.get());
        if (!range_max_column_typed)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Dictionary {} range max column type should be equal to {}",
                getFullName(),
                dict_struct.range_max->type->getName());

        auto & key_to_index_column_data = key_to_index_column->getData();
        auto & range_min_column_data = range_min_column_typed->getData();
        auto & range_max_column_data = range_max_column_typed->getData();

        const auto & container = std::get<KeyAttributeContainerType<RangeStorageType>>(key_attribute.container);
        const auto & invalid_intervals_container = std::get<InvalidIntervalsContainerType<RangeStorageType>>(key_attribute.invalid_intervals_container);

        keys.reserve(element_count);
        key_to_index_column_data.reserve(element_count);
        range_min_column_data.reserve(element_count);
        range_max_column_data.reserve(element_count);

        for (const auto & key : container)
        {
            for (const auto & [interval, index] : key.getMapped())
            {
                keys.emplace_back(key.getKey());
                key_to_index_column_data.emplace_back(index);
                range_min_column_data.push_back(interval.left);
                range_max_column_data.push_back(interval.right);
            }
        }

        for (const auto & invalid_interval_with_key : invalid_intervals_container)
        {
            keys.emplace_back(invalid_interval_with_key.key);
            key_to_index_column_data.emplace_back(invalid_interval_with_key.attribute_value_index);
            range_min_column_data.push_back(invalid_interval_with_key.interval.left);
            range_max_column_data.push_back(invalid_interval_with_key.interval.right);
        }
    });

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

    key_columns.emplace_back(ColumnWithTypeAndName{std::move(key_to_index_column), std::make_shared<DataTypeUInt64>(), ""});

    ColumnsWithTypeAndName data_columns = {std::move(range_min_column_with_type), std::move(range_max_column_with_type)};

    std::shared_ptr<const IDictionary> dictionary = shared_from_this();

    DictionarySourceCoordinator::ReadColumnsFunc read_keys_func = [dictionary_copy = dictionary](
        const Strings & attribute_names,
        const DataTypes & result_types,
        const Columns & key_columns,
        const DataTypes,
        const Columns &)
    {
        auto range_dictionary_ptr = std::static_pointer_cast<const RangeHashedDictionary<dictionary_key_type>>(dictionary_copy);

        size_t attribute_names_size = attribute_names.size();

        Columns result;
        result.reserve(attribute_names_size);

        auto key_column = key_columns.back();

        const auto * key_to_index_column = typeid_cast<const ColumnUInt64 *>(key_column.get());
        if (!key_to_index_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Dictionary {} read expect indexes column with type UInt64",
                range_dictionary_ptr->getFullName());

        const auto & data = key_to_index_column->getData();

        for (size_t i = 0; i < attribute_names_size; ++i)
        {
            const auto & attribute_name = attribute_names[i];
            const auto & result_type = result_types[i];

            result.emplace_back(range_dictionary_ptr->getColumnInternal(attribute_name, result_type, data));
        }

        return result;
    };

    auto coordinator = DictionarySourceCoordinator::create(
        dictionary,
        column_names,
        std::move(key_columns),
        std::move(data_columns),
        max_block_size,
        std::move(read_keys_func));
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
    static constexpr auto layout_name = dictionary_key_type == DictionaryKeyType::Simple ? "range_hashed" : "complex_key_range_hashed";

    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        if (dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout 'range_hashed'");
    }
    else
    {
        if (dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for dictionary of layout 'complex_key_range_hashed'");
    }

    if (!dict_struct.range_min || !dict_struct.range_max)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' requires .structure.range_min and .structure.range_max",
            full_name,
            layout_name);

    const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
    const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
    const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

    String dictionary_layout_prefix = config_prefix + ".layout." + layout_name;
    const bool convert_null_range_bound_to_open = config.getBool(dictionary_layout_prefix + ".convert_null_range_bound_to_open", true);
    String range_lookup_strategy = config.getString(dictionary_layout_prefix + ".range_lookup_strategy", "min");
    RangeHashedDictionaryLookupStrategy lookup_strategy = RangeHashedDictionaryLookupStrategy::min;

    if (range_lookup_strategy == "min")
        lookup_strategy = RangeHashedDictionaryLookupStrategy::min;
    else if (range_lookup_strategy == "max")
        lookup_strategy = RangeHashedDictionaryLookupStrategy::max;

    RangeHashedDictionaryConfiguration configuration
    {
        .convert_null_range_bound_to_open = convert_null_range_bound_to_open,
        .lookup_strategy = lookup_strategy,
        .require_nonempty = require_nonempty
    };

    DictionaryPtr result = std::make_unique<RangeHashedDictionary<dictionary_key_type>>(
        dict_id,
        dict_struct,
        std::move(source_ptr),
        dict_lifetime,
        configuration);

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
