#include <Dictionaries/RangeHashedDictionary.h>

#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/castColumn.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySource.h>


namespace
{

using RangeStorageType = DB::RangeStorageType;

// Null values mean that specified boundary, either min or max is not set on range.
// To simplify comparison, null value of min bound should be bigger than any other value,
// and null value of maxbound - less than any value.
const RangeStorageType RANGE_MIN_NULL_VALUE = std::numeric_limits<RangeStorageType>::max();
const RangeStorageType RANGE_MAX_NULL_VALUE = std::numeric_limits<RangeStorageType>::lowest();

bool isCorrectDate(const RangeStorageType & date)
{
    return 0 < date && date <= DATE_LUT_MAX_DAY_NUM;
}

// Handle both kinds of null values: explicit nulls of NullableColumn and 'implicit' nulls of Date type.
RangeStorageType getColumnIntValueOrDefault(const DB::IColumn & column, size_t index, bool isDate, const RangeStorageType & default_value)
{
    if (column.isNullAt(index))
        return default_value;

    const RangeStorageType result = static_cast<RangeStorageType>(column.getInt(index));
    if (isDate && !isCorrectDate(result))
        return default_value;

    return result;
}

const DB::IColumn & unwrapNullableColumn(const DB::IColumn & column)
{
    if (const auto * m = DB::checkAndGetColumn<DB::ColumnNullable>(&column))
    {
        return m->getNestedColumn();
    }

    return column;
}

}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
}


template <DictionaryKeyType dictionary_key_type>
RangeHashedDictionary<dictionary_key_type>::RangeHashedDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    bool require_nonempty_,
    BlockPtr update_field_loaded_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
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

    /// Cast second column to storage type
    Columns modified_key_columns = key_columns;
    auto range_storage_column = key_columns.back();
    ColumnWithTypeAndName column_to_cast = {range_storage_column->convertToFullColumnIfConst(), key_types.back(), ""};
    auto range_column_storage_type = std::make_shared<DataTypeInt64>();
    modified_key_columns.back() = castColumnAccurate(column_to_cast, range_column_storage_type);

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

template <DictionaryKeyType dictionary_key_type>
ColumnUInt8::Ptr RangeHashedDictionary<dictionary_key_type>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
    {
        auto key_types_copy = key_types;
        key_types_copy.pop_back();
        dict_struct.validateKeyTypes(key_types_copy);
    }

    auto range_column_storage_type = std::make_shared<DataTypeInt64>();
    auto range_storage_column = key_columns.back();
    ColumnWithTypeAndName column_to_cast = {range_storage_column->convertToFullColumnIfConst(), key_types.back(), ""};
    auto range_column_updated = castColumnAccurate(column_to_cast, range_column_storage_type);
    PaddedPODArray<RangeStorageType> range_backup_storage;
    const PaddedPODArray<RangeStorageType> & dates = getColumnVectorData(this, range_column_updated, range_backup_storage);

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
            const auto date = dates[key_index];
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

    auto & key_attribute_container = key_attribute.container;

    for (auto & [_, intervals] : key_attribute_container)
        intervals.build();

    if (require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "{}: dictionary source is empty and 'require_nonempty' property is set.");
}

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::calculateBytesAllocated()
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
    const auto & container = std::get<AttributeContainerType<AttributeType>>(attribute.container);

    size_t keys_found = 0;

    PaddedPODArray<RangeStorageType> range_backup_storage;
    const auto & dates = getColumnVectorData(this, key_columns.back(), range_backup_storage);
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
            const auto date = dates[key_index];
            const auto & interval_tree = it->getMapped();

            size_t min_value_index = 0;
            std::optional<RangeInterval> min_range;

            interval_tree.find(date, [&](auto & interval, auto & value)
            {
                if (min_range && interval < *min_range)
                {
                    min_range = interval;
                    min_value_index = value;
                }
                else
                {
                    min_range = interval;
                    min_value_index = value;
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
void RangeHashedDictionary<dictionary_key_type>::blockToAttributes(const Block & block [[maybe_unused]])
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

    // Support old behaviour, where invalid date means 'open range'.
    const bool is_date = isDate(dict_struct.range_min->type);

    size_t block_attributes_skip_offset = dictionary_keys_size;

    const auto & min_range_column = unwrapNullableColumn(*block.getByPosition(block_attributes_skip_offset).column);
    const auto & max_range_column = unwrapNullableColumn(*block.getByPosition(block_attributes_skip_offset + 1).column);

    block_attributes_skip_offset += 2;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();

        RangeStorageType lower_bound;
        RangeStorageType upper_bound;

        if (is_date)
        {
            lower_bound = getColumnIntValueOrDefault(min_range_column, key_index, is_date, 0);
            upper_bound = getColumnIntValueOrDefault(max_range_column, key_index, is_date, DATE_LUT_MAX_DAY_NUM + 1);
        }
        else
        {
            lower_bound = getColumnIntValueOrDefault(min_range_column, key_index, is_date, RANGE_MIN_NULL_VALUE);
            upper_bound = getColumnIntValueOrDefault(max_range_column, key_index, is_date, RANGE_MAX_NULL_VALUE);
        }

        if constexpr (std::is_same_v<KeyType, StringRef>)
            key = copyStringInArena(string_arena, key);

        if (likely(lower_bound <= upper_bound))
        {
            for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
            {
                const auto & attribute_column = *block.getByPosition(attribute_index + block_attributes_skip_offset).column;
                auto & attribute = attributes[attribute_index];
                setAttributeValue(attribute, attribute_column[key_index]);
            }
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
        keys_extractor.rollbackCurrentKey();
    }
}

template <DictionaryKeyType dictionary_key_type>
template <typename T>
void RangeHashedDictionary<dictionary_key_type>::setAttributeValueImpl(Attribute & attribute, const Field & value)
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

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::setAttributeValue(Attribute & attribute, const Field & value)
{
    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        setAttributeValueImpl<AttributeType>(attribute, value);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

template <DictionaryKeyType dictionary_key_type>
template <typename RangeType>
void RangeHashedDictionary<dictionary_key_type>::getKeysAndRangeValues(
    PaddedPODArray<KeyType> & keys,
    PaddedPODArray<RangeType> & range_start_values,
    PaddedPODArray<RangeType> & range_end_values) const
{
    const auto & container = key_attribute.container;

    keys.reserve(container.size());
    range_start_values.reserve(container.size());
    range_end_values.reserve(container.size());

    const bool is_date = isDate(dict_struct.range_min->type);
    (void)(is_date);

    for (const auto & key : container)
    {
        for (const auto & [interval, _] : key.getMapped())
        {
            keys.push_back(key.getKey());
            range_start_values.push_back(interval.left);
            range_end_values.push_back(interval.right);

            /// Avoid warning about tautological comparison in next line.
            if constexpr (std::numeric_limits<RangeType>::max() > DATE_LUT_MAX_DAY_NUM)
                if (is_date && static_cast<UInt64>(range_end_values.back()) > DATE_LUT_MAX_DAY_NUM)
                    range_end_values.back() = 0;
        }
    }
}

template <DictionaryKeyType dictionary_key_type>
template <typename RangeType>
PaddedPODArray<Int64> RangeHashedDictionary<dictionary_key_type>::makeKeysValues(
    const PaddedPODArray<RangeType> & range_start_values,
    const PaddedPODArray<RangeType> & range_end_values) const
{
    PaddedPODArray<Int64> keys(range_start_values.size());

    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (isCorrectDate(range_start_values[i]))
            keys[i] = range_start_values[i]; // NOLINT
        else
            keys[i] = range_end_values[i]; // NOLINT
    }

    return keys;
}

template <DictionaryKeyType dictionary_key_type>
Pipe RangeHashedDictionary<dictionary_key_type>::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
{
    ColumnsWithTypeAndName key_columns;
    ColumnWithTypeAndName range_min_column;
    ColumnWithTypeAndName range_max_column;

    auto type_call = [&](const auto & types) mutable -> bool
    {
        using Types = std::decay_t<decltype(types)>;
        using LeftDataType = typename Types::LeftType;

        if constexpr (IsDataTypeNumber<LeftDataType> ||
            std::is_same_v<LeftDataType, DataTypeDate> ||
            std::is_same_v<LeftDataType, DataTypeDate32> ||
            std::is_same_v<LeftDataType, DataTypeDateTime>)
        {
            using RangeType = typename LeftDataType::FieldType;

            PaddedPODArray<KeyType> keys;
            PaddedPODArray<RangeType> range_start;
            PaddedPODArray<RangeType> range_end;
            getKeysAndRangeValues(keys, range_start, range_end);

            auto key_column = getColumnFromPODArray(makeKeysValues(range_start, range_end));

            auto range_start_column = getColumnFromPODArray(std::move(range_start));
            range_min_column = ColumnWithTypeAndName{std::move(range_start_column), dict_struct.range_min->type, dict_struct.range_min->name};

            auto range_end_column = getColumnFromPODArray(std::move(range_end));
            range_max_column = ColumnWithTypeAndName{std::move(range_end_column), dict_struct.range_max->type, dict_struct.range_max->name};

            if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
            {
                auto keys_column = getColumnFromPODArray(std::move(keys));
                key_columns = {ColumnWithTypeAndName(std::move(keys_column), std::make_shared<DataTypeUInt64>(), dict_struct.id->name)};
            }
            else
            {
                key_columns = deserializeColumnsWithTypeAndNameFromKeys(dict_struct, keys, 0, keys.size());
            }

            key_columns.emplace_back(ColumnWithTypeAndName{std::move(key_column), std::make_shared<DataTypeInt64>(), ""});

            return true;
        }
        else
        {
            return false;
        }
    };

    auto type = dict_struct.range_min->type;
    if (!callOnIndexAndDataType<void>(type->getTypeId(), type_call))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RangeHashedDictionary min max range type should be numeric");

    ColumnsWithTypeAndName data_columns = {std::move(range_min_column), std::move(range_max_column)};

    std::shared_ptr<const IDictionary> dictionary = shared_from_this();
    auto coordinator = DictionarySourceCoordinator::create(dictionary, column_names, std::move(key_columns), std::move(data_columns), max_block_size);
    auto result = coordinator->read(num_streams);

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
        if (dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout 'range_hashed'");

        if (!dict_struct.range_min || !dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: dictionary of layout 'range_hashed' requires .structure.range_min and .structure.range_max",
                full_name);

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        return std::make_unique<RangeHashedDictionary<DictionaryKeyType::Simple>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
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
        if (dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for dictionary of layout 'complex_key_range_hashed'");

        if (!dict_struct.range_min || !dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: dictionary of layout 'complex_key_range_hashed' requires .structure.range_min and .structure.range_max",
                full_name);

        if (dict_struct.attributes.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Empty attributes are not supported for dictionary of layout 'complex_key_range_hashed'");

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        return std::make_unique<RangeHashedDictionary<DictionaryKeyType::Complex>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
    };
    factory.registerLayout("complex_key_range_hashed", create_layout_complex, true);
}

}
