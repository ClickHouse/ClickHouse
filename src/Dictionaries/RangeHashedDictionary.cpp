#include "RangeHashedDictionary.h"
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include <base/Typelists.h>
#include <Interpreters/castColumn.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/RangeDictionarySource.h>


namespace
{

using RangeStorageType = DB::RangeStorageType;

// Null values mean that specified boundary, either min or max is not set on range.
// To simplify comparison, null value of min bound should be bigger than any other value,
// and null value of maxbound - less than any value.
const RangeStorageType RANGE_MIN_NULL_VALUE = std::numeric_limits<RangeStorageType>::max();
const RangeStorageType RANGE_MAX_NULL_VALUE = std::numeric_limits<RangeStorageType>::lowest();

// Handle both kinds of null values: explicit nulls of NullableColumn and 'implicit' nulls of Date type.
RangeStorageType getColumnIntValueOrDefault(const DB::IColumn & column, size_t index, bool isDate, const RangeStorageType & default_value)
{
    if (column.isNullAt(index))
        return default_value;

    const RangeStorageType result = static_cast<RangeStorageType>(column.getInt(index));
    if (isDate && !DB::Range::isCorrectDate(result))
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

bool Range::isCorrectDate(const RangeStorageType & date)
{
    return 0 < date && date <= DATE_LUT_MAX_DAY_NUM;
}

bool Range::contains(const RangeStorageType & value) const
{
    return left <= value && value <= right;
}

static bool operator<(const Range & left, const Range & right)
{
    return std::tie(left.left, left.right) < std::tie(right.left, right.right);
}

template <DictionaryKeyType dictionary_key_type>
RangeHashedDictionary<dictionary_key_type>::RangeHashedDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    bool require_nonempty_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
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
    bool is_attribute_nullable = attribute.is_nullable;

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

    const auto & attribute = attributes.front();

    auto result = ColumnUInt8::create(keys_size);
    auto & out = result->getData();
    size_t keys_found = 0;

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        const auto & collection = std::get<CollectionType<ValueType>>(attribute.maps);

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            const auto key = keys_extractor.extractCurrentKey();
            const auto it = collection.find(key);

            if (it)
            {
                const auto date = dates[key_index];
                const auto & ranges_and_values = it->getMapped();
                const auto val_it = std::find_if(
                    std::begin(ranges_and_values),
                    std::end(ranges_and_values),
                    [date](const Value<ValueType> & v)
                    {
                        return v.range.contains(date);
                    });

                out[key_index] = val_it != std::end(ranges_and_values);
                keys_found += out[key_index];
            }
            else
            {
                out[key_index] = false;
            }

            keys_extractor.rollbackCurrentKey();
        }
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

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
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attributes.push_back(createAttribute(attribute));

        if (attribute.hierarchical)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hierarchical attributes not supported by {} dictionary.",
                            getDictionaryID().getNameForLogs());
    }
}

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::loadData()
{
    QueryPipeline pipeline(source_ptr->loadAll());

    PullingPipelineExecutor executor(pipeline);
    Block block;
    while (executor.pull(block))
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

        element_count += keys_size;

        // Support old behaviour, where invalid date means 'open range'.
        const bool is_date = isDate(dict_struct.range_min->type);

        const auto & min_range_column = unwrapNullableColumn(*block.safeGetByPosition(skip_keys_size_offset).column);
        const auto & max_range_column = unwrapNullableColumn(*block.safeGetByPosition(skip_keys_size_offset + 1).column);

        skip_keys_size_offset += 2;

        for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
        {
            const auto & attribute_column = *block.safeGetByPosition(attribute_index + skip_keys_size_offset).column;
            auto & attribute = attributes[attribute_index];

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
                    key = copyKeyInArena(key);

                setAttributeValue(attribute, key, Range{lower_bound, upper_bound}, attribute_column[key_index]);
                keys_extractor.rollbackCurrentKey();
            }

            keys_extractor.reset();
        }
    }

    if (require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "{}: dictionary source is empty and 'require_nonempty' property is set.");
}

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (const auto & attribute : attributes)
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            const auto & collection = std::get<CollectionType<ValueType>>(attribute.maps);
            bytes_allocated += sizeof(CollectionType<ValueType>) + collection.getBufferSizeInBytes();
            bucket_count = collection.getBufferSizeInCells();

            if constexpr (std::is_same_v<ValueType, StringRef>)
                bytes_allocated += sizeof(Arena) + attribute.string_arena->size();
        };

        callOnDictionaryAttributeType(attribute.type, type_call);
    }

    if constexpr (dictionary_key_type == DictionaryKeyType::Complex)
        bytes_allocated += complex_key_arena.size();
}

template <DictionaryKeyType dictionary_key_type>
typename RangeHashedDictionary<dictionary_key_type>::Attribute RangeHashedDictionary<dictionary_key_type>::createAttribute(const DictionaryAttribute & dictionary_attribute)
{
    Attribute attribute{dictionary_attribute.underlying_type, dictionary_attribute.is_nullable, {}, {}};

    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        if constexpr (std::is_same_v<AttributeType, String>)
            attribute.string_arena = std::make_unique<Arena>();

        attribute.maps = CollectionType<ValueType>();
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
    const auto & collection = std::get<CollectionType<AttributeType>>(attribute.maps);

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
        const auto it = collection.find(key);

        if (it)
        {
            const auto date = dates[key_index];
            const auto & ranges_and_values = it->getMapped();
            const auto val_it = std::find_if(
                std::begin(ranges_and_values),
                std::end(ranges_and_values),
                [date](const Value<AttributeType> & v)
                {
                    return v.range.contains(date);
                });

            if (val_it != std::end(ranges_and_values))
            {
                ++keys_found;
                auto & value = val_it->value;

                if constexpr (is_nullable)
                {
                    if (value.has_value())
                        set_value(key_index, *value, false);
                    else
                        set_value(key_index, default_value_extractor[key_index], true);
                }
                else
                {
                    set_value(key_index, *value, false);
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
template <typename T>
void RangeHashedDictionary<dictionary_key_type>::setAttributeValueImpl(Attribute & attribute, KeyType key, const Range & range, const Field & value)
{
    using ValueType = std::conditional_t<std::is_same_v<T, String>, StringRef, T>;
    auto & collection = std::get<CollectionType<ValueType>>(attribute.maps);

    Value<ValueType> value_to_insert;

    if (attribute.is_nullable && value.isNull())
    {
        value_to_insert = { range, {} };
    }
    else
    {
        if constexpr (std::is_same_v<T, String>)
        {
            const auto & string = value.get<String>();
            const auto * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
            const StringRef string_ref{string_in_arena, string.size()};
            value_to_insert = Value<ValueType>{ range, { string_ref }};
        }
        else
        {
            value_to_insert = Value<ValueType>{ range, { value.get<ValueType>() }};
        }
    }

    const auto it = collection.find(key);

    if (it)
    {
        auto & values = it->getMapped();

        const auto insert_it = std::lower_bound(
            std::begin(values),
            std::end(values),
            range,
            [](const Value<ValueType> & lhs, const Range & rhs_range)
            {
                return lhs.range < rhs_range;
            });

        values.insert(insert_it, std::move(value_to_insert));
    }
    else
    {
        collection.insert({key, Values<ValueType>{std::move(value_to_insert)}});
    }
}

template <DictionaryKeyType dictionary_key_type>
void RangeHashedDictionary<dictionary_key_type>::setAttributeValue(Attribute & attribute, KeyType key, const Range & range, const Field & value)
{
    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        setAttributeValueImpl<AttributeType>(attribute, key, range, value);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

template <DictionaryKeyType dictionary_key_type>
template <typename RangeType>
void RangeHashedDictionary<dictionary_key_type>::getKeysAndDates(
    PaddedPODArray<KeyType> & keys,
    PaddedPODArray<RangeType> & start_dates,
    PaddedPODArray<RangeType> & end_dates) const
{
    const auto & attribute = attributes.front();

    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        getKeysAndDates<ValueType>(attribute, keys, start_dates, end_dates);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

template <DictionaryKeyType dictionary_key_type>
template <typename T, typename RangeType>
void RangeHashedDictionary<dictionary_key_type>::getKeysAndDates(
    const Attribute & attribute,
    PaddedPODArray<KeyType> & keys,
    PaddedPODArray<RangeType> & start_dates,
    PaddedPODArray<RangeType> & end_dates) const
{
    const auto & collection = std::get<CollectionType<T>>(attribute.maps);

    keys.reserve(collection.size());
    start_dates.reserve(collection.size());
    end_dates.reserve(collection.size());

    const bool is_date = isDate(dict_struct.range_min->type);

    for (const auto & key : collection)
    {
        for (const auto & value : key.getMapped())
        {
            keys.push_back(key.getKey());
            start_dates.push_back(value.range.left);
            end_dates.push_back(value.range.right);

            if constexpr (std::numeric_limits<RangeType>::max() > DATE_LUT_MAX_DAY_NUM) /// Avoid warning about tautological comparison in next line.
                if (is_date && static_cast<UInt64>(end_dates.back()) > DATE_LUT_MAX_DAY_NUM)
                    end_dates.back() = 0;
        }
    }
}

template <DictionaryKeyType dictionary_key_type>
template <typename RangeType>
Pipe RangeHashedDictionary<dictionary_key_type>::readImpl(const Names & column_names, size_t max_block_size) const
{
    PaddedPODArray<KeyType> keys;
    PaddedPODArray<RangeType> start_dates;
    PaddedPODArray<RangeType> end_dates;
    getKeysAndDates(keys, start_dates, end_dates);

    using RangeDictionarySourceType = RangeDictionarySource<dictionary_key_type, RangeType>;

    auto source_data = RangeDictionarySourceData<dictionary_key_type, RangeType>(
        shared_from_this(),
        column_names,
        std::move(keys),
        std::move(start_dates),
        std::move(end_dates));
    auto source = std::make_shared<RangeDictionarySourceType>(std::move(source_data), max_block_size);

    return Pipe(source);
}

template <DictionaryKeyType dictionary_key_type>
StringRef RangeHashedDictionary<dictionary_key_type>::copyKeyInArena(StringRef key)
{
    size_t key_size = key.size;
    char * place_for_key = complex_key_arena.alloc(key_size);
    memcpy(reinterpret_cast<void *>(place_for_key), reinterpret_cast<const void *>(key.data), key_size);
    StringRef updated_key{place_for_key, key_size};
    return updated_key;
}

template <DictionaryKeyType dictionary_key_type>
struct RangeHashedDictionaryCallGetSourceImpl
{
    Pipe pipe;
    const RangeHashedDictionary<dictionary_key_type> * dict;
    const Names * column_names;
    size_t max_block_size;

    template <class RangeType>
    void operator()(Id<RangeType>)
    {
        const auto & type = dict->dict_struct.range_min->type;
        if (pipe.empty() && dynamic_cast<const DataTypeNumberBase<RangeType> *>(type.get()))
            pipe = dict->template readImpl<RangeType>(*column_names, max_block_size);
    }
};

template <DictionaryKeyType dictionary_key_type>
Pipe RangeHashedDictionary<dictionary_key_type>::read(const Names & column_names, size_t max_block_size) const
{
    RangeHashedDictionaryCallGetSourceImpl<dictionary_key_type> callable;
    callable.dict = this;
    callable.column_names = &column_names;
    callable.max_block_size = max_block_size;

    TLUtils::forEach(TLIntegral{}, callable);

    if (callable.pipe.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected range type for RangeHashed dictionary: {}",
            dict_struct.range_min->type->getName());

    return std::move(callable.pipe);
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

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        return std::make_unique<RangeHashedDictionary<DictionaryKeyType::Complex>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
    };
    factory.registerLayout("complex_key_range_hashed", create_layout_complex, true);
}

}
