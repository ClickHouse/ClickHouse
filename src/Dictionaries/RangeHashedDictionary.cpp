#include "RangeHashedDictionary.h"
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Common/TypeList.h>
#include <common/range.h>
#include "DictionaryFactory.h"
#include "RangeDictionaryBlockInputStream.h"
#include <Interpreters/castColumn.h>
#include <DataTypes/DataTypesDecimal.h>

namespace
{
using RangeStorageType = DB::RangeHashedDictionary::RangeStorageType;

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
    if (isDate && !DB::RangeHashedDictionary::Range::isCorrectDate(result))
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
    extern const int TYPE_MISMATCH;
    extern const int UNSUPPORTED_METHOD;
}

bool RangeHashedDictionary::Range::isCorrectDate(const RangeStorageType & date)
{
    return 0 < date && date <= DATE_LUT_MAX_DAY_NUM;
}

bool RangeHashedDictionary::Range::contains(const RangeStorageType & value) const
{
    return left <= value && value <= right;
}

static bool operator<(const RangeHashedDictionary::Range & left, const RangeHashedDictionary::Range & right)
{
    return std::tie(left.left, left.right) < std::tie(right.left, right.right);
}


RangeHashedDictionary::RangeHashedDictionary(
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

ColumnPtr RangeHashedDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    const ColumnPtr & default_values_column) const
{
    ColumnPtr result;

    const auto & attribute = getAttribute(attribute_name);
    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);

    auto keys_size = key_columns.front()->size();

    /// Cast second column to storage type
    Columns modified_key_columns = key_columns;

    auto range_storage_column = key_columns[1];
    ColumnWithTypeAndName column_to_cast = {range_storage_column->convertToFullColumnIfConst(), key_types[1], ""};

    auto range_column_storage_type = std::make_shared<DataTypeInt64>();
    modified_key_columns[1] = castColumnAccurate(column_to_cast, range_column_storage_type);

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

ColumnUInt8::Ptr RangeHashedDictionary::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    auto range_storage_column = key_columns[1];
    ColumnWithTypeAndName column_to_cast = {range_storage_column->convertToFullColumnIfConst(), key_types[1], ""};

    auto range_column_storage_type = std::make_shared<DataTypeInt64>();
    auto range_column_updated = castColumnAccurate(column_to_cast, range_column_storage_type);

    PaddedPODArray<UInt64> key_backup_storage;
    PaddedPODArray<RangeStorageType> range_backup_storage;

    const PaddedPODArray<UInt64> & ids = getColumnVectorData(this, key_columns[0], key_backup_storage);
    const PaddedPODArray<RangeStorageType> & dates = getColumnVectorData(this, range_column_updated, range_backup_storage);

    const auto & attribute = attributes.front();

    ColumnUInt8::Ptr result;

    size_t keys_found = 0;

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        result = hasKeysImpl<ValueType>(attribute, ids, dates, keys_found);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    query_count.fetch_add(ids.size(), std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

template <typename AttributeType>
ColumnUInt8::Ptr RangeHashedDictionary::hasKeysImpl(
    const Attribute & attribute,
    const PaddedPODArray<UInt64> & ids,
    const PaddedPODArray<RangeStorageType> & dates,
    size_t & keys_found) const
{
    auto result = ColumnUInt8::create(ids.size());
    auto& out = result->getData();

    const auto & attr = *std::get<Ptr<AttributeType>>(attribute.maps);

    keys_found = 0;

    for (const auto row : collections::range(0, ids.size()))
    {
        const auto it = attr.find(ids[row]);

        if (it)
        {
            const auto date = dates[row];
            const auto & ranges_and_values = it->getMapped();
            const auto val_it = std::find_if(
                std::begin(ranges_and_values),
                std::end(ranges_and_values),
                [date](const Value<AttributeType> & v)
                {
                    return v.range.contains(date);
                });

            out[row] = val_it != std::end(ranges_and_values);
            keys_found += out[row];
        }
        else
            out[row] = false;
    }

    return result;
}

void RangeHashedDictionary::createAttributes()
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

void RangeHashedDictionary::loadData()
{
    auto stream = source_ptr->loadAll();
    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const auto & id_column = *block.safeGetByPosition(0).column;

        // Support old behaviour, where invalid date means 'open range'.
        const bool is_date = isDate(dict_struct.range_min->type);

        const auto & min_range_column = unwrapNullableColumn(*block.safeGetByPosition(1).column);
        const auto & max_range_column = unwrapNullableColumn(*block.safeGetByPosition(2).column);

        element_count += id_column.size();

        for (const auto attribute_idx : collections::range(0, attributes.size()))
        {
            const auto & attribute_column = *block.safeGetByPosition(attribute_idx + 3).column;
            auto & attribute = attributes[attribute_idx];

            for (const auto row_idx : collections::range(0, id_column.size()))
            {
                RangeStorageType lower_bound;
                RangeStorageType upper_bound;

                if (is_date)
                {
                    lower_bound = getColumnIntValueOrDefault(min_range_column, row_idx, is_date, 0);
                    upper_bound = getColumnIntValueOrDefault(max_range_column, row_idx, is_date, DATE_LUT_MAX_DAY_NUM + 1);
                }
                else
                {
                    lower_bound = getColumnIntValueOrDefault(min_range_column, row_idx, is_date, RANGE_MIN_NULL_VALUE);
                    upper_bound = getColumnIntValueOrDefault(max_range_column, row_idx, is_date, RANGE_MAX_NULL_VALUE);
                }

                setAttributeValue(attribute, id_column.getUInt(row_idx), Range{lower_bound, upper_bound}, attribute_column[row_idx]);
            }
        }
    }

    stream->readSuffix();

    if (require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "{}: dictionary source is empty and 'require_nonempty' property is set.");
}

template <typename T>
void RangeHashedDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & map_ref = std::get<Ptr<T>>(attribute.maps);
    bytes_allocated += sizeof(Collection<T>) + map_ref->getBufferSizeInBytes();
    bucket_count = map_ref->getBufferSizeInCells();
}

template <>
void RangeHashedDictionary::addAttributeSize<String>(const Attribute & attribute)
{
    addAttributeSize<StringRef>(attribute);
    bytes_allocated += sizeof(Arena) + attribute.string_arena->size();
}

void RangeHashedDictionary::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (const auto & attribute : attributes)
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            addAttributeSize<AttributeType>(attribute);
        };

        callOnDictionaryAttributeType(attribute.type, type_call);
    }
}

RangeHashedDictionary::Attribute RangeHashedDictionary::createAttribute(const DictionaryAttribute & dictionary_attribute)
{
    Attribute attribute{dictionary_attribute.underlying_type, dictionary_attribute.is_nullable, {}, {}};

    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        if constexpr (std::is_same_v<AttributeType, String>)
            attribute.string_arena = std::make_unique<Arena>();

        attribute.maps = std::make_unique<Collection<ValueType>>();
    };

    callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);

    return attribute;
}

template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void RangeHashedDictionary::getItemsImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    PaddedPODArray<UInt64> key_backup_storage;
    PaddedPODArray<RangeStorageType> range_backup_storage;

    const PaddedPODArray<UInt64> & ids = getColumnVectorData(this, key_columns[0], key_backup_storage);
    const PaddedPODArray<RangeStorageType> & dates = getColumnVectorData(this, key_columns[1], range_backup_storage);

    const auto & attr = *std::get<Ptr<AttributeType>>(attribute.maps);

    size_t keys_found = 0;

    for (const auto row : collections::range(0, ids.size()))
    {
        const auto it = attr.find(ids[row]);
        if (it)
        {
            const auto date = dates[row];
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
                        set_value(row, *value, false);
                    else
                        set_value(row, default_value_extractor[row], true);
                }
                else
                {
                    set_value(row, *value, false);
                }

                continue;
            }
        }

        if constexpr (is_nullable)
            set_value(row, default_value_extractor[row], default_value_extractor.isNullAt(row));
        else
            set_value(row, default_value_extractor[row], false);
    }

    query_count.fetch_add(ids.size(), std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}


template <typename T>
void RangeHashedDictionary::setAttributeValueImpl(Attribute & attribute, const UInt64 id, const Range & range, const Field & value)
{
    using ValueType = std::conditional_t<std::is_same_v<T, String>, StringRef, T>;
    auto & map = *std::get<Ptr<ValueType>>(attribute.maps);

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

    const auto it = map.find(id);

    if (it)
    {
        auto & values = it->getMapped();

        const auto insert_it
            = std::lower_bound(std::begin(values), std::end(values), range, [](const Value<ValueType> & lhs, const Range & rhs_range)
              {
                  return lhs.range < rhs_range;
              });

        values.insert(insert_it, std::move(value_to_insert));
    }
    else
        map.insert({id, Values<ValueType>{std::move(value_to_insert)}});
}

void RangeHashedDictionary::setAttributeValue(Attribute & attribute, const UInt64 id, const Range & range, const Field & value)
{
    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        setAttributeValueImpl<AttributeType>(attribute, id, range, value);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

const RangeHashedDictionary::Attribute & RangeHashedDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: no such attribute '{}'", full_name, attribute_name);

    return attributes[it->second];
}

const RangeHashedDictionary::Attribute &
RangeHashedDictionary::getAttributeWithType(const std::string & attribute_name, const AttributeUnderlyingType type) const
{
    const auto & attribute = getAttribute(attribute_name);
    if (attribute.type != type)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "attribute {} has type {}",
            attribute_name,
            toString(attribute.type));

    return attribute;
}

template <typename RangeType>
void RangeHashedDictionary::getIdsAndDates(
    PaddedPODArray<UInt64> & ids,
    PaddedPODArray<RangeType> & start_dates,
    PaddedPODArray<RangeType> & end_dates) const
{
    const auto & attribute = attributes.front();

    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        getIdsAndDates<ValueType>(attribute, ids, start_dates, end_dates);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

template <typename T, typename RangeType>
void RangeHashedDictionary::getIdsAndDates(
    const Attribute & attribute,
    PaddedPODArray<UInt64> & ids,
    PaddedPODArray<RangeType> & start_dates,
    PaddedPODArray<RangeType> & end_dates) const
{
    const HashMap<UInt64, Values<T>> & attr = *std::get<Ptr<T>>(attribute.maps);

    ids.reserve(attr.size());
    start_dates.reserve(attr.size());
    end_dates.reserve(attr.size());

    const bool is_date = isDate(dict_struct.range_min->type);

    for (const auto & key : attr)
    {
        for (const auto & value : key.getMapped())
        {
            ids.push_back(key.getKey());
            start_dates.push_back(value.range.left);
            end_dates.push_back(value.range.right);

            if constexpr (std::numeric_limits<RangeType>::max() > DATE_LUT_MAX_DAY_NUM) /// Avoid warning about tautological comparison in next line.
                if (is_date && static_cast<UInt64>(end_dates.back()) > DATE_LUT_MAX_DAY_NUM)
                    end_dates.back() = 0;
        }
    }
}


template <typename RangeType>
BlockInputStreamPtr RangeHashedDictionary::getBlockInputStreamImpl(const Names & column_names, size_t max_block_size) const
{
    PaddedPODArray<UInt64> ids;
    PaddedPODArray<RangeType> start_dates;
    PaddedPODArray<RangeType> end_dates;
    getIdsAndDates(ids, start_dates, end_dates);

    using BlockInputStreamType = RangeDictionaryBlockInputStream<RangeType>;

    auto stream = std::make_shared<BlockInputStreamType>(
        shared_from_this(),
        max_block_size,
        column_names,
        std::move(ids),
        std::move(start_dates),
        std::move(end_dates));

    return stream;
}

struct RangeHashedDictionaryCallGetBlockInputStreamImpl
{
    BlockInputStreamPtr stream;
    const RangeHashedDictionary * dict;
    const Names * column_names;
    size_t max_block_size;

    template <typename RangeType, size_t>
    void operator()()
    {
        const auto & type = dict->dict_struct.range_min->type;
        if (!stream && dynamic_cast<const DataTypeNumberBase<RangeType> *>(type.get()))
            stream = dict->getBlockInputStreamImpl<RangeType>(*column_names, max_block_size);
    }
};

BlockInputStreamPtr RangeHashedDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using ListType = TypeList<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Int128, Float32, Float64>;

    RangeHashedDictionaryCallGetBlockInputStreamImpl callable;
    callable.dict = this;
    callable.column_names = &column_names;
    callable.max_block_size = max_block_size;

    ListType::forEach(callable);

    if (!callable.stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected range type for RangeHashed dictionary: {}",
            dict_struct.range_min->type->getName());

    return callable.stream;
}


void registerDictionaryRangeHashed(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr /* context */,
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
        return std::make_unique<RangeHashedDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
    };
    factory.registerLayout("range_hashed", create_layout, false);
}

}
