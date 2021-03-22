#include "RangeHashedDictionary.h"
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Common/TypeList.h>
#include <ext/range.h>
#include "DictionaryFactory.h"
#include "RangeDictionaryBlockInputStream.h"

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
    : IDictionaryBase(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
{
    createAttributes();
    loadData();
    calculateBytesAllocated();
}


#define DECLARE_MULTIPLE_GETTER(TYPE) \
    void RangeHashedDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const PaddedPODArray<RangeStorageType> & dates, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto & attribute = getAttributeWithType(attribute_name, AttributeUnderlyingType::ut##TYPE); \
        getItems<TYPE>(attribute, ids, dates, out); \
    }
DECLARE_MULTIPLE_GETTER(UInt8)
DECLARE_MULTIPLE_GETTER(UInt16)
DECLARE_MULTIPLE_GETTER(UInt32)
DECLARE_MULTIPLE_GETTER(UInt64)
DECLARE_MULTIPLE_GETTER(UInt128)
DECLARE_MULTIPLE_GETTER(Int8)
DECLARE_MULTIPLE_GETTER(Int16)
DECLARE_MULTIPLE_GETTER(Int32)
DECLARE_MULTIPLE_GETTER(Int64)
DECLARE_MULTIPLE_GETTER(Float32)
DECLARE_MULTIPLE_GETTER(Float64)
DECLARE_MULTIPLE_GETTER(Decimal32)
DECLARE_MULTIPLE_GETTER(Decimal64)
DECLARE_MULTIPLE_GETTER(Decimal128)
#undef DECLARE_MULTIPLE_GETTER

void RangeHashedDictionary::getString(
    const std::string & attribute_name,
    const PaddedPODArray<Key> & ids,
    const PaddedPODArray<RangeStorageType> & dates,
    ColumnString * out) const
{
    const auto & attribute = getAttributeWithType(attribute_name, AttributeUnderlyingType::utString);
    const auto & attr = *std::get<Ptr<StringRef>>(attribute.maps);
    const auto & null_value = std::get<String>(attribute.null_values);

    for (const auto i : ext::range(0, ids.size()))
    {
        const auto * it = attr.find(ids[i]);
        if (it)
        {
            const auto date = dates[i];
            const auto & ranges_and_values = it->getMapped();
            const auto val_it
                = std::find_if(std::begin(ranges_and_values), std::end(ranges_and_values), [date](const Value<StringRef> & v)
                  {
                      return v.range.contains(date);
                  });

            const auto string_ref = val_it != std::end(ranges_and_values) ? val_it->value : StringRef{null_value};
            out->insertData(string_ref.data, string_ref.size);
        }
        else
            out->insertData(null_value.data(), null_value.size());
    }

    query_count.fetch_add(ids.size(), std::memory_order_relaxed);
}


void RangeHashedDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

        if (attribute.hierarchical)
            throw Exception{ErrorCodes::BAD_ARGUMENTS, "Hierarchical attributes not supported by {} dictionary.",
                            getDictionaryID().getNameForLogs()};
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

        for (const auto attribute_idx : ext::range(0, attributes.size()))
        {
            const auto & attribute_column = *block.safeGetByPosition(attribute_idx + 3).column;
            auto & attribute = attributes[attribute_idx];

            for (const auto row_idx : ext::range(0, id_column.size()))
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
        throw Exception{full_name + ": dictionary source is empty and 'require_nonempty' property is set.",
                        ErrorCodes::DICTIONARY_IS_EMPTY};
}

template <typename T>
void RangeHashedDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & map_ref = std::get<Ptr<T>>(attribute.maps);
    bytes_allocated += sizeof(Collection<T>) + map_ref->getBufferSizeInBytes();
    bucket_count = map_ref->getBufferSizeInCells();
}

void RangeHashedDictionary::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (const auto & attribute : attributes)
    {
        switch (attribute.type)
        {
            case AttributeUnderlyingType::utUInt8:
                addAttributeSize<UInt8>(attribute);
                break;
            case AttributeUnderlyingType::utUInt16:
                addAttributeSize<UInt16>(attribute);
                break;
            case AttributeUnderlyingType::utUInt32:
                addAttributeSize<UInt32>(attribute);
                break;
            case AttributeUnderlyingType::utUInt64:
                addAttributeSize<UInt64>(attribute);
                break;
            case AttributeUnderlyingType::utUInt128:
                addAttributeSize<UInt128>(attribute);
                break;
            case AttributeUnderlyingType::utInt8:
                addAttributeSize<Int8>(attribute);
                break;
            case AttributeUnderlyingType::utInt16:
                addAttributeSize<Int16>(attribute);
                break;
            case AttributeUnderlyingType::utInt32:
                addAttributeSize<Int32>(attribute);
                break;
            case AttributeUnderlyingType::utInt64:
                addAttributeSize<Int64>(attribute);
                break;
            case AttributeUnderlyingType::utFloat32:
                addAttributeSize<Float32>(attribute);
                break;
            case AttributeUnderlyingType::utFloat64:
                addAttributeSize<Float64>(attribute);
                break;

            case AttributeUnderlyingType::utDecimal32:
                addAttributeSize<Decimal32>(attribute);
                break;
            case AttributeUnderlyingType::utDecimal64:
                addAttributeSize<Decimal64>(attribute);
                break;
            case AttributeUnderlyingType::utDecimal128:
                addAttributeSize<Decimal128>(attribute);
                break;

            case AttributeUnderlyingType::utString:
            {
                addAttributeSize<StringRef>(attribute);
                bytes_allocated += sizeof(Arena) + attribute.string_arena->size();

                break;
            }
        }
    }
}

template <typename T>
void RangeHashedDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = T(null_value.get<NearestFieldType<T>>());
    attribute.maps = std::make_unique<Collection<T>>();
}

RangeHashedDictionary::Attribute
RangeHashedDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}, {}, {}};

    switch (type)
    {
        case AttributeUnderlyingType::utUInt8:
            createAttributeImpl<UInt8>(attr, null_value);
            break;
        case AttributeUnderlyingType::utUInt16:
            createAttributeImpl<UInt16>(attr, null_value);
            break;
        case AttributeUnderlyingType::utUInt32:
            createAttributeImpl<UInt32>(attr, null_value);
            break;
        case AttributeUnderlyingType::utUInt64:
            createAttributeImpl<UInt64>(attr, null_value);
            break;
        case AttributeUnderlyingType::utUInt128:
            createAttributeImpl<UInt128>(attr, null_value);
            break;
        case AttributeUnderlyingType::utInt8:
            createAttributeImpl<Int8>(attr, null_value);
            break;
        case AttributeUnderlyingType::utInt16:
            createAttributeImpl<Int16>(attr, null_value);
            break;
        case AttributeUnderlyingType::utInt32:
            createAttributeImpl<Int32>(attr, null_value);
            break;
        case AttributeUnderlyingType::utInt64:
            createAttributeImpl<Int64>(attr, null_value);
            break;
        case AttributeUnderlyingType::utFloat32:
            createAttributeImpl<Float32>(attr, null_value);
            break;
        case AttributeUnderlyingType::utFloat64:
            createAttributeImpl<Float64>(attr, null_value);
            break;

        case AttributeUnderlyingType::utDecimal32:
            createAttributeImpl<Decimal32>(attr, null_value);
            break;
        case AttributeUnderlyingType::utDecimal64:
            createAttributeImpl<Decimal64>(attr, null_value);
            break;
        case AttributeUnderlyingType::utDecimal128:
            createAttributeImpl<Decimal128>(attr, null_value);
            break;

        case AttributeUnderlyingType::utString:
        {
            attr.null_values = null_value.get<String>();
            attr.maps = std::make_unique<Collection<StringRef>>();
            attr.string_arena = std::make_unique<Arena>();
            break;
        }
    }

    return attr;
}


template <typename OutputType>
void RangeHashedDictionary::getItems(
    const Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    const PaddedPODArray<RangeStorageType> & dates,
    PaddedPODArray<OutputType> & out) const
{
    if (false) {} // NOLINT
#define DISPATCH(TYPE) else if (attribute.type == AttributeUnderlyingType::ut##TYPE) getItemsImpl<TYPE, OutputType>(attribute, ids, dates, out);
    DISPATCH(UInt8)
    DISPATCH(UInt16)
    DISPATCH(UInt32)
    DISPATCH(UInt64)
    DISPATCH(UInt128)
    DISPATCH(Int8)
    DISPATCH(Int16)
    DISPATCH(Int32)
    DISPATCH(Int64)
    DISPATCH(Float32)
    DISPATCH(Float64)
    DISPATCH(Decimal32)
    DISPATCH(Decimal64)
    DISPATCH(Decimal128)
#undef DISPATCH
    else throw Exception("Unexpected type of attribute: " + toString(attribute.type), ErrorCodes::LOGICAL_ERROR);
}

template <typename AttributeType, typename OutputType>
void RangeHashedDictionary::getItemsImpl(
    const Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    const PaddedPODArray<RangeStorageType> & dates,
    PaddedPODArray<OutputType> & out) const
{
    const auto & attr = *std::get<Ptr<AttributeType>>(attribute.maps);
    const auto null_value = std::get<AttributeType>(attribute.null_values);

    for (const auto i : ext::range(0, ids.size()))
    {
        const auto it = attr.find(ids[i]);
        if (it)
        {
            const auto date = dates[i];
            const auto & ranges_and_values = it->getMapped();
            const auto val_it
                = std::find_if(std::begin(ranges_and_values), std::end(ranges_and_values), [date](const Value<AttributeType> & v)
                  {
                      return v.range.contains(date);
                  });

            out[i] = static_cast<OutputType>(val_it != std::end(ranges_and_values) ? val_it->value : null_value); // NOLINT
        }
        else
        {
            out[i] = static_cast<OutputType>(null_value); // NOLINT
        }
    }

    query_count.fetch_add(ids.size(), std::memory_order_relaxed);
}


template <typename T>
void RangeHashedDictionary::setAttributeValueImpl(Attribute & attribute, const Key id, const Range & range, const T value)
{
    auto & map = *std::get<Ptr<T>>(attribute.maps);
    const auto it = map.find(id);

    if (it)
    {
        auto & values = it->getMapped();

        const auto insert_it
            = std::lower_bound(std::begin(values), std::end(values), range, [](const Value<T> & lhs, const Range & rhs_range)
              {
                  return lhs.range < rhs_range;
              });

        values.insert(insert_it, Value<T>{range, value});
    }
    else
        map.insert({id, Values<T>{Value<T>{range, value}}});
}

void RangeHashedDictionary::setAttributeValue(Attribute & attribute, const Key id, const Range & range, const Field & value)
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::utUInt8:
            setAttributeValueImpl<UInt8>(attribute, id, range, value.get<UInt64>());
            break;
        case AttributeUnderlyingType::utUInt16:
            setAttributeValueImpl<UInt16>(attribute, id, range, value.get<UInt64>());
            break;
        case AttributeUnderlyingType::utUInt32:
            setAttributeValueImpl<UInt32>(attribute, id, range, value.get<UInt64>());
            break;
        case AttributeUnderlyingType::utUInt64:
            setAttributeValueImpl<UInt64>(attribute, id, range, value.get<UInt64>());
            break;
        case AttributeUnderlyingType::utUInt128:
            setAttributeValueImpl<UInt128>(attribute, id, range, value.get<UInt128>());
            break;
        case AttributeUnderlyingType::utInt8:
            setAttributeValueImpl<Int8>(attribute, id, range, value.get<Int64>());
            break;
        case AttributeUnderlyingType::utInt16:
            setAttributeValueImpl<Int16>(attribute, id, range, value.get<Int64>());
            break;
        case AttributeUnderlyingType::utInt32:
            setAttributeValueImpl<Int32>(attribute, id, range, value.get<Int64>());
            break;
        case AttributeUnderlyingType::utInt64:
            setAttributeValueImpl<Int64>(attribute, id, range, value.get<Int64>());
            break;
        case AttributeUnderlyingType::utFloat32:
            setAttributeValueImpl<Float32>(attribute, id, range, value.get<Float64>());
            break;
        case AttributeUnderlyingType::utFloat64:
            setAttributeValueImpl<Float64>(attribute, id, range, value.get<Float64>());
            break;

        case AttributeUnderlyingType::utDecimal32:
            setAttributeValueImpl<Decimal32>(attribute, id, range, value.get<Decimal32>());
            break;
        case AttributeUnderlyingType::utDecimal64:
            setAttributeValueImpl<Decimal64>(attribute, id, range, value.get<Decimal64>());
            break;
        case AttributeUnderlyingType::utDecimal128:
            setAttributeValueImpl<Decimal128>(attribute, id, range, value.get<Decimal128>());
            break;

        case AttributeUnderlyingType::utString:
        {
            auto & map = *std::get<Ptr<StringRef>>(attribute.maps);
            const auto & string = value.get<String>();
            const auto * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
            const StringRef string_ref{string_in_arena, string.size()};

            auto * it = map.find(id);

            if (it)
            {
                auto & values = it->getMapped();

                const auto insert_it = std::lower_bound(
                    std::begin(values), std::end(values), range, [](const Value<StringRef> & lhs, const Range & rhs_range)
                    {
                        return lhs.range < rhs_range;
                    });

                values.insert(insert_it, Value<StringRef>{range, string_ref});
            }
            else
                map.insert({id, Values<StringRef>{Value<StringRef>{range, string_ref}}});

            break;
        }
    }
}

const RangeHashedDictionary::Attribute & RangeHashedDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{full_name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

const RangeHashedDictionary::Attribute &
RangeHashedDictionary::getAttributeWithType(const std::string & attribute_name, const AttributeUnderlyingType type) const
{
    const auto & attribute = getAttribute(attribute_name);
    if (attribute.type != type)
        throw Exception{attribute_name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
                        ErrorCodes::TYPE_MISMATCH};

    return attribute;
}

template <typename RangeType>
void RangeHashedDictionary::getIdsAndDates(
    PaddedPODArray<Key> & ids, PaddedPODArray<RangeType> & start_dates, PaddedPODArray<RangeType> & end_dates) const
{
    const auto & attribute = attributes.front();

    switch (attribute.type)
    {
        case AttributeUnderlyingType::utUInt8:
            getIdsAndDates<UInt8>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utUInt16:
            getIdsAndDates<UInt16>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utUInt32:
            getIdsAndDates<UInt32>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utUInt64:
            getIdsAndDates<UInt64>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utUInt128:
            getIdsAndDates<UInt128>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utInt8:
            getIdsAndDates<Int8>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utInt16:
            getIdsAndDates<Int16>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utInt32:
            getIdsAndDates<Int32>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utInt64:
            getIdsAndDates<Int64>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utFloat32:
            getIdsAndDates<Float32>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utFloat64:
            getIdsAndDates<Float64>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utString:
            getIdsAndDates<StringRef>(attribute, ids, start_dates, end_dates);
            break;

        case AttributeUnderlyingType::utDecimal32:
            getIdsAndDates<Decimal32>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utDecimal64:
            getIdsAndDates<Decimal64>(attribute, ids, start_dates, end_dates);
            break;
        case AttributeUnderlyingType::utDecimal128:
            getIdsAndDates<Decimal128>(attribute, ids, start_dates, end_dates);
            break;
    }
}

template <typename T, typename RangeType>
void RangeHashedDictionary::getIdsAndDates(
    const Attribute & attribute,
    PaddedPODArray<Key> & ids,
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

            if (is_date && static_cast<UInt64>(end_dates.back()) > DATE_LUT_MAX_DAY_NUM)
                end_dates.back() = 0;
        }
    }
}


template <typename RangeType>
BlockInputStreamPtr RangeHashedDictionary::getBlockInputStreamImpl(const Names & column_names, size_t max_block_size) const
{
    PaddedPODArray<Key> ids;
    PaddedPODArray<RangeType> start_dates;
    PaddedPODArray<RangeType> end_dates;
    getIdsAndDates(ids, start_dates, end_dates);

    using BlockInputStreamType = RangeDictionaryBlockInputStream<RangeHashedDictionary, RangeType, Key>;
    auto dict_ptr = std::static_pointer_cast<const RangeHashedDictionary>(shared_from_this());
    return std::make_shared<BlockInputStreamType>(
        dict_ptr, max_block_size, column_names, std::move(ids), std::move(start_dates), std::move(end_dates));
}

struct RangeHashedDIctionaryCallGetBlockInputStreamImpl
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

    RangeHashedDIctionaryCallGetBlockInputStreamImpl callable;
    callable.dict = this;
    callable.column_names = &column_names;
    callable.max_block_size = max_block_size;

    ListType::forEach(callable);

    if (!callable.stream)
        throw Exception(
            "Unexpected range type for RangeHashed dictionary: " + dict_struct.range_min->type->getName(), ErrorCodes::LOGICAL_ERROR);

    return callable.stream;
}


void registerDictionaryRangeHashed(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception{"'key' is not supported for dictionary of layout 'range_hashed'", ErrorCodes::UNSUPPORTED_METHOD};

        if (!dict_struct.range_min || !dict_struct.range_max)
            throw Exception{full_name + ": dictionary of layout 'range_hashed' requires .structure.range_min and .structure.range_max",
                            ErrorCodes::BAD_ARGUMENTS};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        return std::make_unique<RangeHashedDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
    };
    factory.registerLayout("range_hashed", create_layout, false);
}

}
