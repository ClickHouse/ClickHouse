#include <cstddef>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/Serializations/SerializationInfo.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DATA_TYPE_CANNOT_BE_PROMOTED;
    extern const int ILLEGAL_COLUMN;
}

IDataType::~IDataType() = default;

void IDataType::updateAvgValueSizeHint(const IColumn & column, double & avg_value_size_hint)
{
    /// Update the average value size hint if amount of read rows isn't too small
    size_t column_size = column.size();
    if (column_size > 10)
    {
        double current_avg_value_size = static_cast<double>(column.byteSize()) / column_size;

        /// Heuristic is chosen so that avg_value_size_hint increases rapidly but decreases slowly.
        if (current_avg_value_size > avg_value_size_hint)
            avg_value_size_hint = std::min(1024., current_avg_value_size); /// avoid overestimation
        else if (current_avg_value_size * 2 < avg_value_size_hint)
            avg_value_size_hint = (current_avg_value_size + avg_value_size_hint * 3) / 4;
    }
}

MutableColumnPtr IDataType::createColumn(const ISerialization & serialization) const
{
    auto column = createColumn();
    if (serialization.getKind() == ISerialization::Kind::SPARSE)
        return ColumnSparse::create(std::move(column));

    return column;
}

ColumnPtr IDataType::createColumnConst(size_t size, const Field & field) const
{
    auto column = createColumn();
    column->insert(field);
    return ColumnConst::create(std::move(column), size);
}


ColumnPtr IDataType::createColumnConstWithDefaultValue(size_t size) const
{
    return createColumnConst(size, getDefault());
}

DataTypePtr IDataType::promoteNumericType() const
{
    throw Exception(ErrorCodes::DATA_TYPE_CANNOT_BE_PROMOTED, "Data type {} can't be promoted.", getName());
}

size_t IDataType::getSizeOfValueInMemory() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Value of type {} in memory is not of fixed size.", getName());
}

void IDataType::forEachSubcolumn(
    const SubcolumnCallback & callback,
    const SubstreamData & data)
{
    ISerialization::StreamCallback callback_with_data = [&](const auto & subpath)
    {
        for (size_t i = 0; i < subpath.size(); ++i)
        {
            size_t prefix_len = i + 1;
            if (!subpath[i].visited && ISerialization::hasSubcolumnForPath(subpath, prefix_len))
            {
                auto name = ISerialization::getSubcolumnNameForStream(subpath, prefix_len);
                auto subdata = ISerialization::createFromPath(subpath, prefix_len);
                callback(subpath, name, subdata);
            }
            subpath[i].visited = true;
        }
    };

    ISerialization::EnumerateStreamsSettings settings;
    settings.position_independent_encoding = false;
    data.serialization->enumerateStreams(settings, callback_with_data, data);
}

template <typename Ptr>
Ptr IDataType::getForSubcolumn(
    std::string_view subcolumn_name,
    const SubstreamData & data,
    Ptr SubstreamData::*member,
    bool throw_if_null) const
{
    Ptr res;

    ISerialization::StreamCallback callback_with_data = [&](const auto & subpath)
    {
        for (size_t i = 0; i < subpath.size(); ++i)
        {
            size_t prefix_len = i + 1;
            if (!subpath[i].visited && ISerialization::hasSubcolumnForPath(subpath, prefix_len))
            {
                auto name = ISerialization::getSubcolumnNameForStream(subpath, prefix_len);
                /// Create data from path only if it's requested subcolumn.
                if (name == subcolumn_name)
                    res = ISerialization::createFromPath(subpath, prefix_len).*member;
            }
            subpath[i].visited = true;
        }
    };

    ISerialization::EnumerateStreamsSettings settings;
    settings.position_independent_encoding = false;
    data.serialization->enumerateStreams(settings, callback_with_data, data);

    if (!res && throw_if_null)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());

    return res;
}

bool IDataType::hasSubcolumn(std::string_view subcolumn_name) const
{
    return tryGetSubcolumnType(subcolumn_name) != nullptr;
}

DataTypePtr IDataType::tryGetSubcolumnType(std::string_view subcolumn_name) const
{
    auto data = SubstreamData(getDefaultSerialization()).withType(getPtr());
    return getForSubcolumn<DataTypePtr>(subcolumn_name, data, &SubstreamData::type, false);
}

DataTypePtr IDataType::getSubcolumnType(std::string_view subcolumn_name) const
{
    auto data = SubstreamData(getDefaultSerialization()).withType(getPtr());
    return getForSubcolumn<DataTypePtr>(subcolumn_name, data, &SubstreamData::type, true);
}

ColumnPtr IDataType::tryGetSubcolumn(std::string_view subcolumn_name, const ColumnPtr & column) const
{
    auto data = SubstreamData(getDefaultSerialization()).withColumn(column);
    return getForSubcolumn<ColumnPtr>(subcolumn_name, data, &SubstreamData::column, false);
}

ColumnPtr IDataType::getSubcolumn(std::string_view subcolumn_name, const ColumnPtr & column) const
{
    auto data = SubstreamData(getDefaultSerialization()).withColumn(column);
    return getForSubcolumn<ColumnPtr>(subcolumn_name, data, &SubstreamData::column, true);
}

SerializationPtr IDataType::getSubcolumnSerialization(std::string_view subcolumn_name, const SerializationPtr & serialization) const
{
    auto data = SubstreamData(serialization);
    return getForSubcolumn<SerializationPtr>(subcolumn_name, data, &SubstreamData::serialization, true);
}

Names IDataType::getSubcolumnNames() const
{
    Names res;
    forEachSubcolumn([&](const auto &, const auto & name, const auto &)
    {
        res.push_back(name);
    }, SubstreamData(getDefaultSerialization()));
    return res;
}

void IDataType::insertDefaultInto(IColumn & column) const
{
    column.insertDefault();
}

void IDataType::insertManyDefaultsInto(IColumn & column, size_t n) const
{
    for (size_t i = 0; i < n; ++i)
        insertDefaultInto(column);
}

void IDataType::setCustomization(DataTypeCustomDescPtr custom_desc_) const
{
    /// replace only if not null
    if (custom_desc_->name)
        custom_name = std::move(custom_desc_->name);

    if (custom_desc_->serialization)
        custom_serialization = std::move(custom_desc_->serialization);
}

MutableSerializationInfoPtr IDataType::createSerializationInfo(const SerializationInfo::Settings & settings) const
{
    return std::make_shared<SerializationInfo>(ISerialization::Kind::DEFAULT, settings);
}

SerializationInfoPtr IDataType::getSerializationInfo(const IColumn & column) const
{
    if (const auto * column_const = checkAndGetColumn<ColumnConst>(&column))
        return getSerializationInfo(column_const->getDataColumn());

    return std::make_shared<SerializationInfo>(ISerialization::getKind(column), SerializationInfo::Settings{});
}

SerializationPtr IDataType::getDefaultSerialization() const
{
    if (custom_serialization)
        return custom_serialization;

    return doGetDefaultSerialization();
}

SerializationPtr IDataType::getSparseSerialization() const
{
    return std::make_shared<SerializationSparse>(getDefaultSerialization());
}

SerializationPtr IDataType::getSerialization(ISerialization::Kind kind) const
{
    if (supportsSparseSerialization() && kind == ISerialization::Kind::SPARSE)
        return getSparseSerialization();

    return getDefaultSerialization();
}

SerializationPtr IDataType::getSerialization(const SerializationInfo & info) const
{
    return getSerialization(info.getKind());
}

// static
SerializationPtr IDataType::getSerialization(const NameAndTypePair & column, const SerializationInfo & info)
{
    if (column.isSubcolumn())
    {
        const auto & type_in_storage = column.getTypeInStorage();
        auto serialization = type_in_storage->getSerialization(info);
        return type_in_storage->getSubcolumnSerialization(column.getSubcolumnName(), serialization);
    }

    return column.type->getSerialization(info);
}

// static
SerializationPtr IDataType::getSerialization(const NameAndTypePair & column)
{
    if (column.isSubcolumn())
    {
        const auto & type_in_storage = column.getTypeInStorage();
        auto serialization = type_in_storage->getDefaultSerialization();
        return type_in_storage->getSubcolumnSerialization(column.getSubcolumnName(), serialization);
    }

    return column.type->getDefaultSerialization();
}

#define FOR_TYPES_OF_TYPE(M) \
    M(TypeIndex) \
    M(const IDataType &) \
    M(const DataTypePtr &) \
    M(WhichDataType)

#define DISPATCH(TYPE) \
bool isUInt8(TYPE data_type) { return WhichDataType(data_type).isUInt8(); } \
bool isUInt16(TYPE data_type) { return WhichDataType(data_type).isUInt16(); } \
bool isUInt32(TYPE data_type) { return WhichDataType(data_type).isUInt32(); } \
bool isUInt64(TYPE data_type) { return WhichDataType(data_type).isUInt64(); } \
bool isNativeUInt(TYPE data_type) { return WhichDataType(data_type).isNativeUInt(); } \
bool isUInt(TYPE data_type) { return WhichDataType(data_type).isUInt(); } \
\
bool isInt8(TYPE data_type) { return WhichDataType(data_type).isInt8(); } \
bool isInt16(TYPE data_type) { return WhichDataType(data_type).isInt16(); } \
bool isInt32(TYPE data_type) { return WhichDataType(data_type).isInt32(); } \
bool isInt64(TYPE data_type) { return WhichDataType(data_type).isInt64(); } \
bool isNativeInt(TYPE data_type) { return WhichDataType(data_type).isNativeInt(); } \
bool isInt(TYPE data_type) { return WhichDataType(data_type).isInt(); } \
\
bool isInteger(TYPE data_type) { return WhichDataType(data_type).isInteger(); } \
bool isNativeInteger(TYPE data_type) { return WhichDataType(data_type).isNativeInteger(); } \
\
bool isDecimal(TYPE data_type) { return WhichDataType(data_type).isDecimal(); } \
\
bool isFloat(TYPE data_type) { return WhichDataType(data_type).isFloat(); } \
\
bool isNativeNumber(TYPE data_type) { return WhichDataType(data_type).isNativeNumber(); } \
bool isNumber(TYPE data_type) { return WhichDataType(data_type).isNumber(); } \
\
bool isEnum8(TYPE data_type) { return WhichDataType(data_type).isEnum8(); } \
bool isEnum16(TYPE data_type) { return WhichDataType(data_type).isEnum16(); } \
bool isEnum(TYPE data_type) { return WhichDataType(data_type).isEnum(); } \
\
bool isDate(TYPE data_type) { return WhichDataType(data_type).isDate(); } \
bool isDate32(TYPE data_type) { return WhichDataType(data_type).isDate32(); } \
bool isDateOrDate32(TYPE data_type) { return WhichDataType(data_type).isDateOrDate32(); } \
bool isDateTime(TYPE data_type) { return WhichDataType(data_type).isDateTime(); } \
bool isDateTime64(TYPE data_type) { return WhichDataType(data_type).isDateTime64(); } \
bool isDateTimeOrDateTime64(TYPE data_type) { return WhichDataType(data_type).isDateTimeOrDateTime64(); } \
bool isDateOrDate32OrDateTimeOrDateTime64(TYPE data_type) { return WhichDataType(data_type).isDateOrDate32OrDateTimeOrDateTime64(); } \
\
bool isString(TYPE data_type) { return WhichDataType(data_type).isString(); } \
bool isFixedString(TYPE data_type) { return WhichDataType(data_type).isFixedString(); } \
bool isStringOrFixedString(TYPE data_type) { return WhichDataType(data_type).isStringOrFixedString(); } \
\
bool isUUID(TYPE data_type) { return WhichDataType(data_type).isUUID(); } \
bool isIPv4(TYPE data_type) { return WhichDataType(data_type).isIPv4(); } \
bool isIPv6(TYPE data_type) { return WhichDataType(data_type).isIPv6(); } \
bool isArray(TYPE data_type) { return WhichDataType(data_type).isArray(); } \
bool isTuple(TYPE data_type) { return WhichDataType(data_type).isTuple(); } \
bool isMap(TYPE data_type) {return WhichDataType(data_type).isMap(); } \
bool isInterval(TYPE data_type) {return WhichDataType(data_type).isInterval(); } \
bool isObject(TYPE data_type) { return WhichDataType(data_type).isObject(); } \
bool isVariant(TYPE data_type) { return WhichDataType(data_type).isVariant(); } \
bool isNothing(TYPE data_type) { return WhichDataType(data_type).isNothing(); } \
\
bool isColumnedAsNumber(TYPE data_type) \
{ \
    WhichDataType which(data_type); \
    return which.isInteger() || which.isFloat() || which.isDateOrDate32OrDateTimeOrDateTime64() || which.isUUID() || which.isIPv4() || which.isIPv6(); \
} \
\
bool isColumnedAsDecimal(TYPE data_type) \
{ \
    WhichDataType which(data_type); \
    return which.isDecimal() || which.isDateTime64(); \
} \
\
bool isNotCreatable(TYPE data_type) \
{ \
    WhichDataType which(data_type); \
    return which.isNothing() || which.isFunction() || which.isSet(); \
} \
\
bool isNotDecimalButComparableToDecimal(TYPE data_type) \
{ \
    WhichDataType which(data_type); \
    return which.isInt() || which.isUInt() || which.isFloat(); \
} \

FOR_TYPES_OF_TYPE(DISPATCH)

#undef DISPATCH
#undef FOR_TYPES_OF_TYPE

}
