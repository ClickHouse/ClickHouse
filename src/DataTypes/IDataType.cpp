#include <cstddef>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnReplicated.h>

#include <Common/checkStackSize.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/SipHash.h>

#include <IO/WriteHelpers.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/Serializations/SerializationReplicated.h>
#include <DataTypes/Serializations/SerializationInfo.h>

#include <DataTypes/Serializations/SerializationDetached.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DATA_TYPE_CANNOT_BE_PROMOTED;
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
}

IDataType::IDataType() = default;

IDataType::~IDataType() = default;

String IDataType::getName() const
{
    if (custom_name)
        return custom_name->getName();
    return doGetName();
}

String IDataType::getPrettyName(size_t indent) const
{
    if (custom_name)
        return custom_name->getName();
    return doGetPrettyName(indent);
}

void IDataType::updateHash(SipHash & hash) const
{
    if (custom_name)
    {
        hash.update(custom_name->getName().size());
        hash.update(custom_name->getName());
    }
    else
        hash.update(size_t(0));

    hash.update(getTypeId());
    updateHashImpl(hash);
}

void IDataType::updateAvgValueSizeHint(const IColumn & column, double & avg_value_size_hint)
{
    /// Update the average value size hint if amount of read rows isn't too small
    size_t column_size = column.size();
    if (column_size > 10)
    {
        double current_avg_value_size = static_cast<double>(column.byteSize()) / static_cast<double>(column_size);

        /// Heuristic is chosen so that avg_value_size_hint increases rapidly but decreases slowly.
        if (current_avg_value_size > avg_value_size_hint)
            avg_value_size_hint = std::min(1024., current_avg_value_size); /// avoid overestimation
        else if (current_avg_value_size * 2 < avg_value_size_hint)
            avg_value_size_hint = (current_avg_value_size + avg_value_size_hint * 3) / 4;
    }
}


MutableColumnPtr IDataType::createUninitializedColumnWithSize(size_t size) const
{
    auto column = createColumn();
    return column->cloneResized(size);
}

MutableColumnPtr IDataType::createColumn(const ISerialization & serialization) const
{
    auto kind_stack = serialization.getKindStack();
    auto column = createColumn();
    for (auto kind : kind_stack)
    {
        if (kind == ISerialization::Kind::SPARSE)
            column = ColumnSparse::create(std::move(column));
        else if (kind == ISerialization::Kind::REPLICATED)
            column = ColumnReplicated::create(std::move(column), ColumnUInt8::create());
    }

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
                auto path_copy = subpath;
                path_copy.resize(prefix_len);
                callback(path_copy, name, subdata);
            }
            subpath[i].visited = true;
        }
    };

    ISerialization::EnumerateStreamsSettings settings;
    settings.position_independent_encoding = false;
    settings.enumerate_virtual_streams = true;
    data.serialization->enumerateStreams(settings, callback_with_data, data);
}

std::unique_ptr<IDataType::SubstreamData> IDataType::getSubcolumnData(
    std::string_view subcolumn_name,
    const SubstreamData & data,
    size_t initial_array_level,
    bool throw_if_null)
{
    std::unique_ptr<IDataType::SubstreamData> res;

    ISerialization::StreamCallback callback_with_data = [&](const auto & subpath)
    {
        for (size_t i = 0; i < subpath.size(); ++i)
        {
            size_t prefix_len = i + 1;
            if (!subpath[i].visited && ISerialization::hasSubcolumnForPath(subpath, prefix_len))
            {
                auto name = ISerialization::getSubcolumnNameForStream(subpath, prefix_len, false, initial_array_level);
                /// Create data from path only if it's requested subcolumn.
                if (name == subcolumn_name)
                {
                    res = std::make_unique<SubstreamData>(ISerialization::createFromPath(subpath, prefix_len));
                }
                /// Check if this subcolumn is a prefix of requested subcolumn and it can create dynamic subcolumns.
                else if (subcolumn_name.starts_with(name + ".") && subpath[i].data.type && subpath[i].data.type->hasDynamicSubcolumnsData())
                {
                    auto dynamic_subcolumn_name = subcolumn_name.substr(name.size() + 1);
                    auto dynamic_subcolumn_data = subpath[i].data.type->getDynamicSubcolumnData(
                        dynamic_subcolumn_name,
                        subpath[i].data,
                        initial_array_level + ISerialization::getArrayLevel(subpath, prefix_len),
                        false);
                    if (dynamic_subcolumn_data)
                    {
                        /// Create requested subcolumn using dynamic subcolumn data.
                        auto tmp_subpath = subpath;
                        if (tmp_subpath[i].creator)
                        {
                            dynamic_subcolumn_data->type = tmp_subpath[i].creator->create(dynamic_subcolumn_data->type);
                            dynamic_subcolumn_data->column = tmp_subpath[i].creator->create(dynamic_subcolumn_data->column);
                            dynamic_subcolumn_data->serialization = tmp_subpath[i].creator->create(dynamic_subcolumn_data->serialization, dynamic_subcolumn_data->type);
                        }

                        tmp_subpath[i].data = *dynamic_subcolumn_data;
                        res = std::make_unique<SubstreamData>(ISerialization::createFromPath(tmp_subpath, prefix_len));
                    }
                }
            }
            subpath[i].visited = true;
        }
    };

    ISerialization::EnumerateStreamsSettings settings;
    settings.position_independent_encoding = false;
    /// Don't enumerate dynamic subcolumns, they are handled separately.
    settings.enumerate_dynamic_streams = false;
    settings.enumerate_virtual_streams = true;
    settings.array_level = initial_array_level;
    data.serialization->enumerateStreams(settings, callback_with_data, data);

    if (!res && data.type->hasDynamicSubcolumnsData())
        return data.type->getDynamicSubcolumnData(subcolumn_name, data, settings.array_level, throw_if_null);

    if (!res && throw_if_null)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, data.type->getName());

    return res;
}

std::unique_ptr<IDataType::SubstreamData> IDataType::getDynamicSubcolumnData(
    std::string_view /*subcolumn_name*/,
    const SubstreamData & /*data*/,
    size_t /*initial_array_level*/,
    bool throw_if_null) const
{
    if (throw_if_null)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDynamicSubcolumnData is not implemented for type {}", getName());
    return nullptr;
}

bool IDataType::hasSubcolumn(std::string_view subcolumn_name) const
{
    return tryGetSubcolumnType(subcolumn_name) != nullptr;
}

bool IDataType::hasDynamicSubcolumns() const
{
    if (hasDynamicSubcolumnsData())
        return true;

    bool has_dynamic_subcolumns = false;
    auto data = SubstreamData(getDefaultSerialization()).withType(getPtr());
    auto callback = [&](const SubstreamPath &, const String &, const SubstreamData & subcolumn_data)
    {
        has_dynamic_subcolumns |= subcolumn_data.type && subcolumn_data.type->hasDynamicSubcolumnsData();
    };
    forEachSubcolumn(callback, data);
    return has_dynamic_subcolumns;
}

DataTypePtr IDataType::tryGetSubcolumnType(std::string_view subcolumn_name) const
{
    auto data = SubstreamData(getDefaultSerialization()).withType(getPtr());
    auto subcolumn_data = getSubcolumnData(subcolumn_name, data, {}, false);
    return subcolumn_data ? subcolumn_data->type : nullptr;
}

DataTypePtr IDataType::getSubcolumnType(std::string_view subcolumn_name) const
{
    auto data = SubstreamData(getDefaultSerialization()).withType(getPtr());
    return getSubcolumnData(subcolumn_name, data, {}, true)->type;
}

ColumnPtr IDataType::tryGetSubcolumn(std::string_view subcolumn_name, const ColumnPtr & column) const
{
    auto data = SubstreamData(getSerialization(*getSerializationInfo(*column))).withType(getPtr()).withColumn(column);
    auto subcolumn_data = getSubcolumnData(subcolumn_name, data, {}, false);
    return subcolumn_data ? subcolumn_data->column : nullptr;
}

ColumnPtr IDataType::getSubcolumn(std::string_view subcolumn_name, const ColumnPtr & column) const
{
    auto data = SubstreamData(getSerialization(*getSerializationInfo(*column))).withType(getPtr()).withColumn(column);
    return getSubcolumnData(subcolumn_name, data, {}, true)->column;
}

SerializationPtr IDataType::getSubcolumnSerialization(std::string_view subcolumn_name, const SerializationPtr & serialization) const
{
    auto data = SubstreamData(serialization).withType(getPtr());
    return getSubcolumnData(subcolumn_name, data, {}, true)->serialization;
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
    column.reserve(column.size() + n);
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

MutableSerializationInfoPtr IDataType::createSerializationInfo(const SerializationInfoSettings & settings) const
{
    return std::make_shared<SerializationInfo>(ISerialization::KindStack{ISerialization::Kind::DEFAULT}, settings);
}

SerializationInfoPtr IDataType::getSerializationInfo(const IColumn & column) const
{
    if (const auto * column_const = checkAndGetColumn<ColumnConst>(&column))
        return getSerializationInfo(column_const->getDataColumn());

    /// Enable all supported serialization features when deriving info from an existing column. Since the column
    /// reflects the actual in-memory state, the serialization info must accept any variant that the column may contain.
    return std::make_shared<SerializationInfo>(
        ISerialization::getKindStack(column), SerializationInfoSettings::enableAllSupportedSerializations());
}

SerializationPtr IDataType::getDefaultSerialization(SerializationPtr override_default) const
{
    checkStackSize();

    if (override_default)
        return override_default;

    if (custom_serialization)
        return custom_serialization;

    return doGetDefaultSerialization();
}

SerializationPtr IDataType::getSerialization(
    ISerialization::KindStack kind_stack, const SerializationInfoSettings & settings, SerializationPtr override_default) const
{
    auto serialization = getDefaultSerialization(override_default);
    for (auto kind : kind_stack)
    {
        if (settings.canUseSparseSerialization(*this) && kind == ISerialization::Kind::SPARSE)
            serialization = std::make_shared<SerializationSparse>(serialization);
        else if (kind == ISerialization::Kind::DETACHED)
            serialization = std::make_shared<SerializationDetached>(serialization);
        else if (kind == ISerialization::Kind::REPLICATED)
            serialization = std::make_shared<SerializationReplicated>(serialization);
    }

    return serialization;
}

SerializationPtr IDataType::getSerialization(const SerializationInfo & info) const
{
    return getSerialization(info.getKindStack(), info.getSettings());
}

SerializationPtr IDataType::getSerialization(const SerializationInfoSettings & settings) const
{
    return getSerialization(*createSerializationInfo(settings));
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
SerializationPtr IDataType::getSerialization(const NameAndTypePair & column, const SerializationInfoSettings & settings)
{
    if (column.isSubcolumn())
    {
        const auto & type_in_storage = column.getTypeInStorage();
        auto serialization = type_in_storage->getSerialization(settings);
        return type_in_storage->getSubcolumnSerialization(column.getSubcolumnName(), serialization);
    }

    return column.type->getSerialization(settings);
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
bool isUInt128(TYPE data_type) { return WhichDataType(data_type).isUInt128(); } \
bool isUInt256(TYPE data_type) { return WhichDataType(data_type).isUInt256(); } \
bool isNativeUInt(TYPE data_type) { return WhichDataType(data_type).isNativeUInt(); } \
bool isUInt(TYPE data_type) { return WhichDataType(data_type).isUInt(); } \
\
bool isInt8(TYPE data_type) { return WhichDataType(data_type).isInt8(); } \
bool isInt16(TYPE data_type) { return WhichDataType(data_type).isInt16(); } \
bool isInt32(TYPE data_type) { return WhichDataType(data_type).isInt32(); } \
bool isInt64(TYPE data_type) { return WhichDataType(data_type).isInt64(); } \
bool isInt128(TYPE data_type) { return WhichDataType(data_type).isInt128(); } \
bool isInt256(TYPE data_type) { return WhichDataType(data_type).isInt256(); } \
bool isNativeInt(TYPE data_type) { return WhichDataType(data_type).isNativeInt(); } \
bool isInt(TYPE data_type) { return WhichDataType(data_type).isInt(); } \
\
bool isInteger(TYPE data_type) { return WhichDataType(data_type).isInteger(); } \
bool isNativeInteger(TYPE data_type) { return WhichDataType(data_type).isNativeInteger(); } \
\
bool isDecimal(TYPE data_type) { return WhichDataType(data_type).isDecimal(); } \
bool isDecimal64(TYPE data_type) { return WhichDataType(data_type).isDecimal64(); } \
\
bool isFloat(TYPE data_type) { return WhichDataType(data_type).isFloat(); } \
\
bool isIntegerOrDecimal(TYPE data_type) { return WhichDataType(data_type).isIntegerOrDecimal(); } \
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
bool isTime(TYPE data_type) { return WhichDataType(data_type).isTime(); } \
bool isDateTime64(TYPE data_type) { return WhichDataType(data_type).isDateTime64(); } \
bool isTime64(TYPE data_type) { return WhichDataType(data_type).isTime64(); } \
bool isDateTimeOrDateTime64(TYPE data_type) { return WhichDataType(data_type).isDateTimeOrDateTime64(); } \
bool isDateOrDate32OrDateTimeOrDateTime64(TYPE data_type) { return WhichDataType(data_type).isDateOrDate32OrDateTimeOrDateTime64(); } \
bool isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(TYPE data_type) { return WhichDataType(data_type).isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(); } \
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
bool isVariant(TYPE data_type) { return WhichDataType(data_type).isVariant(); } \
bool isDynamic(TYPE data_type) { return WhichDataType(data_type).isDynamic(); } \
bool isObject(TYPE data_type) { return WhichDataType(data_type).isObject(); } \
bool isNothing(TYPE data_type) { return WhichDataType(data_type).isNothing(); } \
bool isQBit(TYPE data_type) { return WhichDataType(data_type).isQBit(); } \
\
bool isColumnedAsNumber(TYPE data_type) \
{ \
    WhichDataType which(data_type); \
    return which.isInteger() || which.isFloat() || which.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64() || which.isUUID() || which.isIPv4() || which.isIPv6(); \
} \
\
bool isColumnedAsDecimal(TYPE data_type) \
{ \
    WhichDataType which(data_type); \
    return which.isDecimal() || which.isDateTime64() || which.isTime64(); \
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
