#include <cstddef>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>

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
    throw Exception("Data type " + getName() + " can't be promoted.", ErrorCodes::DATA_TYPE_CANNOT_BE_PROMOTED);
}

size_t IDataType::getSizeOfValueInMemory() const
{
    throw Exception("Value of type " + getName() + " in memory is not of fixed size.", ErrorCodes::LOGICAL_ERROR);
}

void IDataType::forEachSubcolumn(
    const SubcolumnCallback & callback,
    const SubstreamData & data)
{
    ISerialization::StreamCallback callback_with_data = [&](const auto & subpath)
    {
        for (size_t i = 0; i < subpath.size(); ++i)
        {
            if (!subpath[i].visited && ISerialization::hasSubcolumnForPath(subpath, i + 1))
            {
                auto name = ISerialization::getSubcolumnNameForStream(subpath, i + 1);
                auto subdata = ISerialization::createFromPath(subpath, i);
                callback(subpath, name, subdata);
            }
            subpath[i].visited = true;
        }
    };

    SubstreamPath path;
    data.serialization->enumerateStreams(path, callback_with_data, data);
}

template <typename Ptr>
Ptr IDataType::getForSubcolumn(
    const String & subcolumn_name,
    const SubstreamData & data,
    Ptr SubstreamData::*member,
    bool throw_if_null) const
{
    Ptr res;
    forEachSubcolumn([&](const auto &, const auto & name, const auto & subdata)
    {
        if (name == subcolumn_name)
            res = subdata.*member;
    }, data);

    if (!res && throw_if_null)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());

    return res;
}

DataTypePtr IDataType::tryGetSubcolumnType(const String & subcolumn_name) const
{
    SubstreamData data = { getDefaultSerialization(), getPtr(), nullptr, nullptr };
    return getForSubcolumn<DataTypePtr>(subcolumn_name, data, &SubstreamData::type, false);
}

DataTypePtr IDataType::getSubcolumnType(const String & subcolumn_name) const
{
    SubstreamData data = { getDefaultSerialization(), getPtr(), nullptr, nullptr };
    return getForSubcolumn<DataTypePtr>(subcolumn_name, data, &SubstreamData::type, true);
}

ColumnPtr IDataType::tryGetSubcolumn(const String & subcolumn_name, const ColumnPtr & column) const
{
    SubstreamData data = { getDefaultSerialization(), nullptr, column, nullptr };
    return getForSubcolumn<ColumnPtr>(subcolumn_name, data, &SubstreamData::column, false);
}

ColumnPtr IDataType::getSubcolumn(const String & subcolumn_name, const ColumnPtr & column) const
{
    SubstreamData data = { getDefaultSerialization(), nullptr, column, nullptr };
    return getForSubcolumn<ColumnPtr>(subcolumn_name, data, &SubstreamData::column, true);
}

SerializationPtr IDataType::getSubcolumnSerialization(const String & subcolumn_name, const SerializationPtr & serialization) const
{
    SubstreamData data = { serialization, nullptr, nullptr, nullptr };
    return getForSubcolumn<SerializationPtr>(subcolumn_name, data, &SubstreamData::serialization, true);
}

Names IDataType::getSubcolumnNames() const
{
    Names res;
    forEachSubcolumn([&](const auto &, const auto & name, const auto &)
    {
        res.push_back(name);
    }, { getDefaultSerialization(), nullptr, nullptr, nullptr });
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

}
