#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/NestedUtils.h>


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
    const SerializationPtr & serialization,
    const DataTypePtr & type,
    const ColumnPtr & column)
{
    ISerialization::StreamCallback callback_with_data = [&](const auto & subpath)
    {
        for (size_t i = 0; i < subpath.size(); ++i)
        {
            if (!subpath[i].visited && ISerialization::hasSubcolumnForPath(subpath, i + 1))
            {
                auto name = ISerialization::getSubcolumnNameForStream(subpath, i + 1);
                auto data = ISerialization::createFromPath(subpath, i);
                callback(subpath, name, data);
            }
            subpath[i].visited = true;
        }
    };

    ISerialization::SubstreamPath path;
    serialization->enumerateStreams(path, callback_with_data, type, column);
}

DataTypePtr IDataType::tryGetSubcolumnType(const String & subcolumn_name) const
{
    DataTypePtr res;
    forEachSubcolumn([&](const auto &, const auto & name, const auto & data)
    {
        if (name == subcolumn_name)
            res = data.type;
    }, getDefaultSerialization(), getPtr(), nullptr);

    return res;
}

DataTypePtr IDataType::getSubcolumnType(const String & subcolumn_name) const
{
    auto subcolumn_type = tryGetSubcolumnType(subcolumn_name);
    if (subcolumn_type)
        return subcolumn_type;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());
}

SerializationPtr IDataType::getSubcolumnSerialization(const String & subcolumn_name, const SerializationPtr & serialization) const
{
    SerializationPtr res;
    forEachSubcolumn([&](const auto &, const auto & name, const auto & data)
    {
        if (name == subcolumn_name)
            res = data.serialization;
    }, serialization, nullptr, nullptr);

    if (res)
        return res;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());
}

ColumnPtr IDataType::getSubcolumn(const String & subcolumn_name, const ColumnPtr & column) const
{
    ColumnPtr res;
    forEachSubcolumn([&](const auto &, const auto & name, const auto & data)
    {
        if (name == subcolumn_name)
            res = data.column;
    }, getDefaultSerialization(), nullptr, column);

    if (res)
        return res;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());
}

Names IDataType::getSubcolumnNames() const
{
    Names res;
    forEachSubcolumn([&](const auto &, const auto & name, const auto &)
    {
        res.push_back(name);
    }, getDefaultSerialization(), nullptr, nullptr);
    return res;
}

void IDataType::insertDefaultInto(IColumn & column) const
{
    column.insertDefault();
}

void IDataType::setCustomization(DataTypeCustomDescPtr custom_desc_) const
{
    /// replace only if not null
    if (custom_desc_->name)
        custom_name = std::move(custom_desc_->name);

    if (custom_desc_->serialization)
        custom_serialization = std::move(custom_desc_->serialization);
}

SerializationPtr IDataType::getDefaultSerialization() const
{
    if (custom_serialization)
        return custom_serialization;

    return doGetDefaultSerialization();
}

// static
SerializationPtr IDataType::getSerialization(const NameAndTypePair & column, const IDataType::StreamExistenceCallback & callback)
{
    if (column.isSubcolumn())
    {
        const auto & type_in_storage = column.getTypeInStorage();
        auto default_serialization = type_in_storage->getDefaultSerialization();
        return type_in_storage->getSubcolumnSerialization(column.getSubcolumnName(), default_serialization);
    }

    return column.type->getSerialization(column.name, callback);
}

SerializationPtr IDataType::getSerialization(const String &, const StreamExistenceCallback &) const
{
    return getDefaultSerialization();
}

}
