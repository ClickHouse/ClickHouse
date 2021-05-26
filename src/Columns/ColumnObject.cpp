#include <Columns/ColumnObject.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeNothing.h>
#include <Interpreters/castColumn.h>

#include <Common/FieldVisitors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
}

ColumnObject::Subcolumn::Subcolumn(const Subcolumn & other)
    : data(other.data), least_common_type(other.least_common_type)
{
}

ColumnObject::Subcolumn::Subcolumn(MutableColumnPtr && data_)
    : data(std::move(data_)), least_common_type(getDataTypeByColumn(*data))
{
}

void ColumnObject::Subcolumn::insert(const Field & field, const DataTypePtr & value_type)
{
    data->insert(field);
    least_common_type = getLeastSupertype({least_common_type, value_type}, true);
}

void ColumnObject::Subcolumn::insertDefault()
{
    data->insertDefault();
}

ColumnObject::ColumnObject(SubcolumnsMap && subcolumns_)
    : subcolumns(std::move(subcolumns_))
{
    checkConsistency();
}

void ColumnObject::checkConsistency() const
{
    if (subcolumns.empty())
        return;

    size_t first_size = subcolumns.begin()->second.size();
    for (const auto & [name, column] : subcolumns)
    {
        if (!column.data)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Null subcolumn passed to ColumnObject");

        if (first_size != column.data->size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sizes of subcolumns are inconsistent in ColumnObject."
                " Subcolumn '{}' has {} rows, subcolumn '{}' has {} rows",
                subcolumns.begin()->first, first_size, name, column.data->size());
        }
    }
}

MutableColumnPtr ColumnObject::cloneResized(size_t new_size) const
{
    if (new_size != 0)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "ColumnObject doesn't support resize to non-zero length");

    return ColumnObject::create();
}

size_t ColumnObject::byteSize() const
{
    size_t res = 0;
    for (const auto & [_, column] : subcolumns)
        res += column.data->byteSize();
    return res;
}

size_t ColumnObject::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & [_, column] : subcolumns)
        res += column.data->allocatedBytes();
    return res;
}

const ColumnObject::Subcolumn & ColumnObject::getSubcolumn(const String & key) const
{
    auto it = subcolumns.find(key);
    if (it != subcolumns.end())
        return it->second;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key);
}

ColumnObject::Subcolumn & ColumnObject::getSubcolumn(const String & key)
{
    auto it = subcolumns.find(key);
    if (it != subcolumns.end())
        return it->second;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key);
}

bool ColumnObject::hasSubcolumn(const String & key) const
{
    return subcolumns.count(key) != 0;
}

void ColumnObject::addSubcolumn(const String & key, const ColumnPtr & column_sample, size_t new_size, bool check_size)
{
    if (subcolumns.count(key))
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Subcolumn '{}' already exists", key);

    if (!column_sample->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot add subcolumn '{}' with non-empty sample column", key);

    if (check_size && new_size != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot add subcolumn '{}' with {} rows to ColumnObject with {} rows",
            key, new_size, size());

    auto & subcolumn = subcolumns[key];
    subcolumn.data = column_sample->cloneResized(new_size);
    subcolumn.least_common_type = std::make_shared<DataTypeNothing>();
}

void ColumnObject::addSubcolumn(const String & key, Subcolumn && subcolumn, bool check_size)
{
    if (subcolumns.count(key))
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Subcolumn '{}' already exists", key);

    if (check_size && subcolumn.size() != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot add subcolumn '{}' with {} rows to ColumnObject with {} rows",
            key, subcolumn.size(), size());

    subcolumns[key] = std::move(subcolumn);
}

Names ColumnObject::getKeys() const
{
    Names keys;
    keys.reserve(subcolumns.size());
    for (const auto & [key, _] : subcolumns)
        keys.emplace_back(key);
    return keys;
}

void ColumnObject::optimizeTypesOfSubcolumns()
{
    if (optimized_types_of_subcolumns)
        return;

    for (auto & [_, subcolumn] : subcolumns)
    {
        auto from_type = getDataTypeByColumn(*subcolumn.data);
        if (subcolumn.least_common_type->equals(*from_type))
            continue;

        size_t subcolumn_size = subcolumn.size();
        if (subcolumn.data->getNumberOfDefaultRows(/*step=*/ 1) == 0)
        {
            subcolumn.data = castColumn({subcolumn.data, from_type, ""}, subcolumn.least_common_type);
        }
        else
        {
            auto offsets = ColumnUInt64::create();
            auto & offsets_data = offsets->getData();

            subcolumn.data->getIndicesOfNonDefaultValues(offsets_data, 0, subcolumn_size);

            auto values = subcolumn.data->index(*offsets, offsets->size());
            values = castColumn({values, from_type, ""}, subcolumn.least_common_type);

            subcolumn.data = values->createWithOffsets(
                offsets_data, subcolumn.least_common_type->getDefault(), subcolumn_size, /*shift=*/ 0);
        }
    }

    optimized_types_of_subcolumns = true;
}

}
