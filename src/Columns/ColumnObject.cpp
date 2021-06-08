#include <Columns/ColumnObject.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>


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

size_t ColumnObject::size() const
{
#ifndef NDEBUG
    checkConsistency();
#endif
    return subcolumns.empty() ? 0 : subcolumns.begin()->second.size();
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

void ColumnObject::forEachSubcolumn(ColumnCallback callback)
{
    for (auto & [_, column] : subcolumns)
        callback(column.data);
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

static bool isPrefix(const Strings & prefix, const Strings & strings)
{
    if (prefix.size() > strings.size())
        return false;

    for (size_t i = 0; i < prefix.size(); ++i)
        if (prefix[i] != strings[i])
            return false;
    return true;
}

void ColumnObject::optimizeTypesOfSubcolumns()
{
    if (optimized_types_of_subcolumns)
        return;

    size_t old_size = size();
    SubcolumnsMap new_subcolumns;
    for (auto && [name, subcolumn] : subcolumns)
    {
        auto from_type = getDataTypeByColumn(*subcolumn.data);
        const auto & to_type = subcolumn.least_common_type;

        if (isNothing(getBaseTypeOfArray(to_type)))
            continue;

        Strings name_parts;
        boost::split(name_parts, name, boost::is_any_of("."));

        for (const auto & [other_name, _] : subcolumns)
        {
            if (other_name.size() > name.size())
            {
                Strings other_name_parts;
                boost::split(other_name_parts, other_name, boost::is_any_of("."));

                if (isPrefix(name_parts, other_name_parts))
                    throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Data in Object has ambiguous paths: '{}' and '{}", name, other_name);
            }
        }

        if (to_type->equals(*from_type))
        {
            new_subcolumns[name] = std::move(subcolumn);
            continue;
        }

        size_t subcolumn_size = subcolumn.size();
        if (subcolumn.data->getRatioOfDefaultRows() == 1.0)
        {
            subcolumn.data = castColumn({subcolumn.data, from_type, ""}, to_type);
        }
        else
        {
            auto offsets = ColumnUInt64::create();
            auto & offsets_data = offsets->getData();

            subcolumn.data->getIndicesOfNonDefaultRows(offsets_data, 0, subcolumn_size);
            auto values = subcolumn.data->index(*offsets, offsets->size());

            values = castColumn({values, from_type, ""}, to_type);
            subcolumn.data = values->createWithOffsets(offsets_data, to_type->getDefault(), subcolumn_size, /*shift=*/ 0);
        }

        new_subcolumns[name] = std::move(subcolumn);
    }

    if (new_subcolumns.empty())
        new_subcolumns[COLUMN_NAME_DUMMY] = Subcolumn{ColumnUInt8::create(old_size)};

    std::swap(subcolumns, new_subcolumns);
    optimized_types_of_subcolumns = true;
}

}
