#include <Columns/ColumnObject.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
}

static TypeId getLeastSuperTypeId(const PaddedPODArray<TypeIndex> & type_ids)
{

}

ColumnObject::Subcolumn::Subcolumn(const Subcolumn & other)
    : data(other.data), type_ids(other.type_ids.begin(), other.type_ids.end())
{
}

ColumnObject::Subcolumn::Subcolumn(MutableColumnPtr && data_)
    : data(std::move(data_))
{
}

void ColumnObject::Subcolumn::insert(const Field & field, TypeIndex type_id)
{
    data->insert(field);
    type_ids.push_back(type_id);
}

void ColumnObject::Subcolumn::insertDefault()
{
    data->insertDefault();
    type_ids.push_back(TypeIndex::Nothing);
}

void ColumnObject::Subcolumn::resize(size_t new_size)
{
    data = data->cloneResized(new_size);
    type_ids.resize_fill(new_size, TypeIndex::Nothing);
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
    SubcolumnsMap new_subcolumns;
    for (const auto & [key, subcolumn] : subcolumns)
        new_subcolumns[key].resize(new_size);

    return ColumnObject::create(std::move(new_subcolumns));
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

void ColumnObject::addSubcolumn(const String & key, MutableColumnPtr && column_sample, size_t new_size, bool check_size)
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
    subcolumn = std::move(column_sample);
    subcolumn.resize(new_size);
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

}
