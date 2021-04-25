#include <Columns/ColumnObject.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

ColumnObject::ColumnObject(const SubcolumnsMap & subcolumns_)
    : subcolumns(subcolumns_)
{
    checkConsistency();
}

ColumnObject::ColumnObject(const Names & keys, const Columns & subcolumns_)
{
    if (keys.size() != subcolumns_.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sizes of keys ({}) and subcolumns ({}) are inconsistent");

    for (size_t i = 0; i < keys.size(); ++i)
        subcolumns[keys[i]] = subcolumns_[i];

    checkConsistency();
}

void ColumnObject::checkConsistency() const
{
    if (subcolumns.empty())
        return;

    size_t first_size = subcolumns.begin()->second->size();
    for (const auto & [name, column] : subcolumns)
    {
        if (!column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Null subcolumn passed to ColumnObject");

        if (first_size != column->size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sizes of subcolumns are inconsistent in ColumnObject."
                " Subcolumn '{}' has {} rows, subcolumn '{}' has {} rows",
                subcolumns.begin()->first, first_size, name, column->size());
        }
    }
}

MutableColumnPtr ColumnObject::cloneResized(size_t new_size) const
{
    SubcolumnsMap new_subcolumns;
    for (const auto & [key, subcolumn] : subcolumns)
        new_subcolumns[key] = subcolumn->cloneResized(new_size);

    return ColumnObject::create(new_subcolumns);
}

size_t ColumnObject::byteSize() const
{
    size_t res = 0;
    for (const auto & [_, column] : subcolumns)
        res += column->byteSize();
    return res;
}

size_t ColumnObject::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & [_, column] : subcolumns)
        res += column->allocatedBytes();
    return res;
}

// const ColumnPtr & ColumnObject::tryGetSubcolumn(const String & key) const
// {
//     auto it = subcolumns.find(key);
//     if (it == subcolumns.end())
//         return nullptr;

//     return it->second;
// }

const IColumn & ColumnObject::getSubcolumn(const String & key) const
{
    auto it = subcolumns.find(key);
    if (it != subcolumns.end())
        return *it->second;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key);
}

IColumn & ColumnObject::getSubcolumn(const String & key)
{
    auto it = subcolumns.find(key);
    if (it != subcolumns.end())
        return *it->second;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key);
}

bool ColumnObject::hasSubcolumn(const String & key) const
{
    return subcolumns.count(key) != 0;
}

void ColumnObject::addSubcolumn(const String & key, MutableColumnPtr && subcolumn, bool check_size)
{
    if (check_size && subcolumn->size() != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot add subcolumn '{}' with {} rows to ColumnObject with {} rows",
            key, subcolumn->size(), size());

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
