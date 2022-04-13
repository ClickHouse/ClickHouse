#include <Formats/RowInputMissingColumnsFiller.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/NestedUtils.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


RowInputMissingColumnsFiller::RowInputMissingColumnsFiller() = default;

RowInputMissingColumnsFiller::RowInputMissingColumnsFiller(const NamesAndTypesList & names_and_types)
{
    std::unordered_map<std::string_view, std::vector<size_t>> nested_groups; /// Nested prefix -> column indices.
    size_t i = 0;
    for (auto it = names_and_types.begin(); it != names_and_types.end(); ++it, ++i)
    {
        const auto & name_and_type = *it;
        if (isArray(name_and_type.type))
        {
            auto split = Nested::splitName(name_and_type.name);
            if (!split.second.empty()) /// Is it really a column of Nested data structure?
                nested_groups[split.first].push_back(i);
        }
    }
    setNestedGroups(std::move(nested_groups), names_and_types.size());
}

RowInputMissingColumnsFiller::RowInputMissingColumnsFiller(const Names & names, const DataTypes & types)
{
    std::unordered_map<std::string_view, std::vector<size_t>> nested_groups; /// Nested prefix -> column indices.
    for (size_t i = 0; i != names.size(); ++i)
    {
        if (isArray(types[i]))
        {
            auto split = Nested::splitName(names[i]);
            if (!split.second.empty()) /// Is it really a column of Nested data structure?
                nested_groups[split.first].push_back(i);
        }
    }
    setNestedGroups(std::move(nested_groups), names.size());
}

RowInputMissingColumnsFiller::RowInputMissingColumnsFiller(size_t count, const std::string_view * names, const DataTypePtr * types)
{
    std::unordered_map<std::string_view, std::vector<size_t>> nested_groups; /// Nested prefix -> column indices.
    for (size_t i = 0; i != count; ++i)
    {
        if (isArray(types[i]))
        {
            auto split = Nested::splitName(names[i]);
            if (!split.second.empty()) /// Is it really a column of Nested data structure?
                nested_groups[split.first].push_back(i);
        }
    }
    setNestedGroups(std::move(nested_groups), count);
}

void RowInputMissingColumnsFiller::setNestedGroups(std::unordered_map<std::string_view, std::vector<size_t>> && nested_groups, size_t num_columns)
{
    if (!nested_groups.empty())
    {
        column_infos.resize(num_columns);
        for (auto & nested_group : nested_groups | boost::adaptors::map_values)
        {
            if (nested_group.size() <= 1)
                continue;
            auto nested_group_shared = std::make_shared<std::vector<size_t>>(std::move(nested_group));
            for (size_t i : *nested_group_shared)
                column_infos[i].nested_group = nested_group_shared;
        }
    }
}


void RowInputMissingColumnsFiller::addDefaults(MutableColumns & columns, size_t row_num) const
{
    for (size_t i = 0; i != columns.size(); ++i)
    {
        auto & column = *columns[i];
        size_t column_size = column.size();
        if (row_num < column_size)
            continue; /// The column already has an element in this position, skipping.

        if (row_num > column_size)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Wrong row_number {}, expected either {} or {}", row_num, column_size - 1, column_size);

        if ((i >= column_infos.size()) || !column_infos[i].nested_group)
        {
            column.insertDefault();
            continue;
        }

        const auto & nested_group = *column_infos[i].nested_group;
        size_t size_of_array = 0;
        for (size_t j : nested_group)
        {
            const auto & column_j = columns[j];
            size_t column_size_j = column_j->size();
            if (row_num < column_size_j)
            {
                const auto * column_array = typeid_cast<const ColumnArray *>(column_j.get());
                if (!column_array)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column with Array type is not represented by ColumnArray column: {}", column_j->dumpStructure());
                const auto & offsets = column_array->getOffsets();
                size_of_array = offsets[row_num] - offsets[row_num - 1];
                break;
            }
        }

        for (size_t j : nested_group)
        {
            auto & column_j = columns[j];
            size_t column_size_j = column_j->size();
            if (row_num >= column_size_j)
            {
                if (row_num > column_size_j)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Wrong row_number {}, expected either {} or {}", row_num, column_size_j - 1, column_size_j);

                auto * column_array = typeid_cast<ColumnArray *>(column_j.get());
                if (!column_array)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column with Array type is not represented by ColumnArray column: {}", column_j->dumpStructure());

                auto & data = column_array->getData();
                auto & offsets = column_array->getOffsets();
                for (size_t k = 0; k != size_of_array; ++k)
                    data.insertDefault();
                offsets.push_back(data.size());
            }
        }
    }
}

}
