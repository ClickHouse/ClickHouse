#include <Processors/Merges/SummingSortedTransform.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Row.h>
#include <Common/FieldVisitors.h>

namespace DB
{

namespace
{
bool isInPrimaryKey(const SortDescription & description, const std::string & name, const size_t number)
{
    for (auto & desc : description)
        if (desc.column_name == name || (desc.column_name.empty() && desc.column_number == number))
            return true;

    return false;
}

/// Returns true if merge result is not empty
bool mergeMap(const SummingSortedTransform::MapDescription & desc, Row & row, SortCursor & cursor)
{
    /// Strongly non-optimal.

    Row & left = row;
    Row right(left.size());

    for (size_t col_num : desc.key_col_nums)
        right[col_num] = (*cursor->all_columns[col_num])[cursor->pos].template get<Array>();

    for (size_t col_num : desc.val_col_nums)
        right[col_num] = (*cursor->all_columns[col_num])[cursor->pos].template get<Array>();

    auto at_ith_column_jth_row = [&](const Row & matrix, size_t i, size_t j) -> const Field &
    {
        return matrix[i].get<Array>()[j];
    };

    auto tuple_of_nth_columns_at_jth_row = [&](const Row & matrix, const ColumnNumbers & col_nums, size_t j) -> Array
    {
        size_t size = col_nums.size();
        Array res(size);
        for (size_t col_num_index = 0; col_num_index < size; ++col_num_index)
            res[col_num_index] = at_ith_column_jth_row(matrix, col_nums[col_num_index], j);
        return res;
    };

    std::map<Array, Array> merged;

    auto accumulate = [](Array & dst, const Array & src)
    {
        bool has_non_zero = false;
        size_t size = dst.size();
        for (size_t i = 0; i < size; ++i)
            if (applyVisitor(FieldVisitorSum(src[i]), dst[i]))
                has_non_zero = true;
        return has_non_zero;
    };

    auto merge = [&](const Row & matrix)
    {
        size_t rows = matrix[desc.key_col_nums[0]].get<Array>().size();

        for (size_t j = 0; j < rows; ++j)
        {
            Array key = tuple_of_nth_columns_at_jth_row(matrix, desc.key_col_nums, j);
            Array value = tuple_of_nth_columns_at_jth_row(matrix, desc.val_col_nums, j);

            auto it = merged.find(key);
            if (merged.end() == it)
                merged.emplace(std::move(key), std::move(value));
            else
            {
                if (!accumulate(it->second, value))
                    merged.erase(it);
            }
        }
    };

    merge(left);
    merge(right);

    for (size_t col_num : desc.key_col_nums)
        row[col_num] = Array(merged.size());
    for (size_t col_num : desc.val_col_nums)
        row[col_num] = Array(merged.size());

    size_t row_num = 0;
    for (const auto & key_value : merged)
    {
        for (size_t col_num_index = 0, size = desc.key_col_nums.size(); col_num_index < size; ++col_num_index)
            row[desc.key_col_nums[col_num_index]].get<Array>()[row_num] = key_value.first[col_num_index];

        for (size_t col_num_index = 0, size = desc.val_col_nums.size(); col_num_index < size; ++col_num_index)
            row[desc.val_col_nums[col_num_index]].get<Array>()[row_num] = key_value.second[col_num_index];

        ++row_num;
    }

    return row_num != 0;
}
}

SummingSortedTransform::SummingSortedTransform(
        size_t num_inputs, const Block & header,
        SortDescription description,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum,
        size_t max_block_size)
        : IMergingTransform(num_inputs, header, header, true)
{
    size_t num_columns = header.columns();
    current_row.resize(num_columns);

    /// name of nested structure -> the column numbers that refer to it.
    std::unordered_map<std::string, std::vector<size_t>> discovered_maps;

    /** Fill in the column numbers, which must be summed.
        * This can only be numeric columns that are not part of the sort key.
        * If a non-empty column_names_to_sum is specified, then we only take these columns.
        * Some columns from column_names_to_sum may not be found. This is ignored.
        */
    for (size_t i = 0; i < num_columns; ++i)
    {
        const ColumnWithTypeAndName & column = header.safeGetByPosition(i);

        /// Discover nested Maps and find columns for summation
        if (typeid_cast<const DataTypeArray *>(column.type.get()))
        {
            const auto map_name = Nested::extractTableName(column.name);
            /// if nested table name ends with `Map` it is a possible candidate for special handling
            if (map_name == column.name || !endsWith(map_name, "Map"))
            {
                column_numbers_not_to_aggregate.push_back(i);
                continue;
            }

            discovered_maps[map_name].emplace_back(i);
        }
        else
        {
            bool is_agg_func = WhichDataType(column.type).isAggregateFunction();

            /// There are special const columns for example after prewere sections.
            if ((!column.type->isSummable() && !is_agg_func) || isColumnConst(*column.column))
            {
                column_numbers_not_to_aggregate.push_back(i);
                continue;
            }

            /// Are they inside the PK?
            if (isInPrimaryKey(description, column.name, i))
            {
                column_numbers_not_to_aggregate.push_back(i);
                continue;
            }

            if (column_names_to_sum.empty()
                || column_names_to_sum.end() !=
                   std::find(column_names_to_sum.begin(), column_names_to_sum.end(), column.name))
            {
                // Create aggregator to sum this column
                AggregateDescription desc;
                desc.is_agg_func_type = is_agg_func;
                desc.column_numbers = {i};

                if (!is_agg_func)
                {
                    desc.init("sumWithOverflow", {column.type});
                }

                columns_to_aggregate.emplace_back(std::move(desc));
            }
            else
            {
                // Column is not going to be summed, use last value
                column_numbers_not_to_aggregate.push_back(i);
            }
        }
    }

    /// select actual nested Maps from list of candidates
    for (const auto & map : discovered_maps)
    {
        /// map should contain at least two elements (key -> value)
        if (map.second.size() < 2)
        {
            for (auto col : map.second)
                column_numbers_not_to_aggregate.push_back(col);
            continue;
        }

        /// no elements of map could be in primary key
        auto column_num_it = map.second.begin();
        for (; column_num_it != map.second.end(); ++column_num_it)
            if (isInPrimaryKey(description, header.safeGetByPosition(*column_num_it).name, *column_num_it))
                break;
        if (column_num_it != map.second.end())
        {
            for (auto col : map.second)
                column_numbers_not_to_aggregate.push_back(col);
            continue;
        }

        DataTypes argument_types;
        AggregateDescription desc;
        MapDescription map_desc;

        column_num_it = map.second.begin();
        for (; column_num_it != map.second.end(); ++column_num_it)
        {
            const ColumnWithTypeAndName & key_col = header.safeGetByPosition(*column_num_it);
            const String & name = key_col.name;
            const IDataType & nested_type = *static_cast<const DataTypeArray *>(key_col.type.get())->getNestedType();

            if (column_num_it == map.second.begin()
                || endsWith(name, "ID")
                || endsWith(name, "Key")
                || endsWith(name, "Type"))
            {
                if (!nested_type.isValueRepresentedByInteger() && !isStringOrFixedString(nested_type))
                    break;

                map_desc.key_col_nums.push_back(*column_num_it);
            }
            else
            {
                if (!nested_type.isSummable())
                    break;

                map_desc.val_col_nums.push_back(*column_num_it);
            }

            // Add column to function arguments
            desc.column_numbers.push_back(*column_num_it);
            argument_types.push_back(key_col.type);
        }

        if (column_num_it != map.second.end())
        {
            for (auto col : map.second)
                column_numbers_not_to_aggregate.push_back(col);
            continue;
        }

        if (map_desc.key_col_nums.size() == 1)
        {
            // Create summation for all value columns in the map
            desc.init("sumMapWithOverflow", argument_types);
            columns_to_aggregate.emplace_back(std::move(desc));
        }
        else
        {
            // Fall back to legacy mergeMaps for composite keys
            for (auto col : map.second)
                column_numbers_not_to_aggregate.push_back(col);
            maps_to_sum.emplace_back(std::move(map_desc));
        }
    }
}

}
