#include <DataStreams/SummingSortedBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnTuple.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/FieldVisitors.h>
#include <common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{
    bool isInPrimaryKey(const SortDescription & description, const std::string & name, const size_t number)
    {
        for (auto & desc : description)
            if (desc.column_name == name || (desc.column_name.empty() && desc.column_number == number))
                return true;

        return false;
    }
}


SummingSortedBlockInputStream::SummingSortedBlockInputStream(
    const BlockInputStreams & inputs_,
    const SortDescription & description_,
    /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
    const Names & column_names_to_sum,
    size_t max_block_size_)
    : MergingSortedBlockInputStream(inputs_, description_, max_block_size_)
{
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
            if (!column.type->isSummable() && !is_agg_func)
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
                if (!nested_type.isValueRepresentedByInteger())
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


void SummingSortedBlockInputStream::insertCurrentRowIfNeeded(MutableColumns & merged_columns)
{
    for (auto & desc : columns_to_aggregate)
    {
        // Do not insert if the aggregation state hasn't been created
        if (desc.created)
        {
            if (desc.is_agg_func_type)
            {
                current_row_is_zero = false;
            }
            else
            {
                try
                {
                    desc.function->insertResultInto(desc.state.data(), *desc.merged_column);

                    /// Update zero status of current row
                    if (desc.column_numbers.size() == 1)
                    {
                        // Flag row as non-empty if at least one column number if non-zero
                        current_row_is_zero = current_row_is_zero && desc.merged_column->isDefaultAt(desc.merged_column->size() - 1);
                    }
                    else
                    {
                        /// It is sumMapWithOverflow aggregate function.
                        /// Assume that the row isn't empty in this case (just because it is compatible with previous version)
                        current_row_is_zero = false;
                    }
                }
                catch (...)
                {
                    desc.destroyState();
                    throw;
                }
            }
            desc.destroyState();
        }
        else
            desc.merged_column->insertDefault();
    }

    /// If it is "zero" row, then rollback the insertion
    /// (at this moment we need rollback only cols from columns_to_aggregate)
    if (current_row_is_zero)
    {
        for (auto & desc : columns_to_aggregate)
            desc.merged_column->popBack(1);

        return;
    }

    for (auto i : column_numbers_not_to_aggregate)
        merged_columns[i]->insert(current_row[i]);

    /// Update per-block and per-group flags
    ++merged_rows;
}


Block SummingSortedBlockInputStream::readImpl()
{
    if (finished)
        return Block();

    MutableColumns merged_columns;
    init(merged_columns);

    if (has_collation)
        throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

    if (merged_columns.empty())
        return {};

    /// Update aggregation result columns for current block
    for (auto & desc : columns_to_aggregate)
    {
        // Wrap aggregated columns in a tuple to match function signature
        if (!desc.is_agg_func_type && isTuple(desc.function->getReturnType()))
        {
            size_t tuple_size = desc.column_numbers.size();
            MutableColumns tuple_columns(tuple_size);
            for (size_t i = 0; i < tuple_size; ++i)
                tuple_columns[i] = header.safeGetByPosition(desc.column_numbers[i]).column->cloneEmpty();

            desc.merged_column = ColumnTuple::create(std::move(tuple_columns));
        }
        else
            desc.merged_column = header.safeGetByPosition(desc.column_numbers[0]).column->cloneEmpty();
    }

    merge(merged_columns, queue_without_collation);
    Block res = header.cloneWithColumns(std::move(merged_columns));

    /// Place aggregation results into block.
    for (auto & desc : columns_to_aggregate)
    {
        if (!desc.is_agg_func_type && isTuple(desc.function->getReturnType()))
        {
            /// Unpack tuple into block.
            size_t tuple_size = desc.column_numbers.size();
            for (size_t i = 0; i < tuple_size; ++i)
                res.getByPosition(desc.column_numbers[i]).column = static_cast<const ColumnTuple &>(*desc.merged_column).getColumnPtr(i);
        }
        else
            res.getByPosition(desc.column_numbers[0]).column = std::move(desc.merged_column);
    }

    return res;
}


void SummingSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    merged_rows = 0;

    /// Take the rows in needed order and put them in `merged_columns` until rows no more than `max_block_size`
    while (!queue.empty())
    {
        SortCursor current = queue.top();

        setPrimaryKeyRef(next_key, current);

        bool key_differs;

        if (current_key.empty())    /// The first key encountered.
        {
            key_differs = true;
            current_row_is_zero = true;
        }
        else
            key_differs = next_key != current_key;

        if (key_differs)
        {
            if (!current_key.empty())
                /// Write the data for the previous group.
                insertCurrentRowIfNeeded(merged_columns);

            if (merged_rows >= max_block_size)
            {
                /// The block is now full and the last row is calculated completely.
                current_key.reset();
                return;
            }

            current_key.swap(next_key);

            setRow(current_row, current);

            /// Reset aggregation states for next row
            for (auto & desc : columns_to_aggregate)
                desc.createState();

            // Start aggregations with current row
            addRow(current);

            if (maps_to_sum.empty())
            {
                /// We have only columns_to_aggregate. The status of current row will be determined
                /// in 'insertCurrentRowIfNeeded' method on the values of aggregate functions.
                current_row_is_zero = true;
            }
            else
            {
                /// We have complex maps that will be summed with 'mergeMap' method.
                /// The single row is considered non zero, and the status after merging with other rows
                /// will be determined in the branch below (when key_differs == false).
                current_row_is_zero = false;
            }
        }
        else
        {
            addRow(current);

            // Merge maps only for same rows
            for (const auto & desc : maps_to_sum)
                if (mergeMap(desc, current_row, current))
                    current_row_is_zero = false;
        }

        queue.pop();

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
            fetchNextBlock(current, queue);
        }
    }

    /// We will write the data for the last group, if it is non-zero.
    /// If it is zero, and without it the output stream will be empty, we will write it anyway.
    insertCurrentRowIfNeeded(merged_columns);
    finished = true;
}


bool SummingSortedBlockInputStream::mergeMap(const MapDescription & desc, Row & row, SortCursor & cursor)
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


void SummingSortedBlockInputStream::addRow(SortCursor & cursor)
{
    for (auto & desc : columns_to_aggregate)
    {
        if (!desc.created)
            throw Exception("Logical error in SummingSortedBlockInputStream, there are no description", ErrorCodes::LOGICAL_ERROR);

        if (desc.is_agg_func_type)
        {
            // desc.state is not used for AggregateFunction types
            auto & col = cursor->all_columns[desc.column_numbers[0]];
            static_cast<ColumnAggregateFunction &>(*desc.merged_column).insertMergeFrom(*col, cursor->pos);
        }
        else
        {
            // Specialized case for unary functions
            if (desc.column_numbers.size() == 1)
            {
                auto & col = cursor->all_columns[desc.column_numbers[0]];
                desc.add_function(desc.function.get(), desc.state.data(), &col, cursor->pos, nullptr);
            }
            else
            {
                // Gather all source columns into a vector
                ColumnRawPtrs columns(desc.column_numbers.size());
                for (size_t i = 0; i < desc.column_numbers.size(); ++i)
                    columns[i] = cursor->all_columns[desc.column_numbers[i]];

                desc.add_function(desc.function.get(), desc.state.data(), columns.data(), cursor->pos, nullptr);
            }
        }
    }
}

}
