#include <Processors/Merges/SummingSortedTransform.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Row.h>
#include <Common/FieldVisitors.h>
#include <Columns/ColumnTuple.h>

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

    SummingSortedTransform::ColumnsDefinition defineColumns(
        const Block & header,
        const SortDescription & description,
        const Names & column_names_to_sum)
    {
        size_t num_columns = header.columns();
        SummingSortedTransform::ColumnsDefinition def;

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
                    def.column_numbers_not_to_aggregate.push_back(i);
                    continue;
                }

                discovered_maps[map_name].emplace_back(i);
            }
            else
            {
                bool is_agg_func = WhichDataType(column.type).isAggregateFunction();

                /// There are special const columns for example after prewhere sections.
                if ((!column.type->isSummable() && !is_agg_func) || isColumnConst(*column.column))
                {
                    def.column_numbers_not_to_aggregate.push_back(i);
                    continue;
                }

                /// Are they inside the PK?
                if (isInPrimaryKey(description, column.name, i))
                {
                    def.column_numbers_not_to_aggregate.push_back(i);
                    continue;
                }

                if (column_names_to_sum.empty()
                    || column_names_to_sum.end() !=
                       std::find(column_names_to_sum.begin(), column_names_to_sum.end(), column.name))
                {
                    // Create aggregator to sum this column
                    SummingSortedTransform::AggregateDescription desc;
                    desc.is_agg_func_type = is_agg_func;
                    desc.column_numbers = {i};

                    if (!is_agg_func)
                    {
                        desc.init("sumWithOverflow", {column.type});
                    }

                    def.columns_to_aggregate.emplace_back(std::move(desc));
                }
                else
                {
                    // Column is not going to be summed, use last value
                    def.column_numbers_not_to_aggregate.push_back(i);
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
                    def.column_numbers_not_to_aggregate.push_back(col);
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
                    def.column_numbers_not_to_aggregate.push_back(col);
                continue;
            }

            DataTypes argument_types;
            SummingSortedTransform::AggregateDescription desc;
            SummingSortedTransform::MapDescription map_desc;

            column_num_it = map.second.begin();
            for (; column_num_it != map.second.end(); ++column_num_it)
            {
                const ColumnWithTypeAndName & key_col = header.safeGetByPosition(*column_num_it);
                const String & name = key_col.name;
                const IDataType & nested_type = *assert_cast<const DataTypeArray &>(*key_col.type).getNestedType();

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
                    def.column_numbers_not_to_aggregate.push_back(col);
                continue;
            }

            if (map_desc.key_col_nums.size() == 1)
            {
                // Create summation for all value columns in the map
                desc.init("sumMapWithOverflow", argument_types);
                def.columns_to_aggregate.emplace_back(std::move(desc));
            }
            else
            {
                // Fall back to legacy mergeMaps for composite keys
                for (auto col : map.second)
                    def.column_numbers_not_to_aggregate.push_back(col);
                def.maps_to_sum.emplace_back(std::move(map_desc));
            }
        }
    }

    MutableColumns getMergedDataColumns(
        const Block & header,
        const SummingSortedTransform::ColumnsDefinition & columns_definition)
    {
        MutableColumns columns;
        columns.reserve(columns_definition.getNumColumns());

        for (auto & desc : columns_definition.columns_to_aggregate)
        {
            // Wrap aggregated columns in a tuple to match function signature
            if (!desc.is_agg_func_type && isTuple(desc.function->getReturnType()))
            {
                size_t tuple_size = desc.column_numbers.size();
                MutableColumns tuple_columns(tuple_size);
                for (size_t i = 0; i < tuple_size; ++i)
                    tuple_columns[i] = header.safeGetByPosition(desc.column_numbers[i]).column->cloneEmpty();

                columns.emplace_back(ColumnTuple::create(std::move(tuple_columns)));
            }
            else
                columns.emplace_back(header.safeGetByPosition(desc.column_numbers[0]).column->cloneEmpty());
        }

        for (auto & column_number : columns_definition.column_numbers_not_to_aggregate)
            columns.emplace_back(header.safeGetByPosition(column_number).type->createColumn());

        return columns;
    }

    void finalizeChunk(
        Chunk & chunk, size_t num_result_columns,
        const SummingSortedTransform::ColumnsDefinition & columns_definition)
    {
        size_t num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();

        Columns res_columns(num_result_columns);
        size_t next_column = 0;

        for (auto & desc : columns_definition.columns_to_aggregate)
        {
            auto column = std::move(columns[next_column]);
            ++next_column;

            if (!desc.is_agg_func_type && isTuple(desc.function->getReturnType()))
            {
                /// Unpack tuple into block.
                size_t tuple_size = desc.column_numbers.size();
                for (size_t i = 0; i < tuple_size; ++i)
                    res_columns[desc.column_numbers[i]] = assert_cast<const ColumnTuple &>(*column).getColumnPtr(i);
            }
            else
                res_columns[desc.column_numbers[0]] = std::move(column);
        }

        for (auto column_number : columns_definition.column_numbers_not_to_aggregate)
        {
            auto column = std::move(columns[next_column]);
            ++next_column;

            res_columns[column_number] = std::move(column);
        }

        chunk.setColumns(std::move(res_columns), num_rows);
    }
}

SummingSortedTransform::SummingSortedTransform(
        size_t num_inputs, const Block & header,
        SortDescription description_,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum,
        size_t max_block_size)
        : IMergingTransform(num_inputs, header, header, true)
        , columns_definition(defineColumns(header, description_, column_names_to_sum))
        , merged_data(getMergedDataColumns(header, columns_definition), false, max_block_size)
{
    size_t num_columns = header.columns();
    current_row.resize(num_columns);
}

void SummingSortedTransform::initializeInputs()
{
    queue = SortingHeap<SortCursor>(cursors);
    is_queue_initialized = true;
}

void SummingSortedTransform::consume(Chunk chunk, size_t input_number)
{
    updateCursor(std::move(chunk), input_number);

    if (is_queue_initialized)
        queue.push(cursors[input_number]);
}

void SummingSortedTransform::updateCursor(Chunk chunk, size_t source_num)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);

    auto & source_chunk = source_chunks[source_num];

    if (source_chunk)
    {
        /// Extend lifetime of last chunk.
        last_chunk = std::move(source_chunk);
        last_chunk_sort_columns = std::move(cursors[source_num].all_columns);

        source_chunk = std::move(chunk);
        cursors[source_num].reset(source_chunk.getColumns(), {});
    }
    else
    {
        if (cursors[source_num].has_collation)
            throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

        source_chunk = std::move(chunk);
        cursors[source_num] = SortCursorImpl(source_chunk.getColumns(), description, source_num);
    }
}

void SummingSortedTransform::work()
{
    merge();
    prepareOutputChunk(merged_data);

    if (has_output_chunk)
        finalizeChunk(output_chunk, getOutputs().back().getHeader().columns(), columns_definition);
}

void SummingSortedTransform::merge()
{
    /// Take the rows in needed order and put them in `merged_columns` until rows no more than `max_block_size`
    while (queue.isValid())
    {
        bool key_differs;
        bool has_previous_group = !last_key.empty();

        SortCursor current = queue.current();

        {
            RowRef current_key;
            current_key.set(current);

            if (!has_previous_group)    /// The first key encountered.
            {
                key_differs = true;
                current_row_is_zero = true;
            }
            else
                key_differs = !last_key.hasEqualSortColumnsWith(current_key);

            last_key = current_key;
            last_chunk_sort_columns.clear();
        }

        if (key_differs)
        {
            if (has_previous_group)
                /// Write the data for the previous group.
                insertCurrentRowIfNeeded();

            if (merged_data.hasEnoughRows())
            {
                /// The block is now full and the last row is calculated completely.
                last_key.reset();
                return;
            }

            setRow(current_row, current);

            /// Reset aggregation states for next row
            for (auto & desc : columns_definition.columns_to_aggregate)
                desc.createState();

            // Start aggregations with current row
            addRow(current);

            if (columns_definition.maps_to_sum.empty())
            {
                /// We have only columns_to_aggregate. The status of current row will be determined
                /// in 'insertCurrentRowIfNeeded' method on the values of aggregate functions.
                current_row_is_zero = true; // NOLINT
            }
            else
            {
                /// We have complex maps that will be summed with 'mergeMap' method.
                /// The single row is considered non zero, and the status after merging with other rows
                /// will be determined in the branch below (when key_differs == false).
                current_row_is_zero = false; // NOLINT
            }
        }
        else
        {
            addRow(current);

            // Merge maps only for same rows
            for (const auto & desc : columns_definition.maps_to_sum)
                if (mergeMap(desc, current_row, current))
                    current_row_is_zero = false;
        }

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
            queue.removeTop();
            requestDataForInput(current.impl->order);
            return;
        }
    }

    /// We will write the data for the last group, if it is non-zero.
    /// If it is zero, and without it the output stream will be empty, we will write it anyway.
    insertCurrentRowIfNeeded();
    last_chunk_sort_columns.clear();
    is_finished = true;
}



}
