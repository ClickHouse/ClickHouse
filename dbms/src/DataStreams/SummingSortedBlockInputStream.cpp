#include <DataStreams/SummingSortedBlockInputStream.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/StringUtils.h>
#include <Core/FieldVisitors.h>
#include <common/logger_useful.h>


namespace DB
{


String SummingSortedBlockInputStream::getID() const
{
    std::stringstream res;
    res << "SummingSorted(inputs";

    for (size_t i = 0; i < children.size(); ++i)
        res << ", " << children[i]->getID();

    res << ", description";

    for (size_t i = 0; i < description.size(); ++i)
        res << ", " << description[i].getID();

    res << ")";
    return res.str();
}


void SummingSortedBlockInputStream::insertCurrentRow(ColumnPlainPtrs & merged_columns)
{
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i]->insert(current_row[i]);
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


Block SummingSortedBlockInputStream::readImpl()
{
    if (finished)
        return Block();

    if (children.size() == 1)
        return children[0]->read();

    Block merged_block;
    ColumnPlainPtrs merged_columns;

    init(merged_block, merged_columns);
    if (merged_columns.empty())
        return Block();

    /// Additional initialization.
    if (current_row.empty())
    {
        current_row.resize(num_columns);
        next_key.columns.resize(description.size());

        /// name of nested structure -> the column numbers that refer to it.
        std::unordered_map<std::string, std::vector<size_t>> discovered_maps;

        /** Fill in the column numbers, which must be summed.
          * This can only be numeric columns that are not part of the sort key.
          * If a non-empty column_names_to_sum is specified, then we only take these columns.
          * Some columns from column_names_to_sum may not be found. This is ignored.
          */
        for (size_t i = 0; i < num_columns; ++i)
        {
            ColumnWithTypeAndName & column = merged_block.safeGetByPosition(i);

            /// Discover nested Maps and find columns for summation
            if (typeid_cast<const DataTypeArray *>(column.type.get()))
            {
                const auto map_name = DataTypeNested::extractNestedTableName(column.name);
                /// if nested table name ends with `Map` it is a possible candidate for special handling
                if (map_name == column.name || !endsWith(map_name, "Map"))
                    continue;

                discovered_maps[map_name].emplace_back(i);
            }
            else
            {
                /// Leave only numeric types. Note that dates and datetime here are not considered such.
                if (!column.type->isNumeric() ||
                    column.type->getName() == "Date" ||
                    column.type->getName() == "DateTime" ||
                    column.type->getName() == "Nullable(Date)" ||
                    column.type->getName() == "Nullable(DateTime)")
                    continue;

                /// Do they enter the PK?
                if (isInPrimaryKey(description, column.name, i))
                    continue;

                if (column_names_to_sum.empty()
                    || column_names_to_sum.end() !=
                       std::find(column_names_to_sum.begin(), column_names_to_sum.end(), column.name))
                {
                    column_numbers_to_sum.push_back(i);
                }
            }
        }

        /// select actual nested Maps from list of candidates
        for (const auto & map : discovered_maps)
        {
            /// map should contain at least two elements (key -> value)
            if (map.second.size() < 2)
                continue;

            /// no elements of map could be in primary key
            auto column_num_it = map.second.begin();
            for (; column_num_it != map.second.end(); ++column_num_it)
                if (isInPrimaryKey(description, merged_block.safeGetByPosition(*column_num_it).name, *column_num_it))
                    break;
            if (column_num_it != map.second.end())
                continue;

            /// collect key and value columns
            MapDescription map_description;

            column_num_it = map.second.begin();
            for (; column_num_it != map.second.end(); ++column_num_it)
            {
                const ColumnWithTypeAndName & key_col = merged_block.safeGetByPosition(*column_num_it);
                const String & name = key_col.name;
                const IDataType & nested_type = *static_cast<const DataTypeArray *>(key_col.type.get())->getNestedType();

                if (column_num_it == map.second.begin()
                    || endsWith(name, "ID")
                    || endsWith(name, "Key")
                    || endsWith(name, "Type"))
                {
                    if (!nested_type.isNumeric()
                        || nested_type.getName() == "Float32"
                        || nested_type.getName() == "Float64")
                        break;

                    map_description.key_col_nums.emplace_back(*column_num_it);
                }
                else
                {
                    if (!nested_type.behavesAsNumber())
                        break;

                    map_description.val_col_nums.emplace_back(*column_num_it);
                }
            }
            if (column_num_it != map.second.end())
                continue;

            maps_to_sum.emplace_back(std::move(map_description));
        }
    }

    if (has_collation)
        merge(merged_columns, queue_with_collation);
    else
        merge(merged_columns, queue);

    return merged_block;
}


template <class TSortCursor>
void SummingSortedBlockInputStream::merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue)
{
    size_t merged_rows = 0;

    /// Take the rows in needed order and put them in `merged_block` until rows no more than `max_block_size`
    while (!queue.empty())
    {
        TSortCursor current = queue.top();

        setPrimaryKeyRef(next_key, current);

        bool key_differs;

        if (current_key.empty())    /// The first key encountered.
         {
            current_key.columns.resize(description.size());
            setPrimaryKeyRef(current_key, current);
            key_differs = true;
        }
        else
            key_differs = next_key != current_key;

        /// if there are enough rows and the last one is calculated completely
        if (key_differs && merged_rows >= max_block_size)
            return;

        queue.pop();

        if (key_differs)
        {
            /// Write the data for the previous group.
            if (!current_row_is_zero)
            {
                ++merged_rows;
                output_is_non_empty = true;
                insertCurrentRow(merged_columns);
            }

            current_key.swap(next_key);

            setRow(current_row, current);
            current_row_is_zero = false;
        }
        else
        {
            current_row_is_zero = !addRow(current_row, current);
        }

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
    if (!current_row_is_zero || !output_is_non_empty)
    {
        ++merged_rows;
        insertCurrentRow(merged_columns);
    }

    finished = true;
}


/** Implements `+=` operation.
 *  Returns false if the result is zero.
 */
class FieldVisitorSum : public StaticVisitor<bool>
{
private:
    const Field & rhs;
public:
    FieldVisitorSum(const Field & rhs_) : rhs(rhs_) {}

    bool operator() (UInt64     & x) const { x += get<UInt64>(rhs); return x != 0; }
    bool operator() (Int64         & x) const { x += get<Int64>(rhs); return x != 0; }
    bool operator() (Float64     & x) const { x += get<Float64>(rhs); return x != 0; }

    bool operator() (Null         & x) const { throw Exception("Cannot sum Nulls", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (String     & x) const { throw Exception("Cannot sum Strings", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (Array         & x) const { throw Exception("Cannot sum Arrays", ErrorCodes::LOGICAL_ERROR); }
};


template <class TSortCursor>
bool SummingSortedBlockInputStream::mergeMaps(Row & row, TSortCursor & cursor)
{
    bool non_empty_map_present = false;

    /// merge nested maps
    for (const auto & map : maps_to_sum)
        if (mergeMap(map, row, cursor))
            non_empty_map_present = true;

    return non_empty_map_present;
}


template <class TSortCursor>
bool SummingSortedBlockInputStream::mergeMap(const MapDescription & desc, Row & row, TSortCursor & cursor)
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


template <class TSortCursor>
bool SummingSortedBlockInputStream::addRow(Row & row, TSortCursor & cursor)
{
    bool res = mergeMaps(row, cursor);    /// Is there at least one non-zero number or non-empty array

    for (size_t i = 0, size = column_numbers_to_sum.size(); i < size; ++i)
    {
        size_t j = column_numbers_to_sum[i];
        if (applyVisitor(FieldVisitorSum((*cursor->all_columns[j])[cursor->pos]), row[j]))
            res = true;
    }

    return res;
}

}
