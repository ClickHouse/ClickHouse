#include "SelectiveColumnReader.h"

#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Common/assert_cast.h>

namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
}

namespace DB
{
static void insertManyToFilter(PaddedPODArray<bool> & filter, bool value, size_t count)
{
    if (!count)
        return;
    filter.resize(filter.size() + count);
    std::fill(filter.end() - count, filter.end(), value);
}

// must read full
void ListColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    ListState state = getListState(column);
    auto & offsets = state.offsets;
    const bool has_filter = row_set.has_value();
    size_t valid_count = has_filter ? row_set->count() : rows_to_read;
    auto target_size = offsets.size() + valid_count;
    offsets.reserve(target_size);

    size_t rows_read = 0;
    // read data from multiple pages
    bool finished = true;
    size_t count = 0;
    size_t last_row_level_idx = 0;
    auto appendRecord = [&](size_t size)
    {
        if (!finished) [[unlikely]]
        {
            offsets.back() += size;
            finished = true;
        }
        else
            offsets.push_back(offsets.back() + size);
        last_row_level_idx = count;
    };
    while (rows_read < rows_to_read || !finished)
    {
        int array_size = 0;
        size_t tail_empty_rows = 0;
        //        size_t old_offsets_size = offsets.size();
        size_t old_max_offset = offsets.back();
        const auto & def_levels = getDefinitionLevels();
        bool has_def_level = !def_levels.empty();
        const auto & rep_levels = getRepetitionLevels();
        size_t min_count = minimumAvailableLevels();
        size_t start = levelsOffset();
        size_t levels_size = rep_levels.size();
        if (levels_size == 0)
            break;

        PaddedPODArray<bool> child_filter;
        if (has_filter)
            child_filter.reserve(rows_to_read);
        // read from current page
        while (true)
        {
            size_t idx = start + count;
            // levels out of range or rows out of range
            if (count >= min_count || idx >= levels_size || (rows_read >= rows_to_read && finished))
                break;
            int16_t rl = rep_levels[idx];
            int16_t dl = has_def_level ? def_levels[idx] : 0;
            if (rl <= rep_level)
            {
                if (last_row_level_idx < count)
                {
                    rows_read += finished;
                    if (has_filter)
                    {
                        auto valid = row_set->get(rows_read - (finished || rows_read));
                        if (valid)
                        {
                            insertManyToFilter(child_filter, true, array_size);
                            appendRecord(array_size);
                        }
                        else
                        {
                            insertManyToFilter(child_filter, false, array_size);
                            last_row_level_idx = count;
                            finished = true;
                        }
                    }
                    else
                        appendRecord(array_size);
                    tail_empty_rows = array_size > 0 ? 0 : tail_empty_rows + 1;
                    array_size = 0;
                    if (rows_read >= rows_to_read && finished)
                        break;
                }

                if (has_def_level && dl < def_level)
                {
                    // skip empty record in parent level
                    if (rl != rep_level && (!parent || (parent && dl <= parent_dl)))
                    {
                        count++;
                        last_row_level_idx = count;
                        tail_empty_rows ++;
                        continue;
                    }
                    // value is null
                    if (!has_filter || row_set->get(rows_read))
                    {
                        if (state.null_map)
                        {
                            state.null_map->data()[offsets.size()] = 1;
                        }
                        appendRecord(0);
                    }
                    else
                    {
                        last_row_level_idx = count;
                    }
                    tail_empty_rows++;
                    chassert(array_size == 0);
                    count++;
                    continue;
                }
                else
                {
                    array_size += (dl > def_level);
                }
            }
            else
            {
                array_size += (rl == rep_level + 1);
            }
            count++;
        }

        // read tail record
        if ((rows_read < rows_to_read || !finished) && last_row_level_idx < count)
        {
            if (has_filter)
            {
                auto valid = row_set->get(rows_read - (!finished && rows_read));
                rows_read += finished;
                if (valid)
                {
                    insertManyToFilter(child_filter, true, array_size);
                    appendRecord(array_size);
                }
                else
                {
                    insertManyToFilter(child_filter, false, array_size);
                    finished = true;
                    last_row_level_idx = count;
                }
            }
            else
            {
                rows_read += finished;
                appendRecord(array_size);
            }
            tail_empty_rows = array_size > 0 ? 0 : tail_empty_rows + 1;
        }
        OptionalRowSet filter;
        if (has_filter)
            filter = RowSet(child_filter);
        auto need_read = has_filter ? filter->totalRows() : offsets.back() - old_max_offset;
        for (size_t i = 0; i < children.size(); i++)
        {
            auto & child = children[i];
            child->read(state.columns.at(i), filter, need_read);
            chassert(state.columns.at(i)->size() == offsets.back());
            if (child->isLeafReader())
            {
                child->advance(last_row_level_idx - need_read, false);
            }
            if (filter)
                filter->setOffset(0);
        }

        // skip tail empty records, child reader never read tail empty rows
        if (tail_empty_rows)
            for (auto & child : children)
            {
                if (!child->isLeafReader())
                {
                    child->advance(tail_empty_rows, true);
                }
            }

        // check last row finished
        const auto & next_rep_levels = getRepetitionLevels();
        if (last_row_level_idx < min_count || next_rep_levels.empty() || next_rep_levels[levelsOffset()] <= rep_level) [[likely]]
            finished = true;
        else
            finished = false;
        count = 0;
        last_row_level_idx = 0;
    }
}

void ListColumnReader::computeRowSet(std::optional<RowSet> &, size_t)
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}
MutableColumnPtr ListColumnReader::createColumn()
{
    return ColumnArray::create(children.front()->createColumn(), ColumnArray::ColumnOffsets::create());
}
void ListColumnReader::skip(size_t rows)
{
    // may be can skip generate columns.
    auto tmp = createColumn();
    OptionalRowSet set = RowSet(rows);
    set->setAllFalse();
    read(tmp, set, rows);
}

size_t ListColumnReader::availableRows() const
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}
DataTypePtr ListColumnReader::getResultType()
{
    return std::make_shared<DataTypeArray>(children.front()->getResultType());
}
const PaddedPODArray<Int16> & ListColumnReader::getDefinitionLevels()
{
    return children.front()->getDefinitionLevels();
}
const PaddedPODArray<Int16> & ListColumnReader::getRepetitionLevels()
{
    return children.front()->getRepetitionLevels();
}
size_t ListColumnReader::levelsOffset() const
{
    return children.front()->levelsOffset();
}
ListColumnReader::ListState ListColumnReader::getListState(MutableColumnPtr & column)
{
    // support list inside nullable ?
    NullMap * null_map = nullptr;
    MutableColumnPtr & nested_column = column;
    if (column->isNullable())
    {
        ColumnNullable * null_column = static_cast<ColumnNullable *>(column.get());
        null_map = &null_column->getNullMapData();
        nested_column = null_column->getNestedColumnPtr()->assumeMutable();
    }
    if (!checkColumn<ColumnArray>(*nested_column))
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "column type should be array, but is {}", nested_column->getName());
    }
    ColumnArray * array_column = static_cast<ColumnArray *>(nested_column.get());

    auto & offsets = array_column->getOffsets();
    MutableColumns data_columns;
    data_columns.push_back(array_column->getDataPtr()->assumeMutable());
    return ListState{.null_map = null_map, .offsets = offsets, .columns = std::move(data_columns)};
}

MutableColumnPtr MapColumnReader::createColumn()
{
    MutableColumns columns;
    for (auto & child : children)
    {
        columns.push_back(child->createColumn());
    }
    MutableColumnPtr tuple = ColumnTuple::create(std::move(columns));
    MutableColumnPtr array = ColumnArray::create(std::move(tuple));
    return ColumnMap::create(std::move(array));
}

DataTypePtr MapColumnReader::getResultType()
{
    DataTypes types = {children.front()->getResultType(), children.back()->getResultType()};
    return std::make_shared<DataTypeMap>(std::move(types));
}
void MapColumnReader::advance(size_t rows, bool force)
{
    children.front()->advance(rows, force);
    if (children.back()->isLeafReader() || force)
        children.back()->advance(rows, force);
}
size_t MapColumnReader::minimumAvailableLevels()
{
    return std::min(children.front()->minimumAvailableLevels(), children.back()->minimumAvailableLevels());
}
ListColumnReader::ListState MapColumnReader::getListState(MutableColumnPtr & column)
{
    // support map inside nullable, how parquet serialize null in map type?
    NullMap * null_map = nullptr;
    MutableColumnPtr & column_inside_nullable = column;
    if (column->isNullable())
    {
        ColumnNullable * null_column = static_cast<ColumnNullable *>(column.get());
        null_map = &null_column->getNullMapData();
        column_inside_nullable = null_column->getNestedColumnPtr()->assumeMutable();
    }
    if (!checkColumn<ColumnMap>(*column_inside_nullable))
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "column type should be map, but is {}", column_inside_nullable->getName());
    }
    ColumnMap * map_column = static_cast<ColumnMap *>(column_inside_nullable.get());
    ColumnArray * array_in_map_column = &map_column->getNestedColumn();
    auto data_column = array_in_map_column->getDataPtr();
    const ColumnTuple & tuple_col = checkAndGetColumn<ColumnTuple>(*data_column);
    if (tuple_col.getColumns().size() != 2)
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "map column should have 2 columns, but has {}", tuple_col.getColumns().size());
    }
    MutableColumns data_columns;
    for (const auto & col : tuple_col.getColumns())
    {
        data_columns.push_back(col->assumeMutable());
    }
    auto & offsets = array_in_map_column->getOffsets();
    return {null_map, offsets, std::move(data_columns)};
}

void StructColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    /// TODO support tuple inside nullable
    checkColumn<ColumnTuple>(*column);
    ColumnTuple * tuple_column = static_cast<ColumnTuple *>(column.get());
    const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(structType.get());
    auto names = tuple_type->getElementNames();
    auto original_offset = row_set ? row_set->getOffset() : 0;
    for (size_t i = 0; i < names.size(); i++)
    {
        auto nested_column = tuple_column->getColumn(i).assumeMutable();
        auto & nested_reader = children.at(names.at(i));
        nested_reader->read(nested_column, row_set, rows_to_read);
        if (row_set)
            row_set->setOffset(original_offset);
    }
}

void StructColumnReader::computeRowSet(std::optional<RowSet> &, size_t)
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}

MutableColumnPtr StructColumnReader::createColumn()
{
    MutableColumns columns;
    const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(structType.get());
    for (const auto & name : tuple_type->getElementNames())
    {
        auto & nested_reader = children.at(name);
        columns.push_back(nested_reader->createColumn());
    }
    return ColumnTuple::create(std::move(columns));
}

const PaddedPODArray<Int16> & StructColumnReader::getDefinitionLevels()
{
    return children.begin()->second->getDefinitionLevels();
}
const PaddedPODArray<Int16> & StructColumnReader::getRepetitionLevels()
{
    return children.begin()->second->getRepetitionLevels();
}

void StructColumnReader::skip(size_t rows)
{
    for (auto & child : children)
    {
        child.second->skip(rows);
    }
}

size_t StructColumnReader::availableRows() const
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}

size_t StructColumnReader::levelsOffset() const
{
    return children.begin()->second->levelsOffset();
}

DataTypePtr StructColumnReader::getResultType()
{
    return structType;
}
void StructColumnReader::advance(size_t rows, bool force)
{
    for (auto & child : children)
    {
        if (child.second->isLeafReader() || force)
            child.second->advance(rows, force);
    }
}
size_t StructColumnReader::minimumAvailableLevels()
{
    size_t min_levels = std::numeric_limits<size_t>::max();
    for (auto & child : children)
    {
        min_levels = std::min(min_levels, child.second->minimumAvailableLevels());
    }
    return min_levels;
}
}
