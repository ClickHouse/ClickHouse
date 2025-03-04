#include "SelectiveColumnReader.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Common/assert_cast.h>

namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
}

namespace DB
{

void OptionalColumnReader::nextBatchNullMapIfNeeded(size_t rows_to_read)
{
    if (!cur_null_map.empty())
        return;
    cur_null_map.resize_fill(rows_to_read, 0);
    cur_null_count = 0;
    const auto & def_levels = child->getDefinitionLevels();
    if (def_levels.empty())
        return;
    size_t start = child->levelsOffset();
    int16_t max_def_level = maxDefinitionLevel();
    int16_t max_rep_level = maxRepetitionLevel();
    size_t read = 0;
    size_t count = 0;
    while (read < rows_to_read)
    {
        auto idx = start + count;
        auto dl = def_levels[idx];
        if (dl < max_def_level - 1)
        {
            // for struct reader, when struct is null, child field would be null.
            if (max_rep_level == parent_rl && has_null && dl >= parent_dl)
            {
                cur_null_map[read] = 1;
                cur_null_count++;
            }
            else
            {
                count++;
                continue;
            }
        }
        if (def_levels[idx] == max_def_level - 1)
        {
            if (has_null)
            {
                cur_null_map[read] = 1;
                cur_null_count++;
            }
            else
            {
                count++;
                continue;
            }
        }
        count++;
        read++;
    }
}

void OptionalColumnReader::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    applyLazySkip();
    nextBatchNullMapIfNeeded(rows_to_read);
    if (cur_null_count)
        child->computeRowSetSpace(row_set, cur_null_map, cur_null_count, rows_to_read);
    else
        child->computeRowSet(row_set, rows_to_read);
}

void OptionalColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    applyLazySkip();
    size_t rows_read = 0;
    auto * nullable_column = static_cast<ColumnNullable *>(column.get());
    auto nested_column = nullable_column->getNestedColumnPtr()->assumeMutable();
    auto & null_data = nullable_column->getNullMapData();
    while (rows_read < rows_to_read)
    {
        child->readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, child->availableRows());
        if (!rows_can_read) break;
        auto original_filter_offset = row_set? row_set->getOffset() : 0;
        nextBatchNullMapIfNeeded(rows_can_read);
        if (row_set)
        {
            const auto & sets = row_set.value();
            for (size_t i = 0; i < rows_can_read; i++)
            {
                if (sets.get(i))
                {
                    null_data.push_back(cur_null_map[i]);
                }
            }
        }
        else
            null_data.insert(cur_null_map.begin(), cur_null_map.end());
        if (cur_null_count)
        {
            child->readSpace(nested_column, row_set, cur_null_map, cur_null_count, rows_can_read);
        }
        else
        {
            child->read(nested_column, row_set, rows_can_read);
        }
        cleanNullMap();
        // reset filter offset, child reader may modify filter offset
        if (row_set)
        {
            row_set->setOffset(original_filter_offset);
            row_set->addOffset(rows_can_read);
        }
        rows_read += rows_can_read;
        chassert(nested_column->size() == null_data.size());
    }
}

size_t OptionalColumnReader::skipValuesInCurrentPage(size_t rows)
{
    if (!rows)
        return 0;
    if (!child->state.offsets.remain_rows || !child->state.page)
        return rows;
    auto skipped = std::min(rows, child->state.offsets.remain_rows);
    if (cur_null_map.empty())
        nextBatchNullMapIfNeeded(skipped);
    else
        chassert(rows == cur_null_map.size());
    child->skipNulls(cur_null_count);
    child->skip(skipped - cur_null_count);
    cleanNullMap();
    return rows - skipped;
}

MutableColumnPtr OptionalColumnReader::createColumn()
{
    return ColumnNullable::create(child->createColumn(), ColumnUInt8::create());
}

DataTypePtr OptionalColumnReader::getResultType()
{
    return std::make_shared<DataTypeNullable>(child->getResultType());
}

void OptionalColumnReader::applyLazySkip()
{
    child->initPageReaderIfNeed();
    skipPageIfNeed();
    while (state.lazy_skip_rows)
    {
        child->readAndDecodePage();
        auto remain_skipped_rows = skipValuesInCurrentPage(state.lazy_skip_rows);
        if (remain_skipped_rows == state.lazy_skip_rows)
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "skip values failed. need skip {} rows, but child reader is exhausted", state.lazy_skip_rows);
        }
        state.lazy_skip_rows = remain_skipped_rows;
    }
}
void OptionalColumnReader::skipPageIfNeed()
{
    child->initPageReaderIfNeed();
    child->state.lazy_skip_rows = state.lazy_skip_rows;
    child->skipPageIfNeed();
    state.lazy_skip_rows = child->state.lazy_skip_rows;
    child->state.lazy_skip_rows = 0;
}
size_t OptionalColumnReader::availableRows() const
{
    return child->availableRows() - state.lazy_skip_rows;
}
size_t OptionalColumnReader::levelsOffset() const
{
    return child->levelsOffset();
}

const PaddedPODArray<Int16> & OptionalColumnReader::getDefinitionLevels()
{
    return child->getDefinitionLevels();
}
const PaddedPODArray<Int16> & OptionalColumnReader::getRepetitionLevels()
{
    return child->getRepetitionLevels();
}
}
