#include <algorithm>
#include <Processors/Transforms/LimitInRangeTransform.h>

#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>
#include "Columns/FilterDescription.h"

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
extern const int UNEXPECTED_EXPRESSION;
}

Block LimitInRangeTransform::transformHeader(
    Block header, const String & from_filter_column_name, const String & to_filter_column_name, bool remove_filter_column)
{
    auto processFilterColumn = [&](const String & filter_column_name, const char * filter_name)
    {
        if (!filter_column_name.empty())
        {
            auto filter_type = header.getByName(filter_column_name).type;
            if (!filter_type->onlyNull() && !isUInt8(removeNullable(removeLowCardinality(filter_type))))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                    "Illegal type {} of column '{}' for {}. Must be UInt8 or Nullable(UInt8).",
                    filter_type->getName(),
                    filter_column_name,
                    filter_name);
            }

            if (remove_filter_column)
                header.erase(filter_column_name);
            // else
            // replaceFilterToConstant(header, filter_column_name);
        }
    };

    processFilterColumn(from_filter_column_name, "from_filter");
    processFilterColumn(to_filter_column_name, "to_filter");

    return header;
}


LimitInRangeTransform::LimitInRangeTransform(
    const Block & header_,
    String from_filter_column_name_,
    String to_filter_column_name_,
    bool remove_filter_column_,
    bool on_totals_,
    std::shared_ptr<std::atomic<size_t>> rows_filtered_)
    : ISimpleTransform(header_, transformHeader(header_, from_filter_column_name_, to_filter_column_name_, remove_filter_column_), true)
    , from_filter_column_name(std::move(from_filter_column_name_))
    , to_filter_column_name(std::move(to_filter_column_name_))
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_)
    , rows_filtered(rows_filtered_)
{
    transformed_header = getInputPort().getHeader();

    if (!from_filter_column_name.empty())
        from_filter_column_position = transformed_header.getPositionByName(from_filter_column_name);

    if (!to_filter_column_name.empty())
        to_filter_column_position = transformed_header.getPositionByName(to_filter_column_name);

    (void)on_totals;
}

IProcessor::Status LimitInRangeTransform::prepare()
{
    // TODO: may be some optimizations here. check other transforms.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.pushData(std::move(output_data));
        has_output = false;

        if (!no_more_data_needed)
            return Status::PortFull;
    }

    /// Stop if don't need more data.
    if (no_more_data_needed)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            if (!from_filter_column_name.empty() && !to_filter_column_name.empty() && !from_index_found)
            {
            } // if from index is not found in FROM expr TO expr, then do not throw. The to_index_found is optional here.
            else if (!to_filter_column_name.empty() && !to_index_found)
            {
                throw Exception(
                    ErrorCodes::UNEXPECTED_EXPRESSION,
                    "The 'TO' condition was not satisfied: 'TO' index was not found in the data");
            }
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        input_data = input.pullData(set_input_not_needed_after_read);
        has_input = true;

        if (input_data.exception)
            /// No more data needed. Exception will be thrown (or swallowed) later.
            input.setNotNeeded();
    }

    /// Now transform.
    return Status::Ready;
}

void LimitInRangeTransform::removeFilterIfNeed(Chunk & chunk) const
{
    if (chunk && remove_filter_column)
    {
        if (!from_filter_column_name.empty())
            chunk.erase(from_filter_column_position);

        if (!to_filter_column_name.empty())
        {
            size_t adjusted_position = !from_filter_column_name.empty() ? to_filter_column_position - 1 : to_filter_column_position;
            chunk.erase(adjusted_position);
        }
    }
}

FilterDescription initializeColumn(const Columns & columns, size_t filter_column_position)
{
    const IColumn & filter_column = *columns[filter_column_position];
    return FilterDescription(filter_column);
}

void cutChunkColumns(Chunk & chunk, size_t start, size_t length)
{
    auto columns = chunk.detachColumns();

    for (auto & col : columns)
        col = col->cut(start, length);

    chunk.setColumns(std::move(columns), length);
}

std::optional<size_t> findFirstMatchingIndex(const IColumn::Filter * filter, size_t start = 0)
{
    /// if all elements are zero or starting index is out-of-bound.
    if (!filter || memoryIsZero(filter->data(), 0, filter->size()) || start >= filter->size())
        return std::nullopt;

    const auto * it = std::find(filter->begin() + start, filter->end(), 1);
    if (it == filter->end())
        return std::nullopt;

    /// the index of the found element.
    return static_cast<size_t>(std::distance(filter->begin(), it));
}

void LimitInRangeTransform::transform(Chunk & chunk)
{
    auto chunk_rows_before = chunk.getNumRows();
    if (!from_filter_column_name.empty() && !to_filter_column_name.empty())
        doFromAndToTransform(chunk);
    else if (!from_filter_column_name.empty())
        doFromTransform(chunk);
    else if (!to_filter_column_name.empty())
        doToTransform(chunk);
    if (rows_filtered)
        *rows_filtered += chunk_rows_before - chunk.getNumRows();
}

void LimitInRangeTransform::doFromTransform(Chunk & chunk)
{
    // TODO: constant_filter_description

    if (from_index_found) /// in prev chunks
    {
        removeFilterIfNeed(chunk);
        return;
    }

    auto from_filter_description = initializeColumn(chunk.getColumns(), from_filter_column_position);

    std::optional<size_t> index = findFirstMatchingIndex(from_filter_description.data);
    if (!index)
    {
        chunk.clear();
        return;
    }
    from_index_found = true;

    cutChunkColumns(chunk, *index, chunk.getNumRows() - *index);
    removeFilterIfNeed(chunk);
}

void LimitInRangeTransform::doToTransform(Chunk & chunk)
{
    /// If 'to index' is not found, return all chunks and then throw an exception.

    auto to_filter_description = initializeColumn(chunk.getColumns(), to_filter_column_position);

    std::optional<size_t> index = findFirstMatchingIndex(to_filter_description.data);
    if (!index)
    {
        removeFilterIfNeed(chunk);
        return;
    }
    to_index_found = true;

    cutChunkColumns(chunk, 0, *index + 1);
    removeFilterIfNeed(chunk);

    stopReading();
}

void LimitInRangeTransform::doFromAndToTransform(Chunk & chunk)
{
    auto from_filter_description = initializeColumn(chunk.getColumns(), from_filter_column_position);
    auto to_filter_description = initializeColumn(chunk.getColumns(), to_filter_column_position);

    if (from_index_found)
    {
        std::optional<size_t> to_index = findFirstMatchingIndex(to_filter_description.data);
        if (to_index)
        {
            to_index_found = true;

            cutChunkColumns(chunk, 0, *to_index + 1);
            stopReading();
        }
        removeFilterIfNeed(chunk);
        return;
    }

    std::optional<size_t> from_index = findFirstMatchingIndex(from_filter_description.data);
    if (from_index)
    {
        from_index_found = true;

        std::optional<size_t> to_index = findFirstMatchingIndex(to_filter_description.data, *from_index + 1);
        if (to_index)
        {
            cutChunkColumns(chunk, *from_index, *to_index - *from_index + 1);
            stopReading();
        }
        else
        {
            cutChunkColumns(chunk, *from_index, chunk.getNumRows() - *from_index);
        }

        removeFilterIfNeed(chunk);
        return;
    }

    chunk.clear();
    /// return nothing if none of the indices are found.
}


}
