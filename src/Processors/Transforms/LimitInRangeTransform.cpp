#include <Processors/Transforms/LimitInRangeTransform.h>

#include <algorithm>
#include <optional>

#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int UNEXPECTED_EXPRESSION;
}

Block LimitInRangeTransform::transformHeader(
    Block header,
    const String & from_filter_column_name,
    const String & to_filter_column_name,
    UInt64 limit_inrange_window,
    bool remove_filter_column)
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

            /** TODO:
            * Case: SELECT metric > 1 FROM my_first_table LIMIT INRANGE FROM metric > 1
            * Check remove_filter_column in PlannerExpressionAnalysis.cpp;
            * Case: SELECT * FROM my_first_table LIMIT INRANGE FROM metric > 1 TO metric > 1
            **/
        }
    };

    (void)limit_inrange_window;

    processFilterColumn(from_filter_column_name, "from_filter");
    processFilterColumn(to_filter_column_name, "to_filter");

    return header;
}

LimitInRangeTransform::LimitInRangeTransform(
    const Block & header_,
    String from_filter_column_name_,
    String to_filter_column_name_,
    UInt64 limit_inrange_window_,
    bool remove_filter_column_,
    bool on_totals_)
    : ISimpleTransform(
        header_,
        transformHeader(header_, from_filter_column_name_, to_filter_column_name_, limit_inrange_window_, remove_filter_column_),
        true)
    , from_filter_column_name(std::move(from_filter_column_name_))
    , to_filter_column_name(std::move(to_filter_column_name_))
    , limit_inrange_window(limit_inrange_window_)
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_)
{
    transformed_header = getInputPort().getHeader();

    if (!from_filter_column_name.empty())
        from_filter_column_position = transformed_header.getPositionByName(from_filter_column_name);

    if (!to_filter_column_name.empty())
        to_filter_column_position = transformed_header.getPositionByName(to_filter_column_name);

    auto & from_column = transformed_header.getByPosition(from_filter_column_position).column;
    auto & to_column = transformed_header.getByPosition(to_filter_column_position).column;

    if (from_column)
        constant_from_filter_description = ConstantFilterDescription(*from_column);
    if (to_column)
        constant_to_filter_description = ConstantFilterDescription(*from_column);
}

IProcessor::Status LimitInRangeTransform::prepare()
{
    /// TODO: May be some optimizations here. Check other transforms.

    if (!on_totals && !from_filter_column_name.empty() && constant_from_filter_description.always_false
        /// Optimization for `LIMIT INRANGE FROM column in (empty set)`.
        /// The result will not change after set was created, so we can skip this check.
        /// It is implemented in prepare() stop pipeline before reading from input port.
    )
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

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
            } /// if from index is not found in FROM expr TO expr, then do not throw. The to_index_found is optional here.
            else if (!to_filter_column_name.empty() && !to_index_found)
            {
                throw Exception(
                    ErrorCodes::UNEXPECTED_EXPRESSION, "The 'TO' condition was not satisfied: 'TO' index was not found in the data");
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

bool LimitInRangeTransform::processRemainingWindow(Chunk & chunk)
{
    if (remaining_window == 0)
        return false;

    cutChunkColumns(chunk, 0, std::min<UInt64>(remaining_window, chunk.getNumRows()));
    stopReading();
    return true;
}

/// Processes a chunk when a 'from_index' is found, considering the 'limit_inrange_window',
/// If a 'to_index' is also provided, it handles that case accordingly.
void LimitInRangeTransform::processChunkFromCaseWithWindow(Chunk & chunk, std::optional<size_t> from_index, std::optional<size_t> to_index)
{
    /// If 'from_index' by considering with 'limit_inrange_window', is located in the previous chunk.
    if (*from_index < limit_inrange_window)
    {
        if (!bufferized_chunk.empty())
        {
            size_t buffer_start = bufferized_chunk.getNumRows() - (limit_inrange_window - *from_index);
            size_t buffer_length = limit_inrange_window - *from_index;

            cutChunkColumns(bufferized_chunk, buffer_start, buffer_length);

            if (!to_index)
            {
                bufferized_chunk.append(chunk);
                chunk = std::move(bufferized_chunk);
            }
            else
            {
                processChunkToCaseWithWindow(chunk, 0, limit_inrange_window + *to_index + 1, true);
            }
        }
    }
    else /// In current chunk.
    {
        size_t start = *from_index - limit_inrange_window;
        if (!to_index)
            cutChunkColumns(chunk, start, chunk.getNumRows() - start);
        else
            processChunkToCaseWithWindow(chunk, start, *to_index + limit_inrange_window - start + 1);
    }
}

void LimitInRangeTransform::processChunkToCaseWithWindow(Chunk & chunk, size_t start, size_t length, bool use_bufferized)
{
    auto processBufferized = [&]()
    {
        if (use_bufferized)
        {
            bufferized_chunk.append(chunk);
            chunk = std::move(bufferized_chunk);
        }
    };

    if (length + start > chunk.getNumRows())
    {
        remaining_window = length + start - chunk.getNumRows();
        cutChunkColumns(chunk, start, chunk.getNumRows() - start);
        processBufferized();
        return;
    }

    cutChunkColumns(chunk, start, length);
    processBufferized();

    stopReading();
}

std::optional<size_t> findFirstOneIndex(const IColumn::Filter * filter, size_t start = 0)
{
    /// If all elements are zero or starting index is out-of-bound.
    if (!filter || memoryIsZero(filter->data(), 0, filter->size()) || start >= filter->size())
        return std::nullopt;

    /// Can be optimized.
    const auto * it = std::find(filter->begin() + start, filter->end(), 1);
    if (it == filter->end())
        return std::nullopt;

    return static_cast<size_t>(std::distance(filter->begin(), it));
}

void LimitInRangeTransform::transform(Chunk & chunk)
{
    if (!bufferized_chunk.empty())
        limit_inrange_window = std::min<UInt64>(limit_inrange_window, bufferized_chunk.getNumRows());

    if (!from_filter_column_name.empty() && !to_filter_column_name.empty())
        doFromAndToTransform(chunk);
    else if (!from_filter_column_name.empty())
        doFromTransform(chunk);
    else if (!to_filter_column_name.empty())
        doToTransform(chunk);

    removeFilterIfNeed(chunk);
}

std::optional<size_t> LimitInRangeTransform::findIndex(Chunk & chunk, size_t column_position, bool & index_found, size_t starting_pos = 0)
{
    auto filter_description = initializeColumn(chunk.getColumns(), column_position);
    std::optional<size_t> index = findFirstOneIndex(filter_description.data, starting_pos);

    if (index)
        index_found = true;

    return index;
}

void LimitInRangeTransform::handleFromCase(Chunk & chunk, std::optional<size_t> from_index)
{
    if (limit_inrange_window != 0)
        processChunkFromCaseWithWindow(chunk, from_index, std::nullopt);
    else
        cutChunkColumns(chunk, *from_index, chunk.getNumRows() - *from_index);
}

void LimitInRangeTransform::handleToCase(Chunk & chunk, std::optional<size_t> from_index, std::optional<size_t> to_index)
{
    if (limit_inrange_window != 0)
    {
        if (!from_index)
            processChunkToCaseWithWindow(chunk, 0, limit_inrange_window + *to_index + 1);
        else
            processChunkFromCaseWithWindow(chunk, from_index, to_index);
    }
    else
    {
        if (!from_index)
            cutChunkColumns(chunk, 0, *to_index + 1);
        else
            cutChunkColumns(chunk, *from_index, *to_index - *from_index + 1);
        stopReading();
    }
}

void LimitInRangeTransform::doFromTransform(Chunk & chunk)
{
    /// TODO: constant_filter_description

    /// If 'from_index' has already been found in previous chunks.
    if (from_index_found)
        return;

    /// Search it in current chunk/column.
    auto from_index = findIndex(chunk, from_filter_column_position, from_index_found);
    if (!from_index)
    {
        /// Bufferize every previous chunk.
        bufferized_chunk = std::move(chunk);
        return;
    }

    handleFromCase(chunk, from_index);
}

void LimitInRangeTransform::doToTransform(Chunk & chunk)
{
    /// If 'to index' is not found, return all chunks and then throw an exception in prepare.

    /// Processing the case when the 'to index', considering the window, fell into the next chunk.
    if (processRemainingWindow(chunk))
        return;

    auto to_index = findIndex(chunk, to_filter_column_position, to_index_found);
    if (!to_index)
        return;

    handleToCase(chunk, std::nullopt, to_index);
}

void LimitInRangeTransform::doFromAndToTransform(Chunk & chunk)
{
    if (processRemainingWindow(chunk))
        return;

    if (from_index_found)
    {
        /// Just a LIMIT INRANGE TO case.
        doToTransform(chunk);
        return;
    }

    /// Search for 'from index' and 'to index' in current chunk.
    auto from_index = findIndex(chunk, from_filter_column_position, from_index_found);
    if (!from_index)
    {
        bufferized_chunk = std::move(chunk);
        return;
    }

    auto to_index = findIndex(chunk, to_filter_column_position, to_index_found, *from_index + 1);
    if (!to_index)
    {
        handleFromCase(chunk, from_index);
        return;
    }

    handleToCase(chunk, from_index, to_index);
    /// Return nothing when none of the indices are found.
}

}
