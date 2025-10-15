#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/FractionalLimitTransform.h>

#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <base/types.h>
#include "iostream"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FractionalLimitTransform::FractionalLimitTransform(
    SharedHeader header_, 
    BFloat16 limit_fraction_, 
    BFloat16 offset_fraction_, 
    UInt64 offset_,
    size_t num_streams,
    bool with_ties_,
    SortDescription description_
) : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_)),
    limit_fraction(limit_fraction_),
    offset_fraction(offset_fraction_),
    offset(offset_),
    with_ties(with_ties_),
    description(std::move(description_))
{
    if (num_streams != 1 && with_ties)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot use FractionalLimitTransform with multiple ports and ties");

    ports_data.resize(num_streams);

    size_t cur_stream = 0;
    for (auto & input : inputs)
    {
        ports_data[cur_stream].input_port = &input;
        ++cur_stream;
    }

    cur_stream = 0;
    for (auto & output : outputs)
    {
        ports_data[cur_stream].output_port = &output;
        ++cur_stream;
    }

    for (const auto & desc : description)
        sort_column_positions.push_back(header_->getPositionByName(desc.column_name));
}

Chunk FractionalLimitTransform::makeChunkWithPreviousRow(const Chunk & chunk, UInt64 row) const
{
    assert(row < chunk.getNumRows());
    ColumnRawPtrs current_columns = extractSortColumns(chunk.getColumns());
    MutableColumns last_row_sort_columns;
    for (size_t i = 0; i < current_columns.size(); ++i)
    {
        last_row_sort_columns.emplace_back(current_columns[i]->cloneEmpty());
        last_row_sort_columns[i]->insertFrom(*current_columns[i], row);
    }
    return Chunk(std::move(last_row_sort_columns), 1);
}


IProcessor::Status FractionalLimitTransform::prepare(
        const PortNumbers & updated_input_ports,
        const PortNumbers & updated_output_ports)
{
    bool has_full_port = false;
    bool has_finished_output = false;

    auto process_pair = [&](size_t pos)
    {
        auto status = preparePair(ports_data[pos]);

        switch (status)
        {
            case IProcessor::Status::Finished:
            {
                if (ports_data[pos].output_port->isFinished())
                {
                    has_finished_output = true;
                }
                else if (ports_data[pos].input_port->isFinished())
                {
                    ++num_finished_input_ports;
                }
                return;
            }
            case IProcessor::Status::PortFull:
                has_full_port = true;
                return;
            case IProcessor::Status::NeedData:
                return;
            default:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Unexpected status for FractionalLimitTransform::preparePair : {}", IProcessor::statusToName(status));
        }
    };

    for (auto pos : updated_input_ports)
        process_pair(pos);

    for (auto pos : updated_output_ports)
        process_pair(pos);

    /// We finished all output ports.
    if (has_finished_output)
    {
        for (auto & chunk : chunks_cache)
        {
            chunk.clear();
        }
        return Status::Finished;
    }

    if (has_full_port)
        return Status::PortFull;

    if (num_finished_input_ports == ports_data.size())
    {
        limit = static_cast<UInt64>(std::ceil(rows_cnt * limit_fraction));
        if (!offset)
            offset = static_cast<UInt64>(std::ceil(rows_cnt * offset_fraction));
        else 
            offset = 0; // Already skipped.

        // Caching done, call one more time to start producing output.
        return prepare(updated_input_ports, updated_output_ports);
    }

    return Status::NeedData;
}

FractionalLimitTransform::Status  FractionalLimitTransform::prepare()
{
    if (ports_data.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "prepare without arguments is not supported for multi-port LimitTransform");

    return prepare({0}, {0});
}

FractionalLimitTransform::Status FractionalLimitTransform::preparePair(PortsData & data)
{
    auto & input = *data.input_port;

    if (num_finished_input_ports == ports_data.size())
    {
        auto & output = *data.output_port;

        if (output.isFinished())
        {
            return Status::Finished;
        }

        if (!output.canPush())
        {
            return Status::PortFull;
        }

        /// Check if we are done with pushing.
        bool is_limit_reached = rows_read >= offset + limit && !previous_row_chunk;
        if (is_limit_reached || chunks_cache.empty())
        {
            output.finish();
            return Status::Finished;
        }

        UInt64 rows;
        do
        {
            rows = chunks_cache[0].getNumRows();
            rows_read += rows;
            if (rows_read <= offset)
            {
                chunks_cache[0].clear();
                chunks_cache.pop_front();
            }
        } while (rows_read <= offset);

        if (rows <= std::numeric_limits<UInt64>::max() - offset && rows_read >= offset + rows && rows_read <= offset + limit)
        {
            /// Return the whole chunk.

            /// Save the last row of current chunk to check if next block begins with the same row (for WITH TIES).
            if (with_ties && rows_read == offset + limit)
                previous_row_chunk = makeChunkWithPreviousRow(chunks_cache[0], rows - 1);
        }
        else
            /// This function may be heavy to execute in prepare. But it happens no more than twice, and makes code simpler.
            splitChunk(chunks_cache[0]);

        output.push(std::move(chunks_cache[0]));
        chunks_cache.pop_front();
        return Status::PortFull;
    }

    /// Check can input.
    if (input.isFinished())
    {
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    data.current_chunk = input.pull(true);

    auto rows = data.current_chunk.getNumRows();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(rows);

    /// Process block.
    rows_cnt += rows;

    if (rows_cnt <= offset)
    {
        data.current_chunk.clear();

        if (input.isFinished())
        {
            return Status::Finished;
        }

        /// Now, we pulled from input, and it must be empty.
        input.setNeeded();
        return Status::NeedData;
    }

    chunks_cache.push_back(std::move(data.current_chunk));

    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    return Status::NeedData;
}


void FractionalLimitTransform::splitChunk(Chunk & current_chunk)
{
    auto current_chunk_sort_columns = extractSortColumns(current_chunk.getColumns());
    UInt64 num_rows = current_chunk.getNumRows();
    UInt64 num_columns = current_chunk.getNumColumns();

    bool limit_is_unreachable = (limit > std::numeric_limits<UInt64>::max() - offset);

    if (previous_row_chunk && !limit_is_unreachable && rows_read >= offset + limit)
    {
        /// Scan until the first row, which is not equal to previous_row_chunk (for WITH TIES)
        UInt64 current_row_num = 0;
        for (; current_row_num < num_rows; ++current_row_num)
        {
            if (!sortColumnsEqualAt(current_chunk_sort_columns, current_row_num))
                break;
        }

        auto columns = current_chunk.detachColumns();

        if (current_row_num < num_rows)
        {
            previous_row_chunk = {};
            for (UInt64 i = 0; i < num_columns; ++i)
                columns[i] = columns[i]->cut(0, current_row_num);
        }

        current_chunk.setColumns(std::move(columns), current_row_num);
        return;
    }

    /// return a piece of the block
    UInt64 start = 0;

    /// ------------[....(...).]
    /// <----------------------> rows_read
    ///             <----------> num_rows
    /// <---------------> offset
    ///             <---> start

    assert(offset < rows_read);

    if (offset + num_rows > rows_read)
        start = offset + num_rows - rows_read;

    /// ------------[....(...).]
    /// <----------------------> rows_read
    ///             <----------> num_rows
    /// <---------------> offset
    ///                  <---> limit
    ///                  <---> length
    ///             <---> start

    /// Or:

    /// -----------------(------[....)....]
    /// <---------------------------------> rows_read
    ///                         <---------> num_rows
    /// <---------------> offset
    ///                  <-----------> limit
    ///                         <----> length
    ///                         0 = start

    UInt64 length = num_rows - start;

    if (!limit_is_unreachable && offset + limit < rows_read)
    {
        if (offset + limit < rows_read - num_rows)
            length = 0;
        else
            length = offset + limit - (rows_read - num_rows) - start;
    }

    /// Check if other rows in current block equals to last one in limit
    /// when rows read >= offset + limit.
    if (with_ties && offset + limit <= rows_read && length)
    {
        UInt64 current_row_num = start + length;
        previous_row_chunk = makeChunkWithPreviousRow(current_chunk, current_row_num - 1);

        for (; current_row_num < num_rows; ++current_row_num)
        {
            if (!sortColumnsEqualAt(current_chunk_sort_columns, current_row_num))
            {
                previous_row_chunk = {};
                break;
            }
        }

        length = current_row_num - start;
    }

    if (length == num_rows)
        return;

    auto columns = current_chunk.detachColumns();

    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);

    current_chunk.setColumns(std::move(columns), length);
}

ColumnRawPtrs FractionalLimitTransform::extractSortColumns(const Columns & columns) const
{
    ColumnRawPtrs res;
    res.reserve(description.size());
    for (size_t pos : sort_column_positions)
        res.push_back(columns[pos].get());

    return res;
}

bool FractionalLimitTransform::sortColumnsEqualAt(const ColumnRawPtrs & current_chunk_sort_columns, UInt64 current_chunk_row_num) const
{
    assert(current_chunk_sort_columns.size() == previous_row_chunk.getNumColumns());
    size_t size = current_chunk_sort_columns.size();
    const auto & previous_row_sort_columns = previous_row_chunk.getColumns();
    for (size_t i = 0; i < size; ++i)
        if (0 != current_chunk_sort_columns[i]->compareAt(current_chunk_row_num, 0, *previous_row_sort_columns[i], 1))
            return false;
    return true;
}

}

