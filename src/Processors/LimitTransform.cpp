#include <Processors/LimitTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LimitTransform::LimitTransform(
    const Block & header_, UInt64 limit_, UInt64 offset_, size_t num_streams,
    bool always_read_till_end_, bool with_ties_, bool is_negative_,
    SortDescription description_)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , limit(limit_), offset(offset_)
    , always_read_till_end(always_read_till_end_), with_ties(with_ties_)
    , description(std::move(description_)), is_negative(is_negative_)
{
    limit_is_unreachable = limit > std::numeric_limits<UInt64>::max() - offset;
    if (is_negative)
        rows_to_keep = limit_is_unreachable ? offset : limit + offset;

    if (num_streams != 1 && with_ties)
        throw Exception("Cannot use LimitTransform with multiple ports and ties", ErrorCodes::LOGICAL_ERROR);

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
        sort_column_positions.push_back(header_.getPositionByName(desc.column_name));
}

Chunk LimitTransform::makeChunkWithPreviousRow(const Chunk & chunk, UInt64 row) const
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


IProcessor::Status LimitTransform::prepare(
        const PortNumbers & updated_input_ports,
        const PortNumbers & updated_output_ports)
{
    bool has_full_port = false;

    auto process_pair = [&](size_t pos)
    {
        //auto status = is_negative ? preparePairNegative(ports_data[pos]) : preparePair(ports_data[pos]);
        auto status = preparePair(ports_data[pos]);

        switch (status)
        {
            case IProcessor::Status::Finished:
            {
                if (!ports_data[pos].is_finished)
                {
                    ports_data[pos].is_finished = true;
                    ++num_finished_port_pairs;
                }

                return;
            }
            case IProcessor::Status::PortFull:
            {
                has_full_port = true;
                return;
            }
            case IProcessor::Status::NeedData:
                return;
            default:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Unexpected status for LimitTransform::preparePair : {}", IProcessor::statusToName(status));
        }
    };

    for (auto pos : updated_input_ports)
        process_pair(pos);

    for (auto pos : updated_output_ports)
        process_pair(pos);

    /// All ports are finished. It may happen even before we reached the limit (has less data then limit).
    if (num_finished_port_pairs == ports_data.size())
        return Status::Finished;

    /// If we reached limit for some port, then close others. Otherwise some sources may infinitely read data.
    /// Example: SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT 1
    if (!is_negative && (!limit_is_unreachable && rows_read >= offset + limit)
        && !previous_row_chunk && !always_read_till_end)
    {
        for (auto & input : inputs)
            input.close();

        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    if (has_full_port)
        return Status::PortFull;

    return Status::NeedData;
}

LimitTransform::Status LimitTransform::prepare()
{
    if (ports_data.size() != 1)
        throw Exception("prepare without arguments is not supported for multi-port LimitTransform",
                        ErrorCodes::LOGICAL_ERROR);

    return prepare({0}, {0});
}

LimitTransform::Status LimitTransform::preparePairNegative(PortsData & data)
{
    auto & output = *data.output_port;
    auto & input = *data.input_port;

    /// Check can output.
    bool output_finished = false;
    if (output.isFinished())
    {
        output_finished = true;
        if (!always_read_till_end)
        {
            input.close();
            return Status::Finished;
        }
    }

    if (!output_finished && !output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// When limit is unreachable, need to return all rows before negative offset, so check here
    if (limit_is_unreachable && PopWithoutCut())
    {
        output.push(queuePop());
        return Status::PortFull;
    }

    /// Check can input
    bool input_finished = false;
    if (input.isFinished())
    {
        input_finished = true;
        if (rows_in_queue <= offset)
        {
            queue.clear();
            output.finish();
            return Status::Finished;
        }
    }

    /// Input unfinished, pull chunk from input and push it into queue
    if (!input_finished)
    {
        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        data.current_chunk = input.pull(true);

        /// Skip block (for 'always_read_till_end' case).
        if (output_finished)
        {
            data.current_chunk.clear();
            if (input.isFinished())
            {
                output.finish();
                return Status::Finished;
            }

            /// Now, we pulled from input, and it must be empty.
            input.setNeeded();
            return Status::NeedData;
        }

        queuePush(std::move(data.current_chunk));

        /// Extract queue length
        if (PopWithoutCut())
        {
            Chunk pop(queuePop());
            if (limit_is_unreachable)
            {
                output.push(std::move(pop));
                return Status::PortFull;
            }
            else
                pop.clear();
        }

        input.setNeeded();
        return Status::NeedData;
    }

    /// Input finished, pop chunk from queue and return
    output.push(queuePop());
    return Status::PortFull;
}

bool LimitTransform::PopWithoutCut()
{
    return rows_to_keep <= rows_in_queue - queue.front().getNumRows();
}

void LimitTransform::queuePush(Chunk data)
{
    rows_in_queue += data.getNumRows();
    queue.emplace_back(std::move(data));
}

Chunk LimitTransform::queuePop()
{
    if (PopWithoutCut())
    {
        Chunk res(std::move(queue.front()));
        queue.pop_front();
        rows_in_queue -= res.getNumRows();
        return res;
    }

    Chunk res(std::move(queue.front()));
    queue.pop_front();
    rows_in_queue -= res.getNumRows();

    if (rows_in_queue >= offset)
    {
        if (rows_in_queue + res.getNumRows() <= rows_to_keep)
            return res;

        /// Need to cut chunk
        UInt64 num_columns = res.getNumColumns();
        UInt64 num_rows = res.getNumRows();
        Columns columns = res.detachColumns();

        ///                                 <--------> rows_in_queue
        ///                           <--------------> rows_to_keep
        ///                                      ^ offset
        ///                <----------------> res
        ///                           <-----> to return
        UInt64 diff = num_rows + rows_in_queue - rows_to_keep;
        for (UInt64 i = 0; i < num_columns; ++i)
            columns[i] = columns[i]->cut(diff, num_rows - diff);
        res.setColumns(std::move(columns), num_rows - diff);
        return res;
    }

    /// Need to cut chunk
    UInt64 num_columns = res.getNumColumns();
    UInt64 num_rows = res.getNumRows();
    Columns columns = res.detachColumns();

    ///                    <------------------> rows_in_queue
    ///  <----------------> res
    ///             ^ offset
    UInt64 diff = offset - rows_in_queue;
    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(0, num_rows - diff);
    res.setColumns(std::move(columns), num_rows - diff);
    return res;
}

LimitTransform::Status LimitTransform::preparePair(PortsData & data)
{
    auto & output = *data.output_port;
    auto & input = *data.input_port;

    /// Check can output.
    bool output_finished = false;
    if (output.isFinished())
    {
        output_finished = true;
        if (!always_read_till_end)
        {
            input.close();
            return Status::Finished;
        }
    }

    if (!output_finished && !output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Check if we are done with pushing.
    bool is_limit_reached = !limit_is_unreachable && rows_read >= offset + limit && !previous_row_chunk;
    if (is_limit_reached)
    {
        if (!always_read_till_end)
        {
            output.finish();
            input.close();
            return Status::Finished;
        }
    }

    /// Check can input.

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    data.current_chunk = input.pull(true);

    auto rows = data.current_chunk.getNumRows();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(rows);

    /// Skip block (for 'always_read_till_end' case).
    if (is_limit_reached || output_finished)
    {
        data.current_chunk.clear();
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        /// Now, we pulled from input, and it must be empty.
        input.setNeeded();
        return Status::NeedData;
    }

    /// Process block.

    rows_read += rows;

    if (rows_read <= offset)
    {
        data.current_chunk.clear();

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        /// Now, we pulled from input, and it must be empty.
        input.setNeeded();
        return Status::NeedData;
    }

    if (rows <= std::numeric_limits<UInt64>::max() - offset && rows_read >= offset + rows
        && !limit_is_unreachable && rows_read <= offset + limit)
    {
        /// Return the whole chunk.

        /// Save the last row of current chunk to check if next block begins with the same row (for WITH TIES).
        if (with_ties && rows_read == offset + limit)
            previous_row_chunk = makeChunkWithPreviousRow(data.current_chunk, data.current_chunk.getNumRows() - 1);
    }
    else
        /// This function may be heavy to execute in prepare. But it happens no more than twice, and make code simpler.
        splitChunk(data);

    bool may_need_more_data_for_ties = previous_row_chunk || rows_read - rows <= offset + limit;
    /// No more data is needed.
    if (!always_read_till_end && !limit_is_unreachable && rows_read >= offset + limit && !may_need_more_data_for_ties)
        input.close();

    output.push(std::move(data.current_chunk));

    return Status::PortFull;
}


void LimitTransform::splitChunk(PortsData & data)
{
    auto current_chunk_sort_columns = extractSortColumns(data.current_chunk.getColumns());
    UInt64 num_rows = data.current_chunk.getNumRows();
    UInt64 num_columns = data.current_chunk.getNumColumns();

    if (previous_row_chunk && !limit_is_unreachable && rows_read >= offset + limit)
    {
        /// Scan until the first row, which is not equal to previous_row_chunk (for WITH TIES)
        UInt64 current_row_num = 0;
        for (; current_row_num < num_rows; ++current_row_num)
        {
            if (!sortColumnsEqualAt(current_chunk_sort_columns, current_row_num))
                break;
        }

        auto columns = data.current_chunk.detachColumns();

        if (current_row_num < num_rows)
        {
            previous_row_chunk = {};
            for (UInt64 i = 0; i < num_columns; ++i)
                columns[i] = columns[i]->cut(0, current_row_num);
        }

        data.current_chunk.setColumns(std::move(columns), current_row_num);
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

    /// check if other rows in current block equals to last one in limit
    if (with_ties && length)
    {
        UInt64 current_row_num = start + length;
        previous_row_chunk = makeChunkWithPreviousRow(data.current_chunk, current_row_num - 1);

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

    auto columns = data.current_chunk.detachColumns();

    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);

    data.current_chunk.setColumns(std::move(columns), length);
}

ColumnRawPtrs LimitTransform::extractSortColumns(const Columns & columns) const
{
    ColumnRawPtrs res;
    res.reserve(description.size());
    for (size_t pos : sort_column_positions)
        res.push_back(columns[pos].get());

    return res;
}

bool LimitTransform::sortColumnsEqualAt(const ColumnRawPtrs & current_chunk_sort_columns, UInt64 current_chunk_row_num) const
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

