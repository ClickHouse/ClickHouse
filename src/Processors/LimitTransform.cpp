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
    , description(std::move(description_)), limit_is_unreachable(limit > std::numeric_limits<UInt64>::max() - offset)
    , is_negative(is_negative_)
{
    if (is_negative)
        rows_to_keep = limit_is_unreachable ? offset : limit + offset;

    if (num_streams != 1 && with_ties)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot use LimitTransform with multiple ports and ties");

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
    return is_negative ? prepareNegative(updated_input_ports, updated_output_ports)
                       : prepareNonNegative(updated_input_ports, updated_output_ports);
}

IProcessor::Status LimitTransform::prepareNegative(
    const PortNumbers & updated_input_ports,
    const PortNumbers & updated_output_ports)
{
    if (num_finished_input_port == ports_data.size())
    {
        /// All input ports are finished. Need to get chunks from the queue and do push.
        return loopPop();
    }

    bool has_full_port = false;

    /// When limit is unreachable, need to get all rows before offset.
    /// Pop some chunks here to avoid too many chunks in the queue.
    /// We pop a chunk only if cut is not needed.
    if (limit_is_unreachable)
    {
        while (true)
        {
            skipChunksForFinishedOutputPorts();

            if (num_finished_output_port == ports_data.size())
            {
                if (always_read_till_end)
                    break;

                for (auto & data : ports_data)
                {
                    if (!data.is_input_port_finished)
                        data.input_port->close();
                }

                queue.clear();
                return Status::Finished;
            }

            if (queue.empty() || rows_to_keep > rows_in_queue - queue.front().chunk.getNumRows())
                break;

            /// Check can push
            if (!ports_data[queue.front().port].output_port->canPush())
            {
                has_full_port = true;
                break;
            }

            /// Pop a chunk from queue and push it to its output port
            QueueElement front = queuePopFront();
            ports_data[front.port].output_port->push(std::move(front.chunk));
            has_full_port = true;
        }
    }

    for (auto pos : updated_input_ports)
        preparePairNegative(pos);

    for (auto pos : updated_output_ports)
        preparePairNegative(pos);

    if (!always_read_till_end && num_finished_output_port == ports_data.size())
    {
        for (auto & data : ports_data)
        {
            if (!data.is_input_port_finished)
                data.input_port->close();
        }

        queue.clear();
        return Status::Finished;
    }

    if (num_finished_input_port == ports_data.size())
    {
        /// All input ports are finished. Need to get chunks from the queue and do push.
        return loopPop();
    }

    if (has_full_port)
        return Status::PortFull;

    return Status::NeedData;
}

IProcessor::Status LimitTransform::prepareNonNegative(
    const PortNumbers & updated_input_ports,
    const PortNumbers & updated_output_ports)
{
    bool has_full_port = false;

    auto process_pair = [&](size_t pos)
    {
        auto status = preparePairNonNegative(ports_data[pos]);

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
    if ((!limit_is_unreachable && rows_read >= offset + limit)
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "prepare without arguments is not supported for multi-port LimitTransform");

    return prepare({0}, {0});
}

void LimitTransform::preparePairNegative(size_t pos)
{
    PortsData & data = ports_data[pos];
    auto & output = *data.output_port;
    auto & input = *data.input_port;

    if (output.isFinished())
    {
        if (!data.is_output_port_finished)
        {
            num_finished_output_port++;
            data.is_output_port_finished = true;
        }

        if (!always_read_till_end)
        {
            if (!data.is_input_port_finished)
            {
                num_finished_input_port++;
                data.is_input_port_finished = true;
                if (!input.isFinished())
                    input.close();
            }
            return;
        }
    }

    /// Check can input.
    if (input.isFinished())
    {
        if (!data.is_input_port_finished)
        {
            num_finished_input_port++;
            data.is_input_port_finished = true;
        }
        return;
    }

    /// Check input has data.
    input.setNeeded();
    if (!input.hasData())
        return;

    if (output.isFinished())
    {
        /// When always_read_till_end is true and the output port is finished.
        /// There is no need to push this chunk into the queue.
        data.current_chunk = input.pull(true);
        data.current_chunk.clear();
    }
    else
    {
        /// Push chunk into queue.
        QueueElement to_push;
        to_push.chunk = input.pull(true);
        to_push.port = pos;
        queuePushBack(to_push);
    }

    /// Extract queue length. Discard chunks not needed.
    /// We discard a chunk only if cut is not needed.
    if (!limit_is_unreachable)
    {
        while (!queue.empty() && rows_to_keep <= rows_in_queue - queue.front().chunk.getNumRows())
        {
            rows_in_queue -= queue.front().chunk.getNumRows();
            queue.pop_front();
        }
    }

    if (input.isFinished())
    {
        if (!data.is_input_port_finished)
        {
            num_finished_input_port++;
            data.is_input_port_finished = true;
        }
        return;
    }

    input.setNeeded();
}

/// Push a chunk to the queue.
void LimitTransform::queuePushBack(QueueElement & element)
{
    rows_in_queue += element.chunk.getNumRows();
    queue.emplace_back(std::move(element));
}

/// Pop a chunk from the queue
LimitTransform::QueueElement LimitTransform::queuePopFront()
{
    QueueElement pop(std::move(queue.front()));
    queue.pop_front();
    rows_in_queue -= pop.chunk.getNumRows();
    return pop;
}

/// When limit and offset are negative, pop chunks from the queue if all input ports are finished.
/// Cut operation will not occur more than twice.
LimitTransform::QueueElement LimitTransform::popAndCutIfNeeded()
{
    if (limit_is_unreachable && !queue.empty() && rows_to_keep <= rows_in_queue - queue.front().chunk.getNumRows())
        return queuePopFront();

    QueueElement pop = queuePopFront();

    if (rows_in_queue >= offset)
    {
        if (rows_in_queue + pop.chunk.getNumRows() <= rows_to_keep)
            ///              <--------> rows_in_queue
            /// <---------------------> rows_to_keep
            /// ^limit            ^offset
            ///      <-------> pop
            ///      <-------> to return
            return pop;

        /// Need to cut chunk
        UInt64 num_columns = pop.chunk.getNumColumns();
        UInt64 num_rows = pop.chunk.getNumRows();
        Columns columns = pop.chunk.detachColumns();

        ///                  <--------> rows_in_queue
        ///            <--------------> rows_to_keep
        ///            ^limit      ^offset
        /// <----------------> pop
        ///            <-----> to return
        UInt64 diff = num_rows + rows_in_queue - rows_to_keep;
        for (UInt64 i = 0; i < num_columns; ++i)
            columns[i] = columns[i]->cut(diff, num_rows - diff);
        pop.chunk.setColumns(std::move(columns), num_rows - diff);
        return pop;
    }

    /// Need to cut chunk
    UInt64 num_columns = pop.chunk.getNumColumns();
    UInt64 num_rows = pop.chunk.getNumRows();
    Columns columns = pop.chunk.detachColumns();
    UInt64 start;
    UInt64 length;

    if (rows_in_queue + num_rows > rows_to_keep)
    {
        ///                   <------------------> rows_in_queue
        /// <----------------> pop
        ///    ^limit    ^offset
        ///     <--------> to return (limit is reachable)
        /// <------------> to return (limit is unreachable)
        /// When limit is unreachable, need to return all rows before offset
        if (limit_is_unreachable)
            start = 0;
        else
            start = rows_in_queue + num_rows - rows_to_keep;
    }
    else
    {
        ///                      <------------------> rows_in_queue
        ///     <----------------> pop
        /// ^limit    ^offset
        ///     <-----> to return
        start = 0;
    }

    length = rows_in_queue + num_rows - offset - start;
    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);
    pop.chunk.setColumns(std::move(columns), length);
    return pop;
}

/// Discard closed output ports' chunks in the queue.
void LimitTransform::skipChunksForFinishedOutputPorts()
{
    while (num_finished_output_port < ports_data.size() &&
           rows_in_queue > offset &&
           ports_data[queue.front().port].output_port->isFinished())
    {
        QueueElement & front = queue.front();

        if (!ports_data[front.port].is_output_port_finished)
        {
            num_finished_output_port++;
            ports_data[front.port].is_output_port_finished = true;
        }

        rows_in_queue -= front.chunk.getNumRows();
        queue.pop_front();
    }
}

/// Pop chunks from the queue as many as possible then push them to their ports.
LimitTransform::Status LimitTransform::loopPop()
{
    while (true)
    {
        skipChunksForFinishedOutputPorts();

        if (num_finished_output_port == ports_data.size())
        {
            queue.clear();
            return Status::Finished;
        }

        if (rows_in_queue <= offset)
        {
            for (auto & data : ports_data)
            {
                if (!data.is_output_port_finished)
                    data.output_port->finish();
            }

            queue.clear();
            return Status::Finished;
        }

        if (!ports_data[queue.front().port].output_port->canPush())
            return Status::PortFull;

        QueueElement pop(popAndCutIfNeeded());
        ports_data[pop.port].output_port->push(std::move(pop.chunk));
    }
}

LimitTransform::Status LimitTransform::preparePairNonNegative(PortsData & data)
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

    if (rows_before_limit_at_least && !data.input_port_has_counter)
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

