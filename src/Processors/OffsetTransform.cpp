#include <Processors/OffsetTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

OffsetTransform::OffsetTransform(
    const Block & header_, UInt64 offset_, size_t num_streams, bool is_negative_)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , offset(offset_), is_negative(is_negative_)
{
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
}


IProcessor::Status OffsetTransform::prepare(const PortNumbers & updated_input_ports, const PortNumbers & updated_output_ports)
{
    return is_negative ? prepareNegative(updated_input_ports, updated_output_ports) : prepareNonNegative(updated_input_ports, updated_output_ports);
}

IProcessor::Status OffsetTransform::prepareNegative(const PortNumbers & updated_input_ports, const PortNumbers & updated_output_ports)
{
    if (num_finished_input_port == ports_data.size())
        return loopPop();

    bool has_full_port = false;

    /// Return chunks before offset if cut is not needed.
    while (true)
    {
        skipChunksForFinishedOutputPorts();

        if (num_finished_output_port == ports_data.size())
        {
            for (auto & data : ports_data)
            {
                if (!data.is_input_port_finished)
                    data.input_port->close();
            }

            queue.clear();
            return Status::Finished;
        }

        if (queue.empty() || offset > rows_in_queue - queue.front().chunk.getNumRows())
            break;

        /// Check can push
        if (!ports_data[queue.front().port].output_port->canPush())
        {
            has_full_port = true;
            break;
        }

        /// Pop chunk from queue and push it to its output port
        QueueElement & front = queue.front();
        rows_in_queue -= front.chunk.getNumRows();
        ports_data[front.port].output_port->push(std::move(front.chunk));
        queue.pop_front();
        has_full_port = true;
    }

    for (auto pos : updated_input_ports)
        preparePairNegative(pos);

    for (auto pos : updated_output_ports)
        preparePairNegative(pos);

    if (num_finished_output_port == ports_data.size())
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
        return loopPop();

    if (has_full_port)
        return Status::PortFull;

    return Status::NeedData;
}

IProcessor::Status OffsetTransform::prepareNonNegative(const PortNumbers & updated_input_ports, const PortNumbers & updated_output_ports)
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
                                ErrorCodes::LOGICAL_ERROR, "Unexpected status for OffsetTransform::preparePair : {}",
                                IProcessor::statusToName(status));
        }
    };

    for (auto pos : updated_input_ports)
        process_pair(pos);

    for (auto pos : updated_output_ports)
        process_pair(pos);

    /// All ports are finished. It may happen even before we reached the limit (has less data then limit).
    if (num_finished_port_pairs == ports_data.size())
        return Status::Finished;

    if (has_full_port)
        return Status::PortFull;

    return Status::NeedData;
}

OffsetTransform::Status OffsetTransform::prepare()
{
    if (ports_data.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "prepare without arguments is not supported for multi-port OffsetTransform");

    return prepare({0}, {0});
}

void OffsetTransform::preparePairNegative(size_t pos)
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

        if (!data.is_input_port_finished)
        {
            num_finished_input_port++;
            data.is_input_port_finished = true;
            if (!input.isFinished())
                input.close();
        }

        return;
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

    /// Push chunk into queue.
    QueueElement to_push;
    to_push.chunk = input.pull(true);
    to_push.port = pos;
    queuePushBack(to_push);

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
void OffsetTransform::queuePushBack(QueueElement & element)
{
    rows_in_queue += element.chunk.getNumRows();
    queue.emplace_back(std::move(element));
}

/// Pop a chunk from the queue
OffsetTransform::QueueElement OffsetTransform::queuePopFront()
{
    QueueElement pop(std::move(queue.front()));
    queue.pop_front();
    rows_in_queue -= pop.chunk.getNumRows();
    return pop;
}

/// Pop chunks from the queue as many as possible then push them to their ports.
OffsetTransform::Status OffsetTransform::loopPop()
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

        /// Pop chunk from the queue and push it to its output port
        QueueElement pop(popAndCutIfNeeded());
        ports_data[pop.port].output_port->push(std::move(pop.chunk));
    }
}

/// Discard closed output ports' chunks in the queue.
void OffsetTransform::skipChunksForFinishedOutputPorts()
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

/// When offset is negative, pop chunks from the queue if all input ports are finished.
/// Cut operation will not occur more than once.
OffsetTransform::QueueElement OffsetTransform::popAndCutIfNeeded()
{
    if (!queue.empty() && offset <= rows_in_queue - queue.front().chunk.getNumRows())
        return queuePopFront();

    QueueElement pop = queuePopFront();
    UInt64 num_columns = pop.chunk.getNumColumns();
    UInt64 num_rows = pop.chunk.getNumRows();
    Columns columns = pop.chunk.detachColumns();

    ///           <--------> rows_in_queue
    /// <---------> pop
    /// <-----> to return
    ///       ^offset

    UInt64 length = rows_in_queue + num_rows - offset;
    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(0, length);
    pop.chunk.setColumns(std::move(columns), length);
    return pop;
}

OffsetTransform::Status OffsetTransform::preparePairNonNegative(PortsData & data)
{
    auto & output = *data.output_port;
    auto & input = *data.input_port;

    /// Check can output.

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

    if (!(rows <= std::numeric_limits<UInt64>::max() - offset && rows_read >= offset + rows))
        splitChunk(data);

    output.push(std::move(data.current_chunk));

    return Status::PortFull;
}


void OffsetTransform::splitChunk(PortsData & data) const
{
    UInt64 num_rows = data.current_chunk.getNumRows();
    UInt64 num_columns = data.current_chunk.getNumColumns();

    /// return a piece of the block
    UInt64 start = 0;

    /// ------------[....(.....]
    /// <----------------------> rows_read
    ///             <----------> num_rows
    /// <---------------> offset
    ///             <---> start

    assert(offset < rows_read);

    if (offset + num_rows > rows_read)
        start = offset + num_rows - rows_read;
    else
        return;

    UInt64 length = num_rows - start;

    auto columns = data.current_chunk.detachColumns();

    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);

    data.current_chunk.setColumns(std::move(columns), length);
}

}

