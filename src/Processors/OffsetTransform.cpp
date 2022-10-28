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
    bool has_full_port = false;

    auto process_pair = [&](size_t pos)
    {
        auto status = is_negative ? preparePairNegative(ports_data[pos]) : preparePair(ports_data[pos]);

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
                    ErrorCodes::LOGICAL_ERROR, "Unexpected status for OffsetTransform::preparePair : {}", IProcessor::statusToName(status));
        }
    };

    for (auto pos : updated_input_ports)
        process_pair(pos);

    for (auto pos : updated_output_ports)
        process_pair(pos);

    /// All inputs finished
    if (is_negative && num_finished_port_pairs == ports_data.size() && rows_in_queue > offset)
    {
        /// Pop chunk from queue and return
        PortsData pop(queuePop());
        pop.output_port->push(std::move(pop.current_chunk));
        return Status::PortFull;
    }

    /// All ports are finished. It may happen even before we reached the limit (has less data then limit).
    if (num_finished_port_pairs == ports_data.size())
    {
        if (is_negative)
        {
            queue.clear();
            for (auto & data : ports_data)
            {
                data.output_port->finish();
            }
        }

        return Status::Finished;
    }

    if (has_full_port)
        return Status::PortFull;

    return Status::NeedData;
}

OffsetTransform::Status OffsetTransform::prepare()
{
    if (ports_data.size() != 1)
        throw Exception("prepare without arguments is not supported for multi-port OffsetTransform",
                        ErrorCodes::LOGICAL_ERROR);

    return prepare({0}, {0});
}

OffsetTransform::Status OffsetTransform::preparePairNegative(PortsData & data)
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

    if (popWithoutCut())
    {
        PortsData pop(queuePop());
        pop.output_port->push(std::move(pop.current_chunk));
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

    /// Push chunk into queue
    PortsData to_push;
    to_push.current_chunk = std::move(data.current_chunk);
    to_push.output_port = data.output_port;
    queuePush(to_push);

    /// Extract queue length
    if (popWithoutCut())
    {
        PortsData pop(queuePop());
        pop.output_port->push(std::move(pop.current_chunk));
        return Status::PortFull;
    }

    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    return Status::NeedData;
}

void OffsetTransform::queuePush(PortsData & data)
{
    rows_in_queue += data.current_chunk.getNumRows();
    queue.emplace_back(std::move(data));
}

bool OffsetTransform::popWithoutCut()
{
    return !queue.empty() && offset <= rows_in_queue - queue.front().current_chunk.getNumRows();
}

OffsetTransform::PortsData OffsetTransform::queuePop()
{
    if (popWithoutCut())
    {
        PortsData pop(std::move(queue.front()));
        queue.pop_front();
        rows_in_queue -= pop.current_chunk.getNumRows();
        return pop;
    }

    PortsData pop(std::move(queue.front()));
    queue.pop_front();
    rows_in_queue -= pop.current_chunk.getNumRows();
    UInt64 num_columns = pop.current_chunk.getNumColumns();
    UInt64 num_rows = pop.current_chunk.getNumRows();
    Columns columns = pop.current_chunk.detachColumns();

    ///             <--------> rows_in_queue
    ///   <---------> pop
    ///   <-----> to return
    ///         ^offset

    UInt64 length = rows_in_queue + num_rows - offset;
    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(0, length);
    pop.current_chunk.setColumns(std::move(columns), length);
    return pop;
}

OffsetTransform::Status OffsetTransform::preparePair(PortsData & data)
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

