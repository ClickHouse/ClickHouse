#include <Columns/IColumn.h>
#include <Processors/NegativeOffsetTransform.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

NegativeOffsetTransform::NegativeOffsetTransform(const Block & header_, UInt64 offset_, size_t num_streams)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , offset(offset_)
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

/// First, our goal is to pull all the data from input ports. Once we have reached the end,
/// then it is clear what should be part of the `offset` and what should be pushed out to the output ports.
NegativeOffsetTransform::Status NegativeOffsetTransform::prepare()
{
    if (allOutputsFinished())
    {
        for (auto & port : ports_data)
            port.input_port->close();
        return Status::Finished;
    }

    if (stage == Stage::Pull)
    {

        bool has_data_need = false;
        bool has_full_port = false;

        auto process = [&](size_t pos)
        {
            auto status = advancePort(ports_data[pos]);
            switch (status)
            {
                case IProcessor::Status::Finished: {
                    if (!ports_data[pos].is_input_port_finished)
                    {
                        ports_data[pos].is_input_port_finished = true;
                        ++num_input_ports_finished;
                    }
                    return;
                }
                case IProcessor::Status::NeedData: {
                    has_data_need = true;
                    return;
                }
                case IProcessor::Status::PortFull: {
                    has_full_port = true;
                    return;
                }
                default:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected status in NegativeOffsetTransform::advancePort : {}",
                        IProcessor::statusToName(status));
            }
        };

        for (size_t pos = 0; pos < ports_data.size(); ++pos)
            process(pos);

        if (has_data_need)
            return Status::NeedData;

        if (has_full_port)
            return Status::PortFull;

        /// All data fetching is done. We can now start pushing out the remaining data
        if (num_input_ports_finished == ports_data.size())
        {
            stage = Stage::Push;
        }
        else
        {
            return Status::NeedData;
        }
    }

    if (stage == Stage::Push)
    {
        Status status = tryPushWholeFrontChunk();

        if (status != Status::Finished)
            return status;

        status = tryPushRemainingChunkPrefix();

        if (status != Status::Finished)
            return status;

        if (queued_row_count > offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "In NegativeOffsetTransform::prepare, at this point queued rows {} should be less than or equal to offset {}",
                queued_row_count,
                offset);

        for (auto & port : ports_data)
        {
            port.input_port->close();
            port.output_port->finish();
        }
        return Status::Finished;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "NegativeOffsetTransform::prepare in unknown stage");
}

NegativeOffsetTransform::Status NegativeOffsetTransform::advancePort(PortsData & data)
{
    auto & input = *data.input_port;

    /// Check can input.
    if (input.isFinished())
    {
        return Status::Finished;
    }

    /// If we already have enough rows buffered, try to push whole chunks when output ports become available.
    Status push_status = tryPushWholeFrontChunk();
    if (push_status != Status::Finished)
        return push_status;

    input.setNeeded();

    if (input.hasData())
    {
        Chunk chunk = input.pull(true);

        input.setNeeded();

        auto rows = chunk.getNumRows();

        queued_row_count += rows;

        if (rows_before_limit_at_least)
        {
            rows_before_limit_at_least->add(rows);
        }

        queue.push(ChunkWithPort{std::move(chunk)});

        /// Push whole chunks while we can still keep the required offset.
        /// Ensures that queue does not grow too large.
        push_status = tryPushWholeFrontChunk();

        if (push_status != Status::Finished)
            return push_status;
    }

    if (input.isFinished())
        return Status::Finished;

    return Status::NeedData;
}

IProcessor::Status NegativeOffsetTransform::tryPushWholeFrontChunk()
{
    /// Need to keep at least 'offset' rows queued.
    while (queued_row_count > offset)
    {
        assert(!queue.empty() && "Queue is empty in tryPushWholeFrontChunk");

        auto & front = queue.front();

        Chunk & chunk = front.chunk;
        const UInt64 front_chunk_rows = chunk.getNumRows();

        /// Make sure that front chunk can be completey pushed without potentially
        /// going into the offset area.
        if (queued_row_count - front_chunk_rows < offset)
            return Status::Finished;

        auto * output = getAvailableOutputPort();
        if (!output)
            return Status::PortFull;

        output->push(std::move(chunk));

        queue.pop();
        queued_row_count -= front_chunk_rows;
    }

    return Status::Finished;
}

IProcessor::Status NegativeOffsetTransform::tryPushRemainingChunkPrefix()
{
    /// Need to keep at least 'offset' rows queued.
    if (queued_row_count <= offset)
        return Status::Finished;

    auto & front = queue.front();

    Chunk & chunk = front.chunk;
    const UInt64 front_chunk_rows = chunk.getNumRows();

    if (queued_row_count - front_chunk_rows >= offset)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "NegativeOffsetTransformtryPushRemainingChunkPrefix must not be required to fully push the front chunk");

    auto * output = getAvailableOutputPort();
    if (!output)
        return Status::PortFull;

    /// queued_row_count    <---------------------->
    /// front_chunk_rows    <---------->
    ///           offset           <--------------->
    ///                     <-----> (cut `take` amount)

    /// Push the prefix that leaves exactly 'offset' queued.
    const UInt64 take = queued_row_count - offset;

    const UInt64 num_columns = chunk.getNumColumns();
    auto columns = chunk.detachColumns();
    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(0, take);

    chunk.setColumns(std::move(columns), take);

    /// Remove the remaining rows after the cut.
    queued_row_count -= (front_chunk_rows - take);

    output->push(std::move(chunk));

    queue.pop();
    queued_row_count -= take;

    return Status::Finished;
}

bool NegativeOffsetTransform::allOutputsFinished() const
{
    for (const auto & data : ports_data)
    {
        if (!data.output_port->isFinished())
            return false;
    }
    return true;
}

OutputPort * NegativeOffsetTransform::getAvailableOutputPort()
{
    const size_t num_outputs = ports_data.size();

    if (num_outputs == 0)
        return nullptr;

    for (size_t i = 0; i < num_outputs; ++i)
    {
        const size_t idx = (next_output_port + i) % num_outputs;

        auto & output = *ports_data[idx].output_port;
        if (output.isFinished())
            continue;

        if (!output.canPush())
            continue;

        next_output_port = (idx + 1) % num_outputs;
        return &output;
    }

    return nullptr;
}
}
