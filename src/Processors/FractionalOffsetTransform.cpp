#include <Columns/IColumn.h>
#include <Processors/Chunk.h>
#include <Processors/FractionalOffsetTransform.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void FractionalOffsetTransform::finalizeOffset()
{
    /// Fractions depend on the final total number of rows, so we can only compute the integral
    /// offset once all input is read.
    if (offset_is_final)
        return;

    const UInt64 total_offset = static_cast<UInt64>(std::ceil(static_cast<double>(rows_cnt) * fractional_offset));
    offset = (total_offset > evicted_rows_cnt) ? (total_offset - evicted_rows_cnt) : 0;

    offset_is_final = true;
}

FractionalOffsetTransform::FractionalOffsetTransform(const Block & header_, Float64 fractional_offset_, size_t num_streams)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , fractional_offset(fractional_offset_)
{
    if (fractional_offset <= 0.0 || fractional_offset >= 1.0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fractional OFFSET values must be in the range (0, 1)");

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


FractionalOffsetTransform::Status FractionalOffsetTransform::prepare()
{
    if (allOutputsFinished())
    {
        /// Nobody needs data: stop sources.
        for (auto & port : ports_data)
            port.input_port->close();
        return Status::Finished;
    }

    /// Check can we still pull data from input?
    if (num_finished_input_ports != ports_data.size())
    {
        auto process = [&](size_t pos)
        {
            auto status = pullData(ports_data[pos]);
            switch (status)
            {
                case IProcessor::Status::Finished:
                {
                    if (!ports_data[pos].is_input_port_finished)
                    {
                        ports_data[pos].is_input_port_finished = true;
                        ++num_finished_input_ports;
                    }
                    return;
                }
                case IProcessor::Status::NeedData:
                    return;
                default:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected status for FractionalOffsetTransform::pullData : {}",
                        IProcessor::statusToName(status));
            }
        };

        for (size_t pos = 0; pos < ports_data.size(); ++pos)
            process(pos);

        if (num_finished_input_ports != ports_data.size())
            /// Some input ports still available => we can read more data
            return Status::NeedData;

        /// All input is read: compute the final integral offset.
        finalizeOffset();
    }
    else if (!offset_is_final)
    {
        finalizeOffset();
    }

    /// If we reached here then all input ports are finished.
    /// we start pushing cached chunks to output ports.
    auto status = pushData();

    if (status != Status::Finished)
        return status;

    for (auto & port : ports_data)
    {
        port.input_port->close();
        port.output_port->finish();
    }

    return Status::Finished;
}

bool FractionalOffsetTransform::allOutputsFinished() const
{
    for (const auto & data : ports_data)
        if (!data.output_port->isFinished())
            return false;
    return true;
}

OutputPort * FractionalOffsetTransform::getAvailableOutputPort()
{
    /// Output ports are interchangeable. Pick the next available port in round-robin order.
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

FractionalOffsetTransform::Status FractionalOffsetTransform::pullData(PortsData & data)
{
    auto & input = *data.input_port;

    /// Check can input?
    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    data.current_chunk = input.pull();

    auto rows = data.current_chunk.getNumRows();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(rows);

    /// Cache block.
    rows_cnt += rows;
    chunks_cache.push_back(std::move(data.current_chunk));

    /// Detect blocks that will 100% get removed by the offset and remove them as early as possible.
    /// example: if we have 10 blocks with the same num of rows and offset 0.1 we can freely drop the first block even before reading all data.
    while (!chunks_cache.empty()
           && static_cast<UInt64>(std::ceil(static_cast<double>(rows_cnt) * fractional_offset)) - evicted_rows_cnt >= chunks_cache.front().getNumRows())
    {
        evicted_rows_cnt += chunks_cache.front().getNumRows();
        chunks_cache.pop_front();
    }

    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    return Status::NeedData;
}

FractionalOffsetTransform::Status FractionalOffsetTransform::pushData()
{
    /// Drain the cache while we have an output that can accept data. Return PortFull only if
    /// all outputs are currently blocked; output finishing is handled by prepare().
    while (!chunks_cache.empty())
    {
        auto * output = getAvailableOutputPort();
        if (!output)
            return Status::PortFull;

        auto & chunk = chunks_cache.front();
        UInt64 rows = chunk.getNumRows();

        /// The early removal of blocks at pullData() should have detected all chunks that will be dropped
        /// entirely, but we may still need to offset inside the first block and drop a portion of it.
        if (offset)
        {
            /// We can still have an integral offset to apply that spans multiple chunks.
            if (offset >= rows)
            {
                offset -= rows;
                chunks_cache.pop_front();
                continue;
            }

            /// Offset is inside this chunk: cut the prefix and clear the remaining integral offset.
            /// This function may be heavy to execute. But it happens no more than once.
            splitChunk(chunk);
            offset = 0; // Done.
        }

        output->push(std::move(chunk));
        chunks_cache.pop_front();
    }

    return Status::Finished;
}


void FractionalOffsetTransform::splitChunk(Chunk & current_chunk) const
{
    /// return a piece of the block

    UInt64 num_rows = current_chunk.getNumRows();
    UInt64 num_columns = current_chunk.getNumColumns();

    /// [....(.....]
    /// <----------> num_rows
    /// <---> offset

    assert(offset < num_rows);

    UInt64 length = num_rows - offset;
    auto columns = current_chunk.detachColumns();

    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(offset, length);

    current_chunk.setColumns(std::move(columns), length);
}

}
