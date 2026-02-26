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

FractionalOffsetTransform::FractionalOffsetTransform(const Block & header_, Float64 fractional_offset_, size_t num_streams)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , fractional_offset(fractional_offset_)
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


IProcessor::Status FractionalOffsetTransform::prepare(const PortNumbers & updated_input_ports, const PortNumbers & updated_output_ports)
{
    // Check Can we still pull input?
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

        for (auto pos : updated_input_ports)
            process(pos);

        for (auto pos : updated_output_ports)
            process(pos);

        if (num_finished_input_ports != ports_data.size())
            // Inputs available we can still get more
            return Status::NeedData;

        // Calculate target offset
        offset = static_cast<UInt64>(std::ceil(rows_cnt * fractional_offset));
    }

    // If we reached here then all input ports are finished.
    // we start pushing cached chunks to output ports.
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

FractionalOffsetTransform::Status FractionalOffsetTransform::prepare()
{
    if (ports_data.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "prepare without arguments is not supported for multi-port FractionalOffsetTransform");

    return prepare({0}, {0});
}

FractionalOffsetTransform::Status FractionalOffsetTransform::pullData(PortsData & data)
{
    auto & input = *data.input_port;

    /// Check can input.
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
    chunks_cache.push_back({data.output_port, std::move(data.current_chunk)});

    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    return Status::NeedData;
}

FractionalOffsetTransform::Status FractionalOffsetTransform::pushData()
{
    while (!chunks_cache.empty() && chunks_cache.front().output_port->isFinished())
        chunks_cache.pop_front();

    if (chunks_cache.empty())
        return Status::Finished;

    auto & output = *chunks_cache.front().output_port;

    if (!output.canPush())
        return Status::PortFull;

    UInt64 rows = 0;
    do
    {
        rows = chunks_cache.front().chunk.getNumRows();
        rows_read += rows;
        if (rows_read <= offset)
            chunks_cache.pop_front();
    } while (rows_read <= offset && !chunks_cache.empty());

    if (chunks_cache.empty())
    {
        output.finish();
        return Status::Finished;
    }

    if (!(rows <= std::numeric_limits<UInt64>::max() - offset && rows_read >= offset + rows))
        splitChunk(chunks_cache.front().chunk);

    output.push(std::move(chunks_cache.front().chunk));
    chunks_cache.pop_front();

    return Status::PortFull;
}


void FractionalOffsetTransform::splitChunk(Chunk & current_chunk) const
{
    UInt64 num_rows = current_chunk.getNumRows();
    UInt64 num_columns = current_chunk.getNumColumns();

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

    auto columns = current_chunk.detachColumns();

    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);

    current_chunk.setColumns(std::move(columns), length);
}

}
