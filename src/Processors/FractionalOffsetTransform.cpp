#include <Columns/IColumn.h>
#include <Processors/FractionalOffsetTransform.h>
#include <Processors/Port.h>
#include <base/BFloat16.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FractionalOffsetTransform::FractionalOffsetTransform(
    const Block & header_, BFloat16 fractional_offset_, size_t num_streams)
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
    bool has_full_port = false;
    bool has_finished_output_port = false;

    auto process_pair = [&](size_t pos)
    {
        auto status = preparePair(ports_data[pos]);

        switch (status)
        {
            case IProcessor::Status::Finished:
            {
                if (ports_data[pos].output_port->isFinished())
                {
                    has_finished_output_port = true;
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

    /// All ports are finished. It may happen even before we reached the limit (has less data then limit).
    if (has_finished_output_port)
    {
        for (auto & chunk : chunks_cache)
            chunk.clear();

        chunks_cache.clear();

        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    if (has_full_port)
        return Status::PortFull;

    if (num_finished_input_ports == ports_data.size())
    {
        offset = static_cast<UInt64>(std::ceil(rows_cnt * fractional_offset));
        // Caching done, call one more time to start producing output.
        return prepare(updated_input_ports, updated_output_ports);
    }

    return Status::NeedData;
}

FractionalOffsetTransform::Status FractionalOffsetTransform::prepare()
{
    if (ports_data.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "prepare without arguments is not supported for multi-port OffsetTransform");

    return prepare({0}, {0});
}

FractionalOffsetTransform::Status FractionalOffsetTransform::preparePair(PortsData & data)
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

        if (chunks_cache.empty())
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
        } while (rows_read <= offset && !chunks_cache.empty());

        if (chunks_cache.empty())
        {
            output.finish();
            return Status::Finished;
        }

        if (!(rows <= std::numeric_limits<UInt64>::max() - offset && rows_read >= offset + rows))
            splitChunk(chunks_cache[0]);

        output.push(std::move(chunks_cache[0]));
        chunks_cache.pop_front();
        return Status::PortFull;
    }

    /// Check can input.
    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    data.current_chunk = input.pull(true);

    auto rows = data.current_chunk.getNumRows();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(rows);

    /// Process block.
    rows_cnt += rows;
    chunks_cache.push_back(std::move(data.current_chunk));

    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    return Status::NeedData;
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

