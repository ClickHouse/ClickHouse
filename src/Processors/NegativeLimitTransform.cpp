#include <Columns/IColumn.h>
#include <Processors/NegativeLimitTransform.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

NegativeLimitTransform::NegativeLimitTransform(SharedHeader header_, UInt64 limit_, UInt64 offset_, size_t num_streams)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , limit(limit_)
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
/// then it is clear what should be part of the `limit`, `offset` and what should be pushed out to the output ports.
IProcessor::Status NegativeLimitTransform::prepare(const PortNumbers & updated_input_ports, const PortNumbers & updated_output_ports)
{
    if (stage == Stage::Pull)
    {
        bool has_data_need = false;

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
                default:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected status in NegativeLimitTransform::advancePort : {}",
                        IProcessor::statusToName(status));
            }
        };

        for (auto pos : updated_input_ports)
            process(pos);

        for (auto pos : updated_output_ports)
            process(pos);

        if (has_data_need)
            return Status::NeedData;

        /// All data fetching is done
        if (num_input_ports_finished == ports_data.size())
        {
            stage = Stage::Push;
        }
        else
        {
            return Status::NeedData;
        }
    }

    /// If we enter this stage, it means that we have all the input data and all input ports are closed, and there
    /// are three scenarios:
    /// 1. queued_row_count > limit + offset
    ///    I. We first get rid of the prefix of leftmost chunk to make queued_row_count == limit + offset.
    ///    II. Then keep pushing the left whole chunks to output ports without going into the offset area.
    ///    III. Finally, get rid of the suffix of the leftmost chunk to make queued_row_count == offset, and push the
    ///         `cutted` chunk to output port.
    ///
    /// 2. queued_row_count > offset but <= limit + offset (if there are less than limit + offset rows in total)
    ///    I. Follow step II of scenario 1
    ///    II. Follow step III of scenario 1
    ///
    /// 3. queued_row_count <= offset  (if there are no more than offset rows in total)
    ///     I. Nothing to do or push. Close the output ports.

    /// To simplify the implementation, since scenario 1 is just a generalization of scenario 2 and 3,
    /// we assume scenario 1 always happens and add some extra checks to make scenario 2 and 3 work
    /// through scenario 1.
    if (stage == Stage::Push)
    {
        /// Step I of scenario 1
        Status status = tryPushChunkSuffixWithinLimit();

        if (status != Status::Finished)
            return status;

        /// Step II of scenario 1
        status = tryPushWholeFrontChunk();

        if (status != Status::Finished)
            return status;

        /// Step III of scenario 1
        status = tryPushChunkPrefixWithinLimit();

        if (status != Status::Finished)
            return status;

        if (queued_row_count > offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "In NegativeLimitTransform::prepare, at this point queued rows {} should be less than or equal to offset {}",
                queued_row_count,
                offset);

        for (auto & port : ports_data)
        {
            port.input_port->close();
            port.output_port->finish();
        }
        return Status::Finished;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "NegativeLimitTransform::prepare in unknown stage");
}

NegativeLimitTransform::Status NegativeLimitTransform::prepare()
{
    if (ports_data.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "prepare without arguments is not supported for multi-port NegativeLimitTransform");

    return prepare({0}, {0});
}

NegativeLimitTransform::Status NegativeLimitTransform::advancePort(PortsData & data)
{
    auto & output = *data.output_port;
    auto & input = *data.input_port;

    /// Check can input.
    if (input.isFinished())
    {
        return Status::Finished;
    }

    input.setNeeded();

    if (input.hasData())
    {
        Chunk chunk = input.pull(true);

        input.setNeeded();

        auto rows = chunk.getNumRows();

        queued_row_count += rows;

        if (rows_before_limit_at_least && !data.input_port_has_counter)
        {
            rows_before_limit_at_least->add(rows);
        }

        queue.push(ChunkWithPort{&output, std::move(chunk)});

        /// Try removing the whole chunks that will never be part of the LIMIT
        while (!queue.empty())
        {
            auto & front = queue.front();

            Chunk & fchunk = front.chunk;
            const UInt64 front_chunk_rows = fchunk.getNumRows();

            /// In short, we are checking if (queued_row_count - front_chunk_rows) >= offset + limit
            /// It is written this way to avoid potential overflow.
            const UInt64 rem = queued_row_count - front_chunk_rows;
            if (rem >= offset && (rem - offset) >= limit)
            {
                queued_row_count -= front_chunk_rows;
                queue.pop();
            }
            else
            {
                break;
            }
        }
    }

    if (input.isFinished())
        return Status::Finished;

    return Status::NeedData;
}


IProcessor::Status NegativeLimitTransform::tryPushChunkSuffixWithinLimit()
{
    /// A chunk need to have prefix before limit + offset and suffix inside limit + offset
    // (queued_row_count <= limit + offset) in a overflow-safe way
    if (queued_row_count <= offset || queued_row_count - offset <= limit)
        return Status::Finished;

    assert(!queue.empty() && "Queue is empty in tryPushChunkSuffixWithinLimit");

    auto & front = queue.front();

    auto & output = *front.output_port;

    Chunk & chunk = front.chunk;
    const UInt64 front_chunk_rows = chunk.getNumRows();

    UInt64 rem = queued_row_count - front_chunk_rows;
    if (rem >= offset && rem - offset >= limit)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "In NegativeLimitTransform::tryPushChunkSuffixWithinLimit chunk must be partially inside limit + offset");

    if (output.isFinished())
    {
        queue.pop();
        queued_row_count -= front_chunk_rows;
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    /// queued_row_count    <---------------------->
    /// front_chunk_rows    <---------->
    ///   limit + offset           <--------------->
    ///                            <---> (keep the 'take' amount)

    /// Push the prefix that leaves exactly 'offset' queued.
    const UInt64 start = (queued_row_count - limit) - offset;
    const UInt64 take = front_chunk_rows - start;

    const UInt64 num_columns = chunk.getNumColumns();
    auto columns = chunk.detachColumns();
    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, take);

    chunk.setColumns(std::move(columns), take);

    /// Reduce the rows that does not remain in the chunk after the cut.
    queued_row_count -= (front_chunk_rows - take);

    /// The remaining chunk might not be completely within the `limit` area but
    /// might also partially in the `offset` area. If it goes into the `offset`
    /// area, it will be handled in `tryPushChunkPrefixWithinLimit`.
    if (queued_row_count - take < offset)
    {
        return Status::Finished;
    }

    output.push(std::move(chunk));
    queue.pop();
    queued_row_count -= take;

    return Status::PortFull;
}


IProcessor::Status NegativeLimitTransform::tryPushWholeFrontChunk()
{
    // (queued_row_count > limit + offset) in a overflow-safe way
    if (queued_row_count >= offset && (queued_row_count - offset) > limit)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "In NegativeLimitTransform::tryPushWholeFrontChunk, queued rows should be less than or equal to limit + offset");
    }

    /// Output port is closed, nothing can be done with the chunks; so, we keep discarding them.
    while (!queue.empty() && queue.front().output_port->isFinished())
    {
        auto & front = queue.front();
        const UInt64 front_chunk_rows = front.chunk.getNumRows();
        queue.pop();
        queued_row_count -= front_chunk_rows;
    }

    /// Need to keep at least 'offset' rows queued.
    if (queued_row_count <= offset)
        return Status::Finished;

    assert(!queue.empty() && "Queue is empty in tryPushWholeFrontChunk");

    auto & front = queue.front();

    auto & output = *front.output_port;

    Chunk & chunk = front.chunk;
    const UInt64 front_chunk_rows = chunk.getNumRows();

    /// Make sure that front chunk can be completey pushed without potentially
    /// going into the offset area.
    if (queued_row_count - front_chunk_rows < offset)
        return Status::Finished;

    /// Output port is closed, nothing can be done with the chunk; so, we discard it.
    if (output.isFinished())
    {
        queue.pop();
        queued_row_count -= front_chunk_rows;
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    output.push(std::move(chunk));

    queue.pop();
    queued_row_count -= front_chunk_rows;

    return Status::PortFull;
}

IProcessor::Status NegativeLimitTransform::tryPushChunkPrefixWithinLimit()
{
    /// Need to keep at least 'offset' rows queued.
    if (queued_row_count <= offset)
        return Status::Finished;


    assert(!queue.empty() && "Queue is empty in tryPushChunkPrefixWithinLimit");

    auto & front = queue.front();

    auto & output = *front.output_port;

    Chunk & chunk = front.chunk;
    const UInt64 front_chunk_rows = chunk.getNumRows();

    if (queued_row_count - front_chunk_rows >= offset)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "In NegativeLimitTransform::tryPushChunkPrefixWithinLimit must not be required to fully push the front chunk");

    if (output.isFinished())
    {
        queue.pop();
        queued_row_count -= front_chunk_rows;
        return Status::Finished;
    }

    if (!output.canPush())
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

    /// Reduce the rows that does not remain in the chunk after the cut.
    queued_row_count -= (front_chunk_rows - take);

    output.push(std::move(chunk));
    queue.pop();
    queued_row_count -= take;

    return Status::PortFull;
}

}
