#include <Processors/Transforms/BufferedShardByHashTransform.h>

#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>
#include <Common/HashTable/Hash.h>
#include <Common/MapToRange.h>

namespace DB
{

BufferedShardByHashTransform::BufferedShardByHashTransform(
    SharedHeader header,
    size_t num_shards_,
    ColumnNumbers key_columns_,
    ColumnNumbers sorting_columns_)
    : IProcessor(InputPorts{header}, OutputPorts{num_shards_, header})
    , num_shards(num_shards_)
    , key_columns(std::move(key_columns_))
    , sorting_columns(std::move(sorting_columns_))
    , queues(num_shards)
    , has_rows_in_current_chunk(num_shards, 0)
    , bound_sent(num_shards, 0)
{
    chassert(num_shards > 0);
    chassert(!sorting_columns.empty());
}

Chunk BufferedShardByHashTransform::makeMarker() const
{
    const auto & header = getInputs().front().getHeader();
    Chunk marker(header.cloneEmptyColumns(), 0);
    /// A virtual row: the merge compares it by the sort columns in `bound` and never outputs it.
    marker.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(0, bound, nullptr));
    return marker;
}

IProcessor::Status BufferedShardByHashTransform::prepare()
{
    auto & input = getInputs().front();

    bool all_finished = true;
    size_t shard = 0;
    for (const auto & output : outputs)
    {
        if (output.isFinished())
            queues[shard].clear();
        else
            all_finished = false;
        ++shard;
    }

    if (all_finished)
    {
        input_chunk = {};
        has_input_chunk = false;
        input.close();
        return Status::Finished;
    }

    if (has_input_chunk)
        return Status::Ready;

    /// Push the oldest pending piece of every shard whose port can take it.
    shard = 0;
    for (auto & output : outputs)
    {
        auto & queue = queues[shard];
        if (!queue.empty() && !output.isFinished() && output.canPush())
        {
            if (queue.front().chunk_number == current_chunk_number && shard == last_row_shard)
                bound_sent[shard] = 1;
            output.push(std::move(queue.front().chunk));
            queue.pop_front();
        }
        ++shard;
    }

    if (input.isFinished())
    {
        /// Finishing an output tells its merge that nothing smaller will come from this stream, so it does not
        /// need markers any more. A chunk parked in the port is still pulled by the merge before it sees the end.
        bool fully_drained = true;
        shard = 0;
        for (auto & output : outputs)
        {
            if (queues[shard].empty())
                output.finish();
            else
                fully_drained = false;
            ++shard;
        }
        return fully_drained ? Status::Finished : Status::PortFull;
    }

    /// The scatter cannot read further while some shard has `max_pending_pieces` pieces it cannot push.
    bool blocked = false;
    for (const auto & queue : queues)
    {
        if (queue.size() >= max_pending_pieces)
        {
            blocked = true;
            break;
        }
    }

    /// Tell the bound of the last input chunk to every shard that waits for this stream (can accept a chunk and
    /// has nothing pending) when its merge could otherwise wait for it forever: the shard got no rows from the
    /// chunk, or this scatter cannot read further because of another shard (see the class comment). A shard that
    /// got rows while the scatter is not blocked receives the next bound with the next chunk.
    if (bound.columns())
    {
        shard = 0;
        for (auto & output : outputs)
        {
            if (!bound_sent[shard] && queues[shard].empty() && (blocked || !has_rows_in_current_chunk[shard])
                && !output.isFinished() && output.canPush())
            {
                output.push(makeMarker());
                bound_sent[shard] = 1;
            }
            ++shard;
        }
    }

    if (blocked)
        return Status::PortFull;

    /// Read the next chunk if some shard can take data now.
    input.setNeeded();

    bool any_output_can_push = false;
    for (const auto & output : outputs)
    {
        /// A port closed by the downstream can still report that it is needed.
        if (!output.isFinished() && output.canPush())
        {
            any_output_can_push = true;
            break;
        }
    }

    if (!any_output_can_push)
        return Status::PortFull;

    if (!input.hasData())
        return Status::NeedData;

    input_chunk = input.pull();
    has_input_chunk = true;
    return Status::Ready;
}

void BufferedShardByHashTransform::work()
{
    splitInputChunk();
    input_chunk = {};
    has_input_chunk = false;
}

void BufferedShardByHashTransform::splitInputChunk()
{
    const size_t num_rows = input_chunk.getNumRows();
    if (num_rows == 0)
        return;

    const auto & header = getInputs().front().getHeader();
    auto columns = input_chunk.detachColumns();

    /// Compute a composite 32-bit hash over all key columns into a reusable buffer.
    hash_buffer.assign(num_rows, WEAK_HASH32_INITIAL_VALUE);
    for (auto column_number : key_columns)
        columns[column_number]->computeHashInto(0, num_rows, hash_buffer.data(), false);

    selector.resize(num_rows);
    mapToRange(hash_buffer.data(), num_rows, static_cast<UInt32>(num_shards), selector.data());

    VectorWithMemoryTracking<MutableColumns> shard_columns(num_shards);
    for (const auto & column : columns)
    {
        auto split = column->scatter(num_shards, selector);
        for (size_t shard = 0; shard < num_shards; ++shard)
            shard_columns[shard].push_back(std::move(split[shard]));
    }

    ++current_chunk_number;
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        const size_t shard_rows = shard_columns[shard].front()->size();
        has_rows_in_current_chunk[shard] = shard_rows != 0;
        bound_sent[shard] = 0;
        if (shard_rows != 0 && !output_it->isFinished())
            queues[shard].push_back({Chunk(std::move(shard_columns[shard]), shard_rows), current_chunk_number});
    }

    /// The stream is sorted, so the sort columns of the last row bound everything it can send later.
    last_row_shard = selector[num_rows - 1];
    Block new_bound;
    for (auto column_number : sorting_columns)
    {
        const auto & column = header.getByPosition(column_number);
        new_bound.insert({columns[column_number]->cut(num_rows - 1, 1), column.type, column.name});
    }
    bound = std::move(new_bound);
}

}
