#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/sortBlock.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/AggregatingTransform.h>

#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/// Has several inputs and single output.
/// Read from inputs merged buckets with aggregated data, sort them by bucket number and block number.
/// Presumption: inputs return chunks with increasing bucket and block number, there is at most one chunk with the given bucket and block number.
class SortingAggregatedForMemoryBoundMergingTransform : public IProcessor
{
public:
    explicit SortingAggregatedForMemoryBoundMergingTransform(const Block & header_, size_t num_inputs_)
        : IProcessor(InputPorts(num_inputs_, header_), {header_})
        , header(header_)
        , num_inputs(num_inputs_)
        , last_chunk_id(num_inputs, {std::numeric_limits<Int32>::min(), 0})
        , is_input_finished(num_inputs, false)
    {
    }

    String getName() const override { return "SortingAggregatedForMemoryBoundMergingTransform"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (output.isFinished())
        {
            for (auto & input : inputs)
                input.close();

            return Status::Finished;
        }

        if (!output.canPush())
        {
            for (auto & input : inputs)
                input.setNotNeeded();

            return Status::PortFull;
        }

        /// Push if have chunk that is the next in order
        bool pushed_to_output = tryPushChunk();

        bool need_data = false;
        bool all_finished = true;

        /// Try read new chunk
        auto in = inputs.begin();
        for (size_t input_num = 0; input_num < num_inputs; ++input_num, ++in)
        {
            if (in->isFinished())
            {
                is_input_finished[input_num] = true;
                continue;
            }

            /// We want to keep not more than `num_inputs` chunks in memory (and there will be only a single chunk with the given (bucket_id, chunk_num)).
            const bool bucket_from_this_input_still_in_memory = chunks.contains(last_chunk_id[input_num]);
            if (bucket_from_this_input_still_in_memory)
            {
                all_finished = false;
                continue;
            }

            in->setNeeded();

            if (!in->hasData())
            {
                need_data = true;
                all_finished = false;
                continue;
            }

            auto chunk = in->pull();
            addChunk(std::move(chunk), input_num);

            if (in->isFinished())
            {
                is_input_finished[input_num] = true;
            }
            else
            {
                /// If chunk was pulled, then we need data from this port.
                need_data = true;
                all_finished = false;
            }
        }

        if (pushed_to_output)
            return Status::PortFull;

        if (tryPushChunk())
            return Status::PortFull;

        if (need_data)
            return Status::NeedData;

        if (!all_finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SortingAggregatedForMemoryBoundMergingTransform has read bucket, but couldn't push it.");

        if (overflow_chunk)
        {
            output.push(std::move(overflow_chunk));
            return Status::PortFull;
        }

        output.finish();
        return Status::Finished;
    }

private:
    bool tryPushChunk()
    {
        auto & output = outputs.front();

        if (chunks.empty())
            return false;

        /// Chunk with min id
        auto it = chunks.begin();
        auto current_chunk_id = it->first;

        /// Check if it is actually next in order
        for (size_t input = 0; input < num_inputs; ++input)
            if (!is_input_finished[input] && last_chunk_id[input] < current_chunk_id)
                return false;

        output.push(std::move(it->second));
        chunks.erase(it);
        return true;
    }

    void addChunk(Chunk chunk, size_t from_input)
    {
        if (!chunk.hasRows())
            return;

        const auto & agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>();
        if (!agg_info)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Chunk should have AggregatedChunkInfo in SortingAggregatedForMemoryBoundMergingTransform.");

        Int32 bucket_id = agg_info->bucket_num;
        bool is_overflows = agg_info->is_overflows;
        UInt64 chunk_num = agg_info->chunk_num;

        if (is_overflows)
            overflow_chunk = std::move(chunk);
        else
        {
            const auto chunk_id = ChunkId{bucket_id, chunk_num};
            if (chunks.contains(chunk_id))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "SortingAggregatedForMemoryBoundMergingTransform already got bucket with number {}",
                    bucket_id);
            }

            chunks[chunk_id] = std::move(chunk);
            last_chunk_id[from_input] = chunk_id;
        }
    }

    struct ChunkId
    {
        Int32 bucket_id;
        UInt64 chunk_num;

        bool operator<(const ChunkId & other) const
        {
            return std::make_pair(bucket_id, chunk_num) < std::make_pair(other.bucket_id, other.chunk_num);
        }
    };

    Block header;
    size_t num_inputs;

    std::vector<ChunkId> last_chunk_id;
    std::vector<bool> is_input_finished;
    std::map<ChunkId, Chunk> chunks;
    Chunk overflow_chunk;
};

}
