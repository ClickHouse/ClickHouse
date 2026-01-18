#include <Processors/Transforms/SortChunksBySequenceNumber.h>

namespace
{

struct ChunkSequenceNumber : public DB::ChunkInfoCloneable<ChunkSequenceNumber>
{
    explicit ChunkSequenceNumber(UInt64 sequence_number_)
        : sequence_number(sequence_number_)
    {
    }

    UInt64 sequence_number = 0;
};

}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void AddSequenceNumber::transform(Chunk & chunk)
{
    // We should extract it first (if present) to support the case of distributed table over another distributed table.
    chunk.getChunkInfos().extract<ChunkSequenceNumber>();
    chunk.getChunkInfos().add(std::make_shared<ChunkSequenceNumber>(++chunk_sequence_number));
}

SortChunksBySequenceNumber::SortChunksBySequenceNumber(const Block & header_, size_t num_inputs_)
    : IProcessor(InputPorts{num_inputs_, header_}, {header_})
    , num_inputs(num_inputs_)
    , chunk_snums(num_inputs, -1)
    , chunks(num_inputs)
    , is_input_finished(num_inputs, false)
{
}

IProcessor::Status SortChunksBySequenceNumber::prepare()
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

    const bool pushed_to_output = tryPushChunk();

    bool need_data = false;
    bool all_finished = true;

    /// Try read anything.
    auto in = inputs.begin();
    for (size_t input_num = 0; input_num < num_inputs; ++input_num, ++in)
    {
        if (in->isFinished())
        {
            is_input_finished[input_num] = true;
            continue;
        }

        if (chunk_snums[input_num] != -1)
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SortChunksBySequenceNumber has read bucket, but couldn't push it.");

    output.finish();
    return Status::Finished;
}

bool SortChunksBySequenceNumber::tryPushChunk()
{
    auto & output = outputs.front();

    bool can_peak_next = false;
    Int64 min_snum = std::numeric_limits<Int64>::max();
    size_t min_snum_idx = 0;
    for (size_t i = 0; i < num_inputs; ++i)
    {
        if (is_input_finished[i] && chunk_snums[i] == -1)
            continue;

        if (chunk_snums[i] == -1)
            return false;

        can_peak_next = true;
        if (chunk_snums[i] < min_snum)
        {
            min_snum = chunk_snums[i];
            min_snum_idx = i;
        }
    }

    if (!can_peak_next)
        return false;

    auto & chunk = chunks[min_snum_idx];
    output.push(std::move(chunk));
    chunk_snums[min_snum_idx] = -1;
    return true;
}

void SortChunksBySequenceNumber::addChunk(Chunk chunk, size_t input)
{
    auto info = chunk.getChunkInfos().get<ChunkSequenceNumber>();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have ChunkSequenceNumber in SortChunksBySequenceNumber.");

    chunk_snums[input] = info->sequence_number;
    chunks[input] = std::move(chunk);
}

}
