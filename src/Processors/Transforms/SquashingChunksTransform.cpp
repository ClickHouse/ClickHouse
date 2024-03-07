#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Processors/IProcessor.h>
#include "Common/logger_useful.h"

namespace DB
{

SquashingChunksTransform::SquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header, header, false)
    , squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SquashingChunksTransform::onConsume(Chunk chunk)
{
    LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: !finished, hasInfo: {}", reinterpret_cast<void*>(this), chunk.hasChunkInfo());
    if (auto block = squashing.add(std::move(chunk)))
    {
        cur_chunk.setColumns(block.getColumns(), block.rows());
    }
}

SquashingChunksTransform::GenerateResult SquashingChunksTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

void SquashingChunksTransform::onFinish()
{
    auto block = squashing.add({});
    LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: finished, structure of block: {}", reinterpret_cast<void*>(this), block.dumpStructure());
    finish_chunk.setColumns(block.getColumns(), block.rows());
    LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: finished, hasInfo: {}", reinterpret_cast<void*>(this), finish_chunk.hasChunkInfo());
}

void SquashingChunksTransform::work()
{
    if (stage == Stage::Exception)
    {
        data.chunk.clear();
        ready_input = false;
        return;
    }

    ExceptionKeepingTransform::work();
    if (finish_chunk)
    {
        data.chunk = std::move(finish_chunk);
        ready_output = true;
    }
}

SimpleSquashingChunksTransform::SimpleSquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, [[maybe_unused]] bool skip_empty_chunks_)
    : ISimpleTransform(header, header, false), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::transform(Chunk & chunk)
{
    if (!finished)
    {
        LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: !finished, hasInfo: {}", reinterpret_cast<void*>(this), chunk.hasChunkInfo());
        if (auto block = squashing.add(std::move(chunk)))
            chunk.setColumns(block.getColumns(), block.rows());
    }
    else
    {
        LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: finished, hasInfo: {}", reinterpret_cast<void*>(this), chunk.hasChunkInfo());
        auto block = squashing.add({});
        chunk.setColumns(block.getColumns(), block.rows());
    }
}

IProcessor::Status SimpleSquashingChunksTransform::prepare()
{
    if (!finished && input.isFinished())
    {
        finished = true;
        return Status::Ready;
    }
    return ISimpleTransform::prepare();
}

//maybe it makes sense to pass not the IProcessor entity, but the SimpleTransform? anyway we have one input and one output
ProcessorSquashingTransform::ProcessorSquashingTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, [[maybe_unused]]size_t num_ports)
    : IProcessor(InputPorts(1, header), OutputPorts(1, header)), squashing(min_block_size_rows, min_block_size_bytes)
{
}

IProcessor::Status ProcessorSquashingTransform::prepare()
{
    Status status = Status::Ready;

    while (status == Status::Ready)
    {
        status = !has_data ? prepareConsume()
                           : prepareGenerate();
    }

    return status;
}

IProcessor::Status ProcessorSquashingTransform::prepareConsume()
{
    LOG_TRACE(getLogger("balancing"), "prepareConsume");
    for (auto & input : getInputPorts())
    {
        bool all_finished = true;
        for (auto & output : outputs)
        {
            if (output.isFinished())
                continue;

            all_finished = false;
        }

        if (all_finished)
        {
            input.close();
            return Status::Finished;
        }

        if (input.isFinished())
        {
            for (auto & output : outputs)
                output.finish();

            return Status::Finished;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        chunk = input.pull();
        has_data = true;
        was_output_processed.assign(outputs.size(), false);
        transform(chunk);
        // if (chunk)
        //     chunks.push_back(std::move(chunk));
    }
    return Status::Ready;
}

void ProcessorSquashingTransform::transform(Chunk & chunk_)
{
    // [[maybe_unused]]const auto * agg_info = typeid_cast<const ChunksToSquash *>(chunk.getChunkInfo().get());
    // if (agg_info)
    // {
    //     std::cerr << "Beginning of SquashingTransform: size of one group: " << agg_info->chunks.size() << std::endl;
    //     if (!agg_info->chunks.empty())
    //         std::cerr << "!group is not empty, first column: " << agg_info->chunks[0].dumpStructure() << std::endl;
    // }
    LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: Struct of input chunk: {}", reinterpret_cast<void*>(this), chunk_.dumpStructure());
    if (!finished)
    {
        LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: !finished, hasInfo: {}", reinterpret_cast<void*>(this), chunk_.hasChunkInfo());
        if (auto block = squashing.add(std::move(chunk_)))
            chunk_.setColumns(block.getColumns(), block.rows());
    }
    else
    {
        LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: finished, hasInfo: {}", reinterpret_cast<void*>(this), chunk_.hasChunkInfo());
        auto block = squashing.add({});
        chunk_.setColumns(block.getColumns(), block.rows());
    }
}

IProcessor::Status ProcessorSquashingTransform::prepareGenerate()
{
    LOG_TRACE(getLogger("squashingProcessor"), "prepareGenerate");
    bool all_outputs_processed = true;

    size_t chunk_number = 0;
    for (auto &output : getOutputPorts())
    {
        auto & was_processed = was_output_processed[chunk_number];
        ++chunk_number;

        if (was_processed)
            continue;

        if (output.isFinished())
            continue;

        if (!output.canPush())
        {
            all_outputs_processed = false;
            continue;
        }

        LOG_TRACE(getLogger("squashingProcessor"), "chunk struct: {}", chunk.dumpStructure());
        output.push(chunk.clone());
        was_processed = true;
    }

    if (all_outputs_processed)
    {
        has_data = false;
        return Status::Ready;
    }
    return Status::PortFull;
}
}
