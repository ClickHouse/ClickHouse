#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/ITransformingStep.h>

#include <mutex>
#include <queue>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

using ColumnIdentifier = std::string;
using ColumnIdentifiers = std::vector<ColumnIdentifier>;


struct ChunkBuffer
{
    void append(Chunk && chunk)
    {
        std::lock_guard lock(mutex);
        chunks.push(std::move(chunk));
    }

    Chunk extractNext()
    {
        std::lock_guard lock(mutex);

        if (!isReady())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Trying to extract chunk from ChunkBuffer before all inputs are finished (unfinished inputs: {})",
                unfinished_inputs);

        if (chunks.empty())
            return {};

        Chunk chunk = std::move(chunks.front());
        chunks.pop();
        return chunk;
    }

    void onInputFinish()
    {
        std::lock_guard lock(mutex);
        --unfinished_inputs;
    }

    bool isReady() const { return unfinished_inputs == 0; }

    void setInputsNumber(size_t num) { unfinished_inputs = num; }

private:
    std::mutex mutex;
    std::queue<Chunk> chunks;
    size_t unfinished_inputs = 1; /// Initialized to 1 to avoid being ready before setting the actual number of inputs
};

using ChunkBufferPtr = std::shared_ptr<ChunkBuffer>;

class SaveSubqueryResultToBufferStep : public ITransformingStep
{
public:
    SaveSubqueryResultToBufferStep(
        const SharedHeader & header_,
        ColumnIdentifiers columns_to_save_,
        ChunkBufferPtr chunk_buffer_);

    String getName() const override { return "SaveSubqueryResultToBuffer"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

private:
    ColumnIdentifiers columns_to_save;
    ChunkBufferPtr chunk_buffer;
};

}
