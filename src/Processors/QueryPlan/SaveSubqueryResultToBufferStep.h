#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

using ColumnIdentifier = std::string;
using ColumnIdentifiers = std::vector<ColumnIdentifier>;

struct ChunkBuffer;
using ChunkBufferPtr = std::shared_ptr<ChunkBuffer>;

/** Save data to ChunkBuffer to be read later by ReadFromCommonBufferStep.
  * Used to implement result buffering for common subplan.
  */
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
