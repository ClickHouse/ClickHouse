#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/Formats/IInputFormat.h>

namespace DB::Iceberg
{

/// Transform that ensures ChunkInfoRowNumbers is added to chunks in the pipeline
class RowNumbersTransform : public ISimpleTransform
{
public:
    explicit RowNumbersTransform(const SharedHeader & header_)
        : ISimpleTransform(header_, header_, false)
    {
    }

    String getName() const override { return "RowNumbersTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        if (!chunk.getChunkInfos().has<ChunkInfoRowNumbers>())
        {
            chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(current_row_num));
        }
        current_row_num += chunk.getNumRows();
    }

private:
    size_t current_row_num = 0;
};