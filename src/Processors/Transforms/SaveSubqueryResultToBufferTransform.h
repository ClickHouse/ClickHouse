#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

struct ChunkBuffer;
using ChunkBufferPtr = std::shared_ptr<ChunkBuffer>;

class SaveSubqueryResultToBufferTransform : public ISimpleTransform
{
public:
    SaveSubqueryResultToBufferTransform(
        SharedHeader header_,
        ChunkBufferPtr chunk_buffer_,
        const std::vector<size_t> & columns_to_save_indices_
    );

    String getName() const override { return "SaveSubqueryResultToBuffer"; }
    void transform(Chunk & chunk) override;

private:
    ChunkBufferPtr chunk_buffer;
    std::vector<size_t> columns_to_save_indices;
};

}
