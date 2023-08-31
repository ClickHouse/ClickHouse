#pragma once

#include <Processors/Chunk.h>
#include <Interpreters/Context.h>

namespace DB
{

class IOutputChunkGenerator {
public:
    static std::unique_ptr<IOutputChunkGenerator> createCopyRanges(ContextPtr context);
    static std::unique_ptr<IOutputChunkGenerator> createDefault();

    virtual ~IOutputChunkGenerator() = default;

    virtual void onNewChunkArrived(Chunk chunk) = 0;
    virtual void onRowsProcessed(size_t row_count, bool append) = 0;

    virtual Chunk generateChunk() = 0;
};

}
