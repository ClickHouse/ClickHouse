#pragma once

#include <Processors/Chunk.h>
#include <Interpreters/Context.h>

namespace DB
{

/// This interface is meant to be used by the SinkToStorage processor
/// SinkToStorage delegates on it the creation of the data chunk that will deliver to the next stages of the query pipeline
/// Default implementation (createDefault() factory method) just forwards everything that it receives
class IOutputChunkGenerator
{
public:
    static std::unique_ptr<IOutputChunkGenerator> createCopyRanges(bool deduplicate_later);
    static std::unique_ptr<IOutputChunkGenerator> createDefault();

    virtual ~IOutputChunkGenerator() = default;

    virtual void onNewChunkArrived(Chunk chunk) = 0;
    virtual void onRowsProcessed(size_t row_count, bool append) = 0;

    virtual Chunk generateChunk() = 0;
};

}
