#include <Storages/MergeTree/PatchParts/BuildPatchJoinCacheSink.h>
#include <Columns/ColumnSparse.h>
#include <Processors/Port.h>

namespace DB
{

BuildPatchJoinCacheSink::BuildPatchJoinCacheSink(SharedHeader header, PatchJoinCache::EntryPtr entry_)
    : ISink(std::move(header))
    , entry(std::move(entry_))
{
}

void BuildPatchJoinCacheSink::consume(Chunk chunk)
{
    auto columns = chunk.detachColumns();
    for (auto & col : columns)
        col = removeSpecialRepresentations(col);
    Block block = getPort().getHeader().cloneWithColumns(columns);
    entry->addBlock(std::move(block));
}

}
