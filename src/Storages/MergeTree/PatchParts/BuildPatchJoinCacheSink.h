#pragma once
#include <Processors/ISink.h>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>

namespace DB
{

/// Consumes chunks and fills a single PatchJoinCache::Entry via addBlock.
/// Each sink is assigned to one bucket -- no locking needed.
class BuildPatchJoinCacheSink : public ISink
{
public:
    BuildPatchJoinCacheSink(SharedHeader header, PatchJoinCache::EntryPtr entry_);

    String getName() const override { return "BuildPatchJoinCacheSink"; }

protected:
    void consume(Chunk chunk) override;

private:
    PatchJoinCache::EntryPtr entry;
};

}
