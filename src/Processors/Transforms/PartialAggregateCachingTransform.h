#pragma once

#include <Processors/IAccumulatingTransform.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Cache/PartialAggregateCache.h>
#include <Parsers/IASTHash.h>
#include <Core/Block.h>

#include <unordered_map>

namespace DB
{

/// Transform that implements per-part partial aggregate caching.
///
/// This transform:
/// 1. Receives chunks with PartialAggregateInfo attached (part_name, mutation_version)
/// 2. Groups chunks by part
/// 3. For each part:
///    - Checks cache for existing partial aggregate
///    - If cache hit: uses cached block
///    - If cache miss: aggregates all chunks for that part, stores in cache
/// 4. Merges all partial aggregates and outputs final result
///
/// This is a drop-in replacement for AggregatingTransform when partial aggregate caching is enabled.
class PartialAggregateCachingTransform : public IAccumulatingTransform
{
public:
    PartialAggregateCachingTransform(
        const Block & header_,
        Aggregator::Params params_,
        IASTHash query_hash_,
        PartialAggregateCachePtr cache_,
        bool final_);

    String getName() const override { return "PartialAggregateCachingTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    Aggregator::Params params;
    IASTHash query_hash;
    PartialAggregateCachePtr cache;
    bool final;

    /// Group chunks by part
    struct PartData
    {
        String part_name;
        UInt64 mutation_version = 0;
        UUID table_uuid = UUIDHelpers::Nil;
        Chunks chunks;
    };

    std::unordered_map<String, PartData> parts_data;

    /// After merging, stores final blocks to output
    std::list<Block> merged_blocks;
    std::list<Block>::iterator output_iterator;
    bool generate_started = false;

    LoggerPtr log = getLogger("PartialAggregateCachingTransform");
};

}

