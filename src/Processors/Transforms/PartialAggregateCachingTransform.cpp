#include <Processors/Transforms/PartialAggregateCachingTransform.h>
#include <Interpreters/Cache/PartialAggregateInfo.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/logger_useful.h>

namespace DB
{

PartialAggregateCachingTransform::PartialAggregateCachingTransform(
    const Block & header_,
    Aggregator::Params params_,
    IASTHash query_hash_,
    PartialAggregateCachePtr cache_,
    bool final_)
    : IAccumulatingTransform(header_, Aggregator(header_, params_).getHeader(final_))
    , params(std::move(params_))
    , query_hash(query_hash_)
    , cache(std::move(cache_))
    , final(final_)
{
}

void PartialAggregateCachingTransform::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    /// Get part info from chunk
    auto part_info = chunk.getChunkInfos().get<PartialAggregateInfo>();
    if (!part_info)
    {
        /// No part info - use a default key (won't benefit from caching)
        auto & data = parts_data["__no_part_info__"];
        data.part_name = "__no_part_info__";
        data.chunks.push_back(std::move(chunk));
        return;
    }

    /// Group by part name
    auto & data = parts_data[part_info->part_name];
    if (data.part_name.empty())
    {
        data.part_name = part_info->part_name;
        data.mutation_version = part_info->part_mutation_version;
        data.table_uuid = part_info->table_uuid;
    }
    data.chunks.push_back(std::move(chunk));
}

Chunk PartialAggregateCachingTransform::generate()
{
    if (!generate_started)
    {
        generate_started = true;

        LOG_DEBUG(log, "Processing {} parts for partial aggregate caching", parts_data.size());

        /// Collect all partial aggregates (from cache or freshly computed)
        std::vector<Block> partial_aggregates;

        for (auto & [part_key, part_data] : parts_data)
        {
            PartialAggregateCache::Key cache_key{
                .query_hash = query_hash,
                .part_name = part_data.part_name,
                .part_mutation_version = part_data.mutation_version
            };

            /// Try cache lookup (only for parts with valid info, not __no_part_info__)
            if (cache && part_data.part_name != "__no_part_info__")
            {
                auto cached = cache->get(cache_key);
                if (cached)
                {
                    LOG_DEBUG(log, "Cache hit for part {}", part_data.part_name);
                    partial_aggregates.push_back(std::move(*cached));
                    continue;
                }
            }

            /// Cache miss - aggregate this part's data
            LOG_DEBUG(log, "Cache miss for part {}, aggregating {} chunks",
                      part_data.part_name, part_data.chunks.size());

            Aggregator aggregator(getInputPort().getHeader(), params);
            AggregatedDataVariants variants;

            ColumnRawPtrs key_columns(params.keys_size);
            Aggregator::AggregateColumns aggregate_columns(params.aggregates_size);

            for (auto & chunk : part_data.chunks)
            {
                const auto num_rows = chunk.getNumRows();
                aggregator.executeOnBlock(
                    chunk.detachColumns(), 0, num_rows, variants,
                    key_columns, aggregate_columns, /*no_more_keys=*/false);
            }

            /// Convert to blocks (non-final for caching, we'll finalize later)
            auto blocks = aggregator.convertToBlocks(variants, /*final=*/false);

            for (auto & block : blocks)
            {
                if (block.rows() == 0)
                    continue;

                /// Store in cache (only for parts with valid info)
                if (cache && part_data.part_name != "__no_part_info__")
                {
                    cache->put(cache_key, block);
                }

                partial_aggregates.push_back(std::move(block));
            }
        }

        parts_data.clear();
        LOG_DEBUG(log, "Generated {} partial aggregates, now merging", partial_aggregates.size());

        /// Merge all partial aggregates
        if (!partial_aggregates.empty())
        {
            Aggregator aggregator(getInputPort().getHeader(), params);
            ManyAggregatedDataVariants many_data(1);
            many_data[0] = std::make_shared<AggregatedDataVariants>();

            /// Convert blocks to bucket_to_blocks format for mergeBlocks
            Aggregator::BucketToBlocks bucket_to_blocks;
            for (auto & block : partial_aggregates)
            {
                bucket_to_blocks[-1].push_back(std::move(block));
            }

            /// Merge all partial aggregates
            std::atomic<bool> is_cancelled{false};
            aggregator.mergeBlocks(std::move(bucket_to_blocks), *many_data[0], is_cancelled);

            /// Convert merged result to final blocks
            merged_blocks = aggregator.convertToBlocks(*many_data[0], final);
        }

        output_iterator = merged_blocks.begin();
        LOG_DEBUG(log, "Merged into {} output blocks", merged_blocks.size());
    }

    /// Output merged blocks one by one
    if (output_iterator != merged_blocks.end())
    {
        auto & block = *output_iterator;
        ++output_iterator;

        auto num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    return {};
}

}

