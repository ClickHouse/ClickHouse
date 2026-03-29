#include <Interpreters/Cache/PartAggregationCachePopulator.h>

#include <Interpreters/Aggregator.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/StorageSnapshot.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Common/logger_useful.h>


namespace DB
{

void populatePartAggregationCache(
    const PartAggregationCachePtr & cache,
    const IASTHash & query_hash,
    const RangesInDataParts & parts,
    const Aggregator::Params & params,
    const Block & input_header,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    auto log = getLogger("PartAggregationCachePopulator");

    /// Determine which columns we need to read: GROUP BY keys + aggregate function arguments.
    Names columns_to_read;
    for (const auto & key : params.keys)
        columns_to_read.push_back(key);
    for (const auto & agg : params.aggregates)
        for (const auto & arg : agg.argument_names)
            columns_to_read.push_back(arg);

    /// Deduplicate column names.
    std::sort(columns_to_read.begin(), columns_to_read.end());
    columns_to_read.erase(std::unique(columns_to_read.begin(), columns_to_read.end()), columns_to_read.end());

    for (const auto & part : parts)
    {
        PartAggregationCache::Key key{query_hash, part.data_part->name};

        /// Skip if already cached.
        if (cache->get(key))
            continue;

        try
        {
            /// Create a sequential source for this part.
            auto alter_conversions = std::make_shared<AlterConversions>();

            auto pipe = createMergeTreeSequentialSource(
                MergeTreeSequentialSourceType::Merge,
                storage,
                storage_snapshot,
                part,
                alter_conversions,
                /* merged_part_offsets = */ nullptr,
                columns_to_read,
                /* mark_ranges = */ std::nullopt,
                /* filtered_rows_count = */ nullptr,
                /* apply_deleted_mask = */ true,
                /* read_with_direct_io = */ false,
                /* prefetch = */ false);

            /// Create a pipeline and executor.
            QueryPipeline pipeline(std::move(pipe));
            PullingPipelineExecutor executor(pipeline);

            /// Create an aggregator for this part.
            auto params_copy = params;
            params_copy.only_merge = false;

            Aggregator aggregator(input_header, params_copy);
            AggregatedDataVariants data_variants;
            ColumnRawPtrs key_columns(params.keys_size);
            Aggregator::AggregateColumns aggregate_columns(params.aggregates_size);
            bool no_more_keys = false;

            /// Read and aggregate all blocks from the part.
            Block block;
            while (executor.pull(block))
            {
                if (block.rows() == 0)
                    continue;
                aggregator.executeOnBlock(block, data_variants, key_columns, aggregate_columns, no_more_keys);
            }

            /// Convert to intermediate blocks (final=false) and store in cache.
            auto blocks = aggregator.convertToBlocks(data_variants, /* final = */ false);

            if (!blocks.empty())
            {
                /// Merge all blocks into a single block for caching.
                Block result_block = blocks.front();
                for (auto it = std::next(blocks.begin()); it != blocks.end(); ++it)
                {
                    for (size_t i = 0; i < result_block.columns(); ++i)
                    {
                        auto mut_col = IColumn::mutate(std::move(result_block.getByPosition(i).column));
                        mut_col->insertRangeFrom(*it->getByPosition(i).column, 0, it->rows());
                        result_block.getByPosition(i).column = std::move(mut_col);
                    }
                }

                size_t cached_rows = result_block.rows();
                cache->set(key, std::move(result_block));

                LOG_DEBUG(log, "Cached aggregation state for part {} ({} rows)",
                    part.data_part->name, cached_rows);
            }
        }
        catch (...)
        {
            /// If caching fails for a part, log and continue.
            tryLogCurrentException(log, "Failed to populate cache for part " + part.data_part->name);
        }
    }

    (void)context;
}

}
