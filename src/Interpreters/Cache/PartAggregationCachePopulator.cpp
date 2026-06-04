#include <Interpreters/Cache/PartAggregationCachePopulator.h>

#include <Columns/FilterDescription.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ActionsDAG.h>
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
    const String & table_id,
    const RangesInDataParts & parts,
    const Aggregator::Params & params,
    const Block & aggregator_header,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    const std::vector<IntermediateStepAction> & intermediate_actions)
{
    auto log = getLogger("PartAggregationCachePopulator");

    /// Collect all columns needed: aggregation keys + aggregate args + columns required by intermediate actions.
    Names columns_to_read;
    for (const auto & key : params.keys)
        columns_to_read.push_back(key);
    for (const auto & agg : params.aggregates)
        for (const auto & arg : agg.argument_names)
            columns_to_read.push_back(arg);
    for (const auto & action : intermediate_actions)
        for (const auto & col : action.actions->getRequiredColumnsWithTypes())
            columns_to_read.push_back(col.name);

    std::sort(columns_to_read.begin(), columns_to_read.end());
    columns_to_read.erase(std::unique(columns_to_read.begin(), columns_to_read.end()), columns_to_read.end());

    for (const auto & part : parts)
    {
        PartAggregationCache::Key key{query_hash, table_id, part.data_part->name};

        if (cache->get(key))
            continue;

        try
        {
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

            QueryPipeline pipeline(std::move(pipe));
            PullingPipelineExecutor executor(pipeline);

            auto params_copy = params;
            params_copy.only_merge = false;

            Aggregator aggregator(aggregator_header, params_copy);
            AggregatedDataVariants data_variants;
            ColumnRawPtrs key_columns(params.keys_size);
            Aggregator::AggregateColumns aggregate_columns(params.aggregates_size);
            bool no_more_keys = false;

            Block block;
            while (executor.pull(block))
            {
                if (block.rows() == 0)
                    continue;

                /// Apply intermediate steps (expressions and filters) to transform
                /// the block from ReadFromMergeTree format to AggregatingStep input format.
                for (const auto & action : intermediate_actions)
                {
                    action.actions->execute(block);

                    if (!action.filter_column_name.empty())
                    {
                        const auto & filter_col = block.getByName(action.filter_column_name).column;
                        FilterDescription filter_desc(*filter_col);

                        if (filter_desc.countBytesInFilter() == 0)
                        {
                            block = aggregator_header.cloneEmpty();
                            break;
                        }

                        Block filtered_block;
                        for (const auto & col : block)
                            filtered_block.insert({filter_desc.filter(*col.column, -1), col.type, col.name});

                        block = std::move(filtered_block);

                        /// Only erase the filter column if the original FilterStep was configured
                        /// to remove it. Otherwise downstream steps may still need it (e.g.
                        /// `SELECT f, sum(v) FROM t WHERE f GROUP BY f`).
                        if (action.remove_filter_column && block.has(action.filter_column_name))
                            block.erase(action.filter_column_name);
                    }
                }

                if (block.rows() == 0)
                    continue;

                aggregator.executeOnBlock(
                    block.getColumns(), 0, block.rows(),
                    data_variants, key_columns, aggregate_columns, no_more_keys);
            }

            /// `executeOnBlock` flushes part of `data_variants` to temporary files once the part's
            /// aggregation exceeds `max_bytes_before_external_group_by`. Unlike `AggregatingTransform`,
            /// this populator does not merge that spilled data back, so `convertToChunks` below would
            /// only return the in-memory remainder. Caching that would store partial aggregates for
            /// the part. Skip the part entirely in that case (fail-closed); the main pipeline will
            /// aggregate it normally.
            if (aggregator.hasTemporaryData())
            {
                LOG_DEBUG(log, "Part {} spilled to temporary files during cache population; not caching it",
                    part.data_part->name);
                continue;
            }

            auto chunks = aggregator.convertToChunks(data_variants, /* final = */ false);

            auto intermediate_header = Aggregator::Params::getHeader(
                aggregator_header, params.only_merge, params.keys, params.aggregates, /* final = */ false);

            /// Always cache an entry — even when `chunks` is empty — so that parts that produce
            /// zero rows after filtering still register as cache hits on subsequent queries.
            /// Otherwise selective predicates over many historical parts would always re-scan.
            Block result_block;
            if (chunks.empty())
            {
                result_block = intermediate_header.cloneEmpty();
            }
            else
            {
                result_block = intermediate_header.cloneWithColumns(chunks.front().chunk.detachColumns());
                for (auto it = std::next(chunks.begin()); it != chunks.end(); ++it)
                {
                    auto cols = it->chunk.detachColumns();
                    for (size_t i = 0; i < result_block.columns(); ++i)
                    {
                        auto mut_col = IColumn::mutate(std::move(result_block.getByPosition(i).column));
                        mut_col->insertRangeFrom(*cols[i], 0, cols[i]->size());
                        result_block.getByPosition(i).column = std::move(mut_col);
                    }
                }
            }

            size_t cached_rows = result_block.rows();
            cache->set(key, std::move(result_block));

            LOG_DEBUG(log, "Cached aggregation state for part {} ({} rows)",
                part.data_part->name, cached_rows);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to populate cache for part " + part.data_part->name);
        }
    }

    (void)context;
}

}
