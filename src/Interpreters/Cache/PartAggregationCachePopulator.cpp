#include <Interpreters/Cache/PartAggregationCachePopulator.h>

#include <Columns/FilterDescription.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/StorageSnapshot.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Common/Arena.h>
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

    /// On a cold cache this path reads and aggregates every uncached part before the normal query
    /// pipeline exists. Attach the query's process-list element so the per-part reads honour the
    /// same cancellation (`KILL QUERY`) and time limits (`max_execution_time`) as the query they
    /// serve; otherwise warmup would keep doing the expensive work for an already-cancelled query.
    auto process_list_element = context->getProcessListElement();

    for (const auto & part : parts)
    {
        /// Stop populating promptly if the query was cancelled or timed out, instead of warming the
        /// cache for the remaining parts.
        if (process_list_element && (process_list_element->isKilled() || !process_list_element->checkTimeLimitSoft()))
            break;

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
                /// Read only the marks `ReadFromMergeTree` selected for this part (primary key and
                /// skip-index analysis), not the whole part. Passing `std::nullopt` here would scan
                /// and aggregate every row of each selected part on a cold cache, causing severe
                /// first-run regressions for selective `GROUP BY` queries. It is also more robust:
                /// the residual `WHERE`/`PREWHERE` filter is applied below, so reading exactly the
                /// selected ranges reproduces what the normal pipeline aggregates.
                /* mark_ranges = */ part.ranges,
                /* filtered_rows_count = */ nullptr,
                /* apply_deleted_mask = */ true,
                /* read_with_direct_io = */ false,
                /* prefetch = */ false);

            QueryPipeline pipeline(std::move(pipe));
            pipeline.setProcessListElement(process_list_element);
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

                /// `Aggregator` resolves the key and aggregate-argument column positions from
                /// `aggregator_header` once at construction, and `executeOnBlock` then consumes the
                /// passed columns positionally (`columns.at(keys_positions[i])`). The block produced
                /// here is not guaranteed to be in `aggregator_header` order: `columns_to_read` is
                /// sorted, and an intermediate `ExpressionStep` emits columns in its own output order.
                /// Project the block onto `aggregator_header` by name so the positions line up;
                /// otherwise a key and an aggregate argument could be swapped, silently storing wrong
                /// states in the global cache (or throwing on a type mismatch).
                Columns aggregator_columns;
                aggregator_columns.reserve(aggregator_header.columns());
                for (const auto & header_column : aggregator_header)
                    aggregator_columns.push_back(block.getByName(header_column.name).column);

                aggregator.executeOnBlock(
                    std::move(aggregator_columns), 0, block.rows(),
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

            /// Memory retained by the cached states that `block.allocatedBytes()` does not see.
            /// `convertToChunks(final = false)` attaches `data_variants.aggregates_pools` to the
            /// `ColumnAggregateFunction` columns as foreign arenas, and
            /// `ColumnAggregateFunction::allocatedBytes` counts only the pointer array, not those
            /// arenas. The arenas hold the actual state data (almost all of the memory for states
            /// such as `uniqExact` or `groupArray`), so charge them to the cache budget explicitly.
            /// Only meaningful when there are chunks: an empty result block keeps no arenas alive.
            size_t state_arena_bytes = 0;

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

                for (const auto & pool : data_variants.aggregates_pools)
                    state_arena_bytes += pool->allocatedBytes();
            }

            size_t cached_rows = result_block.rows();
            cache->set(key, std::move(result_block), state_arena_bytes);

            LOG_DEBUG(log, "Cached aggregation state for part {} ({} rows)",
                part.data_part->name, cached_rows);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to populate cache for part " + part.data_part->name);
        }
    }
}

}
