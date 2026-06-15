#include <Processors/QueryPlan/Optimizations/optimizeUsePartAggregationCache.h>

#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <Interpreters/Cache/PartAggregationCache.h>
#include <Interpreters/Cache/PartAggregationCachePopulator.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/ColumnsDescription.h>
#include <Common/SipHash.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/PartAggregationCacheSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_part_aggregation_cache;
    extern const SettingsBool enable_reads_from_part_aggregation_cache;
    extern const SettingsBool enable_writes_to_part_aggregation_cache;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsUInt64 max_bytes_to_read_leaf;
}

namespace QueryPlanOptimizations
{

static QueryPlan::Node * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (typeid_cast<ReadFromMergeTree *>(step))
        return &node;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front());

    return nullptr;
}

/// Collect all ExpressionStep/FilterStep actions between AggregatingStep and ReadFromMergeTree.
/// Returned in bottom-up order (ReadFromMergeTree → AggregatingStep).
static std::vector<IntermediateStepAction> collectIntermediateActions(QueryPlan::Node & node)
{
    std::vector<IntermediateStepAction> actions;
    QueryPlan::Node * current = &node;

    while (current)
    {
        IQueryPlanStep * step = current->step.get();

        if (typeid_cast<ReadFromMergeTree *>(step))
            break;

        if (auto * expr = typeid_cast<ExpressionStep *>(step))
            actions.push_back({std::make_shared<ExpressionActions>(expr->getExpression().clone()), {}, false});
        else if (auto * filter = typeid_cast<FilterStep *>(step))
            actions.push_back({std::make_shared<ExpressionActions>(filter->getExpression().clone()),
                filter->getFilterColumnName(), filter->removesFilterColumn()});

        if (current->children.size() != 1)
            break;
        current = current->children.front();
    }

    std::reverse(actions.begin(), actions.end());
    return actions;
}

void optimizeUsePartAggregationCache(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    bool is_explain)
{
    /// `EXPLAIN` is intended to perform only static planning. This optimization can call
    /// `populatePartAggregationCache`, which reads part data and mutates the global cache while
    /// building the plan, so running it under `EXPLAIN` would scan table data and change the
    /// behavior of subsequent queries. Skip it entirely in that case.
    if (is_explain)
        return;

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return;

    if (node.children.size() != 1)
        return;

    if (aggregating->isGroupingSets() || aggregating->inOrder())
        return;

    if (!aggregating->getFinal())
        return;

    if (!aggregating->canUseProjection())
        return;

    QueryPlan::Node * reading_node = findReadingStep(*node.children.front());
    if (!reading_node)
        return;

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());
    if (!reading)
        return;

    auto context = reading->getContext();
    if (!context)
        return;

    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::allow_experimental_part_aggregation_cache])
        return;

    auto cache = context->getPartAggregationCache();
    if (!cache)
        return;

    /// A cache configured with size 0 is disabled (`clickhouse-local` creates such a dummy cache,
    /// and a server can set `part_aggregation_cache.max_size_in_bytes = 0`). In that state every
    /// `set` is rejected, so running the optimization would read and aggregate each uncached part
    /// only to discard the result and produce no cache entries — doubling the work of an eligible
    /// `GROUP BY`. Skip the optimization entirely when the cache is disabled.
    if (!cache->isEnabled())
        return;

    /// Apply the same `ReadFromMergeTree` eligibility gates as aggregate projections. The
    /// populator reads raw per-part rows with `createMergeTreeSequentialSource` and therefore
    /// cannot preserve read semantics such as `FINAL`, `SAMPLE`, read-in-order, or pending
    /// mutations/patch parts. Caching under those modes would store pre-`FINAL` (or otherwise
    /// incomplete) aggregate states and return incorrect results. Parallel-replica reads need a
    /// stronger guard than this helper provides and are rejected separately below.
    if (!canUseProjectionForReadingStep(reading))
        return;

    /// `canUseProjectionForReadingStep` still permits parallel-replica reads when projection support
    /// is enabled (`parallel_replicas_support_projection`). Aggregate projections then rely on the
    /// initiator coordinating part assignment so each part is read by exactly one replica. This cache
    /// path does not preserve that contract: for a warm cache it builds a local
    /// `PartAggregationCacheSource` from every selected part on every replica (see below), so two
    /// replicas would each emit the cached state for the same part and the final merge would
    /// double-count `sum`/`count`. Reject parallel-replica reads (fail-closed).
    if (reading->isParallelReadingEnabled())
        return;

    /// Read limits (`max_rows_to_read`, `max_bytes_to_read`, and their `_leaf` variants) are enforced
    /// by the `ReadFromMergeTree` storage read over raw rows. This optimization replaces that read
    /// with `PartAggregationCacheSource`, which emits already-aggregated state rows, and the
    /// populator's own per-part reads do not feed these limits either. As a result an eligible
    /// `GROUP BY` over a part with more rows than `max_rows_to_read` would silently return a cached
    /// aggregate instead of throwing or honoring `read_overflow_mode`. Skip the optimization when any
    /// read limit is active (fail-closed).
    if (settings[Setting::max_rows_to_read] || settings[Setting::max_bytes_to_read]
        || settings[Setting::max_rows_to_read_leaf] || settings[Setting::max_bytes_to_read_leaf])
        return;

    /// Row-level security filters are not part of the cache key and are not applied by the
    /// populator, while the cache is global. Without this guard a query running under a
    /// permissive row policy could populate entries that a later query under a restrictive
    /// policy reuses, bypassing the policy. Reject such queries (fail-closed).
    if (reading->getRowLevelFilter())
        return;

    /// `canUseProjectionForReadingStep` rejects data mutations and patch parts, but not lightweight
    /// deletes or pending `ALTER` (data/metadata) mutations. The cache key is only `{table_id,
    /// part_name}`, and neither the lightweight delete mask version nor pending `ALTER` conversions
    /// are represented in it or applied by the populator (which builds an empty `AlterConversions`).
    /// An entry cached before such a mutation would be reused with a stale mask/schema. Reject these
    /// cases (fail-closed).
    auto mutations_snapshot = reading->getMutationsSnapshot();
    if (mutations_snapshot
        && (mutations_snapshot->hasLightweightDeletedMask()
            || mutations_snapshot->hasAlterMutations()
            || mutations_snapshot->hasMetadataMutations()))
        return;

    const auto & parts = reading->getParts();
    if (parts.empty())
        return;

    const auto & params = aggregating->getParams();

    /// `group_by_overflow_mode` limits (`max_rows_to_group_by`) and `overflow_row` apply per
    /// aggregation invocation. The populator aggregates each part independently, so the limit would
    /// be applied once per part and then the per-part states merged, producing more keys than the
    /// query limit (or different overflow rows) than the normal single-pass aggregation. Skip the
    /// optimization in that case (fail-closed).
    if (params.max_rows_to_group_by != 0 || params.overflow_row)
        return;

    /// Note: `params.max_bytes_before_external_group_by` cannot be used as a gate here. It defaults
    /// to a non-zero threshold derived from `max_bytes_ratio_before_external_group_by` (a fraction
    /// of available memory) for essentially every query, so gating on it would disable the
    /// optimization entirely. Actual spilling is instead detected per part inside the populator
    /// (see `populatePartAggregationCache`), which skips caching any part that spilled to disk.

    auto intermediate_actions = collectIntermediateActions(*node.children.front());

    /// If ReadFromMergeTree has a prewhere/where filter, convert it to an IntermediateStepAction
    /// so the populator applies it when reading data.
    auto prewhere = reading->getPrewhereInfo();
    const ActionsDAG * filter_dag_for_hash = nullptr;
    if (prewhere)
    {
        filter_dag_for_hash = &prewhere->prewhere_actions;
        intermediate_actions.insert(intermediate_actions.begin(), IntermediateStepAction{
            std::make_shared<ExpressionActions>(prewhere->prewhere_actions.clone()),
            prewhere->prewhere_column_name,
            prewhere->remove_prewhere_column});
    }

    /// Skip when any key/filter expression is non-deterministic across queries (e.g. `rand`,
    /// `now`, `nowInBlock`, `rowNumberInAllBlocks`). The cache hashes only the function graph,
    /// not per-execution values, so the first execution's states would be cached and incorrectly
    /// reused by every later execution that hashes to the same key. `hasNonDeterministic` uses
    /// `IFunction::isDeterministic` (deterministic across queries), which is the notion the
    /// cross-query cache requires.
    for (const auto & action : intermediate_actions)
        if (action.actions->getActionsDAG().hasNonDeterministic())
            return;

    /// The populator reads each part's data directly from storage with the set of columns required
    /// to feed the aggregator: the GROUP BY keys, the aggregate arguments, and the input columns of
    /// the intermediate `ExpressionStep`/`FilterStep` actions. When a key or aggregate argument is
    /// produced by an intermediate action (`GROUP BY toYear(d)`, `GROUP BY lower(s)`) its name is
    /// not a storage column, so `createMergeTreeSequentialSource` would throw inside the populator,
    /// which swallows the exception and silently never caches the part. Verify up-front that every
    /// column the populator would read is present in the storage snapshot, and skip the
    /// optimization otherwise (fail-closed), instead of relying on the populator's catch-all.
    {
        const auto & storage_snapshot = reading->getStorageSnapshot();
        auto column_is_readable = [&](const String & name)
        {
            return storage_snapshot->tryGetColumn(
                GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), name).has_value();
        };

        bool all_readable = true;
        for (const auto & key : params.keys)
            all_readable &= column_is_readable(key);
        for (const auto & agg : params.aggregates)
            for (const auto & arg : agg.argument_names)
                all_readable &= column_is_readable(arg);
        for (const auto & action : intermediate_actions)
            for (const auto & col : action.actions->getRequiredColumnsWithTypes())
                all_readable &= column_is_readable(col.name);

        if (!all_readable)
            return;

        /// When the populator would read no column at all — a keyless global aggregation over
        /// zero-argument aggregates with no intermediate actions, e.g. `SELECT count() FROM t` —
        /// `createMergeTreeSequentialSource` reads an empty column set, so every block reports
        /// `rows() == 0` and the part's real row count is lost. Skip the optimization in that case
        /// (fail-closed) rather than relying on the populator silently producing nothing, so such
        /// queries keep going through the normal aggregation path.
        bool has_column_to_read = !params.keys.empty();
        for (const auto & agg : params.aggregates)
            has_column_to_read |= !agg.argument_names.empty();
        for (const auto & action : intermediate_actions)
            has_column_to_read |= !action.actions->getRequiredColumnsWithTypes().empty();

        if (!has_column_to_read)
            return;
    }

    /// The aggregator's input header carries the actual key and aggregate-argument column types.
    /// It is hashed into the cache key so that metadata-only `ALTER` (e.g. `MODIFY COLUMN`), which
    /// keeps the same `{table_id, part_name}`, cannot reuse a cached state built for the old type.
    const auto & aggregator_input_header = *aggregating->getInputHeaders().front();

    IASTHash query_hash = PartAggregationCache::calculateQueryHash(
        aggregator_input_header, params.keys, params.aggregates, filter_dag_for_hash);

    /// Include the full intermediate ExpressionStep/FilterStep action DAGs in the hash.
    /// Hashing only output names is not enough: two filters can share output column
    /// names while computing different predicates, which would alias incompatible
    /// queries to the same cache key and return cached states from a different query.
    if (!intermediate_actions.empty())
    {
        SipHash extra_hash;
        extra_hash.update(query_hash.low64);
        extra_hash.update(query_hash.high64);

        /// As in `calculateQueryHash`, every variable-length component needs an explicit
        /// boundary: the action count before the loop and a length prefix before each string.
        /// `SipHash::update(String)` feeds raw bytes only, so without boundaries different
        /// action/filter sequences could concatenate to the same byte stream and alias
        /// incompatible queries to one cache key.
        extra_hash.update(intermediate_actions.size());
        for (const auto & action : intermediate_actions)
        {
            action.actions->getActionsDAG().updateHash(extra_hash);
            extra_hash.update(action.filter_column_name.size());
            extra_hash.update(action.filter_column_name);
            extra_hash.update(action.remove_filter_column);
        }
        query_hash = getSipHash128AsPair(extra_hash);
    }

    auto storage_id = reading->getMergeTreeData().getStorageID();

    /// Require a stable table identity. `MergeTree` part names restart from `all_1_1_0` after
    /// `DROP TABLE` + `CREATE TABLE`, and this cache is global and not invalidated on drop, so a
    /// recreated table could hit stale states from the previous instance and return incorrect
    /// results. The table `UUID` is stable across drop/recreate; the full table name is not.
    /// Fall back to skipping the optimization (fail-closed) when no `UUID` is available.
    if (!storage_id.hasUUID())
        return;
    String table_id = toString(storage_id.uuid);

    bool enable_reads = settings[Setting::enable_reads_from_part_aggregation_cache];

    RangesInDataParts uncached_parts;
    std::vector<PartAggregationCache::EntryPtr> cached_entries;

    for (const auto & part : parts)
    {
        PartAggregationCache::Key key{query_hash, table_id, part.data_part->name};
        auto entry = enable_reads ? cache->get(key) : nullptr;

        if (entry)
            cached_entries.push_back(std::move(entry));
        else
            uncached_parts.push_back(part);
    }

    bool enable_writes = settings[Setting::enable_writes_to_part_aggregation_cache];

    /// Populate cache for uncached parts (both cold and partially warm cache).
    if (enable_writes && !uncached_parts.empty())
    {
        populatePartAggregationCache(
            cache, query_hash, table_id, uncached_parts, params,
            aggregator_input_header,
            reading->getMergeTreeData(),
            reading->getStorageSnapshot(),
            context,
            intermediate_actions);

        /// Re-check: move newly cached parts from uncached to cached.
        RangesInDataParts still_uncached;
        for (const auto & part : uncached_parts)
        {
            PartAggregationCache::Key key{query_hash, table_id, part.data_part->name};
            auto entry = enable_reads ? cache->get(key) : nullptr;
            if (entry)
                cached_entries.push_back(std::move(entry));
            else
                still_uncached.push_back(part);
        }
        uncached_parts = std::move(still_uncached);
    }

    if (cached_entries.empty())
        return;

    /// Derive the cached-blocks header from the aggregator's input header, which is
    /// the same header the populator passes into `Aggregator::Params::getHeader` (see
    /// `PartAggregationCachePopulator.cpp`). Using `reading->getOutputHeader()` would
    /// diverge when intermediate `ExpressionStep`s compute GROUP BY keys not present
    /// on the read step (e.g. `toYear(date) AS y`).
    auto intermediate_header = std::make_shared<Block>(
        Aggregator::Params::getHeader(
            aggregator_input_header, params.only_merge, params.keys, params.aggregates, /* final = */ false));

    Pipe cached_pipe(std::make_shared<PartAggregationCacheSource>(
        *intermediate_header, std::move(cached_entries)));

    if (uncached_parts.empty())
    {
        auto & cached_source_node = nodes.emplace_back();
        cached_source_node.step = std::make_unique<ReadFromPreparedSource>(std::move(cached_pipe));
        cached_source_node.children = {};

        node.children.front() = &cached_source_node;
        aggregating->requestOnlyMergeForAggregateProjection(intermediate_header);
    }
    else
    {
        auto & cached_source_node = nodes.emplace_back();
        cached_source_node.step = std::make_unique<ReadFromPreparedSource>(std::move(cached_pipe));
        cached_source_node.children = {};

        auto analyzed = reading->getAnalyzedResult();
        if (!analyzed)
            return;
        auto new_result = std::make_shared<ReadFromMergeTree::AnalysisResult>(*analyzed);
        new_result->parts_with_ranges = std::move(uncached_parts);
        reading->setAnalyzedResult(std::move(new_result));

        auto projection_step = aggregating->convertToAggregatingProjection(intermediate_header);
        node.step = std::move(projection_step);
        node.children = {node.children.front(), &cached_source_node};
    }
}

}
}
