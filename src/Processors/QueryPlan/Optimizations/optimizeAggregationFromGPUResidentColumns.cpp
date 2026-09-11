#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include "config.h"

#if USE_GPU

#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/GPUAggregatingStep.h>
#include <Processors/QueryPlan/ReadFromGPUResidentColumns.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Access/EnabledRowPolicies.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <GPU/GPUAggregation.h>
#include <GPU/GPUColumnCache.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <set>
#include <unordered_map>

/// Answers a keyless `sum` from columns already held in GPU device memory: replaces the
/// `ReadFromMergeTree` under a `GPUAggregatingStep` with a `ReadFromGPUResidentColumns` that emits
/// one row of per-part sums, which the aggregation above adds up. The pass only rewrites the plan;
/// the cache is read, and filled where it misses, at execution time.
///
/// Modelled on `optimizeTrivialCountFromTextIndex`, which does the same thing for `count()`: match
/// an aggregation over a `MergeTree` read, swap the read for a source that answers from somewhere
/// else, and leave the aggregation above to combine the per-part answers.

namespace DB
{

namespace
{

/// Every refusal below is a reason the device path was not taken, and there is no way to find out
/// which one fired from the outside: `EXPLAIN` shows the plan that was kept, not the check that
/// kept it - and the plan it shows is not even the plan this pass saw, since it hides the
/// expression steps this pass has to descend through. So each refusal says so at trace level,
/// where a refusal being the normal case does no harm. `SET send_logs_level = 'trace'` is how to
/// read them.
///
/// `return {}` rather than `return false`, so that the same macro serves the functions that answer
/// with a `std::optional` and the ones that answer with a `bool`.
///
/// Two refusals are deliberately silent, and are plain `return false` at their site instead: the
/// step not being a `GPUAggregatingStep`, which is every node of every plan in the server, and a
/// helper having already logged the reason it refused for. Logging either would bury the lines
/// that say something.
#define GPU_RESIDENT_REFUSE(reason) \
    do \
    { \
        LOG_TRACE(getLogger("GPUResidentColumns"), "Not reading columns from device memory: {}", (reason)); \
        return {}; \
    } while (false)

}

namespace Setting
{
    extern const SettingsBool allow_experimental_gpu_aggregation;
    extern const SettingsBool serialize_query_plan;
    extern const SettingsInt64 max_partitions_to_read;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsUInt64 max_bytes_to_read_leaf;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsOverflowMode timeout_overflow_mode_leaf;
}
namespace MergeTreeSetting
{
    extern const MergeTreeSettingsInt64 max_partitions_to_read;
    extern const MergeTreeSettingsUInt64 max_concurrent_queries;
}
}

namespace DB::QueryPlanOptimizations
{

namespace
{

/// cuDF counts the rows of a column in a signed 32-bit integer, so a part with more rows than that
/// cannot be one column view on the device. Splitting it into several views and adding their sums
/// on the host is possible and is not this first cut; a part that large simply keeps the ordinary
/// path, where the batching already deals with the same limit.
constexpr size_t max_rows_per_part = (1UL << 31) - 1;

/// The names of the columns the aggregation sums, in the order its aggregates have them, or
/// nothing when the aggregation is not a bare keyless `sum` of single columns.
///
/// These are the names the aggregation uses, which are not the names the table has: the planner
/// puts an expression between the two that renames `u64` to `__table1.u64`, and `matchChain` below
/// is what translates one into the other.
std::optional<Names> collectSummedArguments(const GPUAggregatingStep & aggregating)
{
    const Aggregator::Params & params = aggregating.getParams();

    /// With `GROUP BY` the source would have to emit one row per group per part and the step above
    /// would have to merge groups rather than add up scalars. That is the keyed version of this
    /// optimization, not this one.
    if (!params.keys.empty())
        GPU_RESIDENT_REFUSE("the aggregation has GROUP BY keys");

    if (params.aggregates.empty())
        GPU_RESIDENT_REFUSE("the aggregation has no aggregate functions");

    /// An overflow row and a merge-only aggregation both mean the step above is not simply going to
    /// add up what it is given.
    if (params.only_merge || params.overflow_row || params.max_rows_to_group_by != 0)
        GPU_RESIDENT_REFUSE("the aggregation merges states, has an overflow row or a group limit");

    Names arguments;
    arguments.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
    {
        /// `sum` by its own name and with one argument, so that a combinator (`sumIf`,
        /// `sumDistinct`) or a parametric form cannot slip through - the same test
        /// `GPUAggregatingStep::canRunOnDevice` makes, asked again here because this pass has to
        /// stand on its own checks rather than on what some other step decided.
        if (aggregate.function->getName() != "sum" || !aggregate.parameters.empty() || aggregate.argument_names.size() != 1)
            GPU_RESIDENT_REFUSE("an aggregate that is not a single-argument `sum`");

        arguments.push_back(aggregate.argument_names.front());
    }

    return arguments;
}

/// The name a column has in an expression's input, when that expression hands it through
/// untouched - and nothing when it does not hand it through at all.
///
/// `isSortKeyPassThrough` in `AggregatingStep.cpp` asks almost this question and answers yes or no;
/// this one has to answer with the name, because the expression between the aggregation and the
/// read is a rename. An `ALIAS` node is a second name for its child's values and computes nothing,
/// so following the chain down to an `INPUT` proves that the output column is one of the
/// expression's input columns, bit for bit - and says which one. Anything else (a `FUNCTION`, a
/// `COLUMN` constant, an `ARRAY_JOIN`) is a value the table does not store, and a per-part sum of
/// it is not something a cached column can answer.
std::optional<String> passedThroughInputName(const ActionsDAG & dag, const String & name)
{
    const auto * node = dag.tryFindInOutputs(name);
    if (!node)
        return {};

    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();

    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};

    return node->result_name;
}

/// What the descent from the aggregation down to the read found.
struct MatchedChain
{
    /// The node whose only child is the read - which is the node whose child has to be replaced.
    /// It is the aggregation itself only when nothing is in between.
    QueryPlan::Node * above_read = nullptr;

    ReadFromMergeTree * reading = nullptr;

    /// For every name the aggregation sums, the name that column has in the read step's header.
    /// Not the same name: see `passedThroughInputName`.
    std::unordered_map<String, String> read_names;
};

/// Descends from the aggregation to the `ReadFromMergeTree` under it, through the expressions the
/// planner puts in between, proving on the way that every summed column is handed through them
/// untouched - the same descent `optimizeTrivialCountFromTextIndex::matchSubtree` makes, with a
/// proof attached.
///
/// There is always at least one expression in between: the planner's "Change column names to
/// column identifiers" step, which renames every column of the table to `__tableN.` plus its name,
/// merged by `mergeExpressions` with the "Before GROUP BY" step. So skipping expressions is not
/// optional, and neither is proving what they do - a query whose aggregation sums a computed
/// column reaches this with an expression that computes it, and summing the column it was computed
/// from would be a wrong answer rather than a slower one.
///
/// The proof is carried down: the set of names required at each level is the set of input names the
/// level above resolved to, so an expression which hands a column through and one which computes it
/// are distinguished at every step of the chain rather than only at the top.
std::optional<MatchedChain> matchChain(QueryPlan::Node & aggregating_node, const Names & arguments)
{
    MatchedChain matched;

    /// Keyed by the aggregation's own name for the column, valued by the name it has at the level
    /// the descent has reached. Several aggregates summing one column share an entry.
    for (const auto & argument : arguments)
        matched.read_names[argument] = argument;

    QueryPlan::Node * current = &aggregating_node;

    while (true)
    {
        if (current->children.size() != 1)
            GPU_RESIDENT_REFUSE("a step with other than one input below the aggregation");

        QueryPlan::Node * child = current->children.front();
        IQueryPlanStep * step = child->step.get();

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        {
            matched.above_read = current;
            matched.reading = reading;
            return matched;
        }

        /// A filter keeps only some of a part's rows, so a part's whole sum is not the answer for
        /// that part - whether the filter sits here or inside the read.
        if (typeid_cast<FilterStep *>(step))
            GPU_RESIDENT_REFUSE("a filter between the aggregation and the read");

        const auto * expression = typeid_cast<ExpressionStep *>(step);
        if (!expression)
            GPU_RESIDENT_REFUSE("a step that is neither an expression nor the read of a MergeTree table");

        /// An expression can hand a column through untouched and still not hand the rows through:
        /// an `ARRAY JOIN` inside it replicates every other column across the rows it expands, so
        /// the aggregation above would sum a value once per expanded row where a part stores it
        /// once. The step's own traits are what say whether that is happening - `ExpressionStep`
        /// sets `preserves_number_of_rows` from `ActionsDAG::hasArrayJoin` - so ask it rather than
        /// re-derive it here.
        if (!expression->getTransformTraits().preserves_number_of_rows)
            GPU_RESIDENT_REFUSE("an expression that does not preserve the number of rows");

        const ActionsDAG & dag = expression->getExpression();

        for (auto & [argument, name] : matched.read_names)
        {
            const auto input_name = passedThroughInputName(dag, name);
            if (!input_name)
                GPU_RESIDENT_REFUSE("an expression that computes a summed column instead of passing it through");

            name = *input_name;
        }

        current = child;
    }
}

/// The columns to sum, in the order the read step's header has them - which is the order
/// `ReadFromGPUResidentColumns` produces them in, because its output header is that header.
///
/// The names here are the read step's, not the aggregation's: `read_names` has translated them.
/// The invariant this establishes, and that the source step relies on, is that the read step's
/// header is exactly the columns some aggregate sums, under the names the table stores them by -
/// so the source can read a column out of a part by the name in its own output header, and the
/// expression above it goes on renaming them for the aggregation as it did for the read.
std::optional<std::vector<ReadFromGPUResidentColumns::ColumnToSum>> matchSummedColumns(
    const GPUAggregatingStep & aggregating, const ReadFromMergeTree & reading, const std::unordered_map<String, String> & read_names)
{
    const Block & read_header = *reading.getOutputHeader();
    const StorageMetadataPtr metadata = reading.getStorageMetadata();
    const ColumnsDescription & table_columns = metadata->getColumns();

    NameSet summed_names;

    for (const auto & aggregate : aggregating.getParams().aggregates)
    {
        const auto read_name = read_names.find(aggregate.argument_names.front());
        if (read_name == read_names.end())
            GPU_RESIDENT_REFUSE("a summed column the descent did not translate");

        const String & name = read_name->second;

        /// The argument has to be a column of the table rather than anything else the read can
        /// produce. What it is not is already known - the descent has proved that the aggregation
        /// sums this very column of the read's output - so what is left is that the read's own
        /// column is a stored column of this table and not a virtual one.
        const ColumnWithTypeAndName * read_column = read_header.findByName(name);
        if (!read_column)
            GPU_RESIDENT_REFUSE("a summed column that is not in the read step's header");

        if (metadata->virtuals.has(name))
            GPU_RESIDENT_REFUSE("a summed column that is a virtual column");

        if (!table_columns.hasPhysical(name))
            GPU_RESIDENT_REFUSE("a summed column that is not a stored column of the table");

        /// The type the table says the column has, the type the read step's header says it has, and
        /// the type `sum` returns all have to be the same one. The first two disagreeing would mean
        /// a conversion happens on the way out of the read; the third disagreeing is what makes
        /// this optimization possible at all, since it is what lets a column of per-part sums be a
        /// column of the same type.
        const DataTypePtr & type = read_column->type;
        if (!table_columns.getPhysical(name).type->equals(*type))
            GPU_RESIDENT_REFUSE("a summed column whose type differs from the table's");

        if (!aggregate.function->getResultType()->equals(*type))
            GPU_RESIDENT_REFUSE("a sum whose type differs from its argument's");

        /// `UInt64`, `Int64` and `Float64` and nothing else - see `ReadFromGPUResidentColumns`.
        /// `canSumOnDevice` asked with one type for both the argument and the result is exactly
        /// that question: which columns does the device sum into their own type.
        if (!GPU::canSumOnDevice(*type, *type))
            GPU_RESIDENT_REFUSE("a summed column of a type the device does not sum into itself");

        summed_names.insert(name);
    }

    std::vector<ReadFromGPUResidentColumns::ColumnToSum> columns;
    columns.reserve(read_header.columns());

    for (const auto & read_column : read_header)
    {
        /// A column the read produces that nothing sums. The source would have to put something in
        /// it, and a per-part sum of a column that is not summed is a value nobody asked for - so
        /// refuse rather than invent one. In practice the read of an eligible query produces
        /// exactly the summed columns.
        if (!summed_names.contains(read_column.name))
            GPU_RESIDENT_REFUSE("the read produces a column nothing sums");

        const auto element_type = GPU::elementTypeOf(*read_column.type);
        const auto sum_type = GPU::sumTypeOf(*read_column.type);
        if (!element_type || !sum_type)
            GPU_RESIDENT_REFUSE("a column of a type the device has no element type for");

        columns.push_back({
            .name = read_column.name,
            .element_type = *element_type,
            .sum_type = *sum_type,
            .element_size = GPU::elementSizeOf(*element_type),
        });
    }

    return columns;
}

/// Whether the read is of whole parts, unfiltered, of this table's own data - everything about the
/// read that has to hold for a part's whole sum to be the answer for that part.
bool readIsOfWholeParts(const ReadFromMergeTree & reading)
{
    /// Any of these filters rows, and a filtered part's sum is not the sum of its column. A filter
    /// pushed into the read is `filter_actions_dag`; `PREWHERE` and the row-level filter have
    /// places of their own, and each has a deferred form that a lazy `FINAL` plan leaves behind.
    if (reading.getFilterActionsDAG() || reading.getPrewhereInfo() || reading.getRowLevelFilter())
        GPU_RESIDENT_REFUSE("a filter, PREWHERE or row-level filter in the read");

    if (reading.getDeferredPrewhereInfo() || reading.getDeferredRowLevelFilter())
        GPU_RESIDENT_REFUSE("a deferred PREWHERE or row-level filter in the read");

    /// `FINAL` collapses rows across parts, so a per-part sum is not a partial result of anything.
    /// Sampling reads a fraction of each part and scales the result.
    if (reading.isQueryWithFinal() || reading.isQueryWithSampling())
        GPU_RESIDENT_REFUSE("the read is FINAL or sampled");

    /// Reads that produce something other than the table's stored columns: a direct read from a
    /// text index adds virtual columns, a vector search replaces a column with `_distance` or
    /// filters rows by a candidate list, and a top-k read filters by a threshold.
    if (!reading.getIndexReadTasks().empty() || reading.getVectorSearchParameters() || reading.isVectorColumnReplaced())
        GPU_RESIDENT_REFUSE("the read is from an index or a vector search");

    if (reading.isSelectedForTopKFilterOptimization())
        GPU_RESIDENT_REFUSE("the read is filtered by a top-k threshold");

    if (!reading.getProjectionIndexReadDescription().read_ranges.empty())
        GPU_RESIDENT_REFUSE("the read uses a projection index");

    /// A read with an output port per partition feeds several aggregations rather than one.
    if (reading.willOutputEachPartitionThroughSeparatePort())
        GPU_RESIDENT_REFUSE("the read outputs each partition through its own port");

    /// Every replica would sum every part it is given, and the coordination that keeps them from
    /// reading the same rows twice is in the reader this replaces.
    if (reading.isParallelReadingFromReplicas() || reading.isParallelReadingEnabled() || reading.getDistributedReadBucketCount() > 0)
        GPU_RESIDENT_REFUSE("the read is distributed across replicas");

    /// A `UNIQUE KEY` table filters rows by a delete bitmap while reading, which the sequential
    /// source that fills the cache does not apply.
    if (reading.getStorageMetadata()->hasUniqueKey())
        GPU_RESIDENT_REFUSE("the table has a UNIQUE KEY");

    return true;
}

/// Whether the query around the read is one whose answer does not depend on how much was read -
/// because on a cache hit nothing is.
bool queryLimitsAllowReadingFromCache(const ReadFromMergeTree & reading)
{
    const ContextPtr context = reading.getContext();
    const Settings & settings = context->getSettingsRef();

    /// `ReadFromGPUResidentColumns` has no serialization, so it must not end up in a plan that is
    /// sent somewhere.
    if (settings[Setting::serialize_query_plan])
        GPU_RESIDENT_REFUSE("`serialize_query_plan` is on");

    /// A transaction can see Outdated parts, and what it may see is decided by the reader.
    if (context->getCurrentTransaction())
        GPU_RESIDENT_REFUSE("the query is in a transaction");

    /// These bound how much a query may read, and on a cache hit it reads nothing - so the same
    /// query would throw on the run that fills the cache and succeed on the next one. Refusing the
    /// optimization keeps the limit meaning what it says.
    if (settings[Setting::max_rows_to_read] != 0 || settings[Setting::max_bytes_to_read] != 0
        || settings[Setting::max_rows_to_read_leaf] != 0 || settings[Setting::max_bytes_to_read_leaf] != 0)
        GPU_RESIDENT_REFUSE("a limit on how much may be read is set");

    /// With `break`, a query that runs out of time stops reading and returns what it has. A part
    /// whose read stopped half way has no partial answer here - the row the source emits claims to
    /// be that part's whole sum - so this path only takes queries whose time limit throws.
    if (settings[Setting::timeout_overflow_mode] != OverflowMode::THROW
        || settings[Setting::timeout_overflow_mode_leaf] != OverflowMode::THROW)
        GPU_RESIDENT_REFUSE("`timeout_overflow_mode` is not `throw`");

    /// The rewrite bypasses the reader's `checkLimits`, so mirror the two things it establishes:
    /// `max_concurrent_queries`, which needs a holder kept for the length of the execution that
    /// this pass has nowhere to put, and `max_partitions_to_read`, which the reader would have
    /// thrown `TOO_MANY_PARTITIONS` for.
    const auto & data_settings = *reading.getMergeTreeData().getSettings();

    if (data_settings[MergeTreeSetting::max_concurrent_queries] > 0)
        GPU_RESIDENT_REFUSE("the table has `max_concurrent_queries` set");

    const Int64 max_partitions_to_read = settings[Setting::max_partitions_to_read].changed
        ? settings[Setting::max_partitions_to_read]
        : data_settings[MergeTreeSetting::max_partitions_to_read];

    if (max_partitions_to_read > 0)
    {
        std::set<String> partitions;
        for (const auto & part_with_ranges : reading.getParts())
        {
            partitions.insert(part_with_ranges.data_part->info.getPartitionId());
            if (partitions.size() > static_cast<size_t>(max_partitions_to_read))
                GPU_RESIDENT_REFUSE("the read is over more than `max_partitions_to_read` partitions");
        }
    }

    /// A row policy filters rows that a part's whole sum ignores. Without a database name the
    /// policy cannot be looked up, so that case is refused rather than assumed to be empty.
    const StorageID storage_id = reading.getStorageID();
    if (!storage_id.hasDatabase())
        GPU_RESIDENT_REFUSE("the table has no database name to look a row policy up by");

    if (const auto row_policy_filter = context->getRowPolicyFilter(
            storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
        row_policy_filter && !row_policy_filter->isAlwaysTrue())
        GPU_RESIDENT_REFUSE("a row policy applies to the table");

    return true;
}

/// The parts to sum, or nothing when any of them is not one this path can sum whole.
///
/// Parts of no rows are left out rather than refused: such a part produces no row on the ordinary
/// path either, and emitting one for it would turn the empty result of
/// `empty_result_for_aggregation_by_empty_set` into a row of zeroes.
std::optional<DataPartsVector> matchWholeParts(
    const ReadFromMergeTree & reading, const std::vector<ReadFromGPUResidentColumns::ColumnToSum> & columns)
{
    const MergeTreeData::MutationsSnapshotPtr & mutations = reading.getMutationsSnapshot();

    /// Without it there is no way to ask whether a part needs anything applied on the fly, and a
    /// part that does is one whose stored values are not the values the query must see.
    if (!mutations)
        GPU_RESIDENT_REFUSE("the read has no mutations snapshot");

    if (mutations->hasDataMutations() || mutations->hasAlterMutations() || mutations->hasMetadataMutations()
        || mutations->hasPatchParts() || mutations->hasLightweightDeletedMask())
        GPU_RESIDENT_REFUSE("the table has mutations, patch parts or a lightweight delete mask");

    /// A masking policy is applied while reading, as alter conversions the sequential source that
    /// fills the cache is not given. Always false outside the Cloud build.
    if (reading.getMergeTreeData().hasEnabledMaskingPolicies(reading.getContext()))
        GPU_RESIDENT_REFUSE("a masking policy applies to the table");

    const Block & read_header = *reading.getOutputHeader();

    DataPartsVector parts;
    parts.reserve(reading.getParts().size());

    for (const auto & part_with_ranges : reading.getParts())
    {
        const DataPartPtr & part = part_with_ranges.data_part;
        if (!part)
            GPU_RESIDENT_REFUSE("a part without a data part object");

        /// A projection part holds a projection's rows, not the table's.
        if (part->isProjectionPart() || part_with_ranges.parent_part)
            GPU_RESIDENT_REFUSE("a projection part");

        /// The primary key, a skip index or partition pruning left less than the whole part to
        /// read. Asked of the part rather than of the analysis as a whole, because this is the
        /// thing that matters: the row the source emits is this part's whole sum.
        if (part_with_ranges.getRowsCount() != part->rows_count)
            GPU_RESIDENT_REFUSE("a part of which less than every row is read");

        /// Rows deleted by a lightweight delete are filtered while reading, so the part's stored
        /// column is not what the query sums.
        if (part->hasLightweightDelete())
            GPU_RESIDENT_REFUSE("a part with a lightweight delete mask");

        if (!mutations->getOnFlyMutationCommandsForPart(part).empty() || !mutations->getPatchesForPart(part).empty())
            GPU_RESIDENT_REFUSE("a part with an on-the-fly mutation or a patch");

        if (part->rows_count > max_rows_per_part)
            GPU_RESIDENT_REFUSE("a part with more rows than the device can view at once");

        if (part->rows_count == 0)
            continue;

        for (const auto & column : columns)
        {
            /// The column has to be in this part, with the type the table says it has and with
            /// files of its own. A column the part does not store is filled from the column's
            /// `DEFAULT` while reading - and that expression lives in the table's metadata, which
            /// can be changed without writing a new part. Caching such values would be the one way
            /// a cached column could go stale, so those parts are refused instead.
            const auto part_column = part->tryGetColumn(column.name);
            if (!part_column || !part_column->type->equals(*read_header.getByName(column.name).type))
                GPU_RESIDENT_REFUSE("a part that does not store a summed column with the table's type");

            if (!part->hasColumnFiles(*part_column))
                GPU_RESIDENT_REFUSE("a part that has no files for a summed column");
        }

        parts.push_back(part);
    }

    return parts;
}

}

bool optimizeAggregationFromGPUResidentColumns(
    QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
    /// `ReadFromGPUResidentColumns` is not serializable, so it must not end up in a distributed plan
    /// fragment - and `applyParallelReplicas` builds such fragments around the `ReadFromMergeTree`
    /// this would replace.
    if (settings.make_distributed_plan || settings.enable_parallel_replicas)
        GPU_RESIDENT_REFUSE("the plan is being made distributed");

    /// Replacing the read makes the plan one that no projection pass will look at, so a query that
    /// insists on a projection has to be left alone rather than quietly answered without one.
    if (settings.force_use_projection || !settings.force_projection_name.empty())
        GPU_RESIDENT_REFUSE("the query insists on using a projection");

    const auto * aggregating = typeid_cast<const GPUAggregatingStep *>(node.step.get());
    if (!aggregating)
        return false;

    auto arguments = collectSummedArguments(*aggregating);
    if (!arguments)
        return false;

    auto chain = matchChain(node, *arguments);
    if (!chain)
        return false;

    ReadFromMergeTree * reading = chain->reading;
    const ContextPtr context = reading->getContext();

    /// Asked rather than assumed from the step above existing: this setting is what the whole
    /// device path hangs on, and a check of it here is what makes this pass readable on its own.
    if (!context->getSettingsRef()[Setting::allow_experimental_gpu_aggregation])
        GPU_RESIDENT_REFUSE("`allow_experimental_gpu_aggregation` is off");

    /// No cache, no optimization - which is what `gpu_column_cache_size = 0` means: the query
    /// aggregates on the device as before, uploading its columns again.
    GPUColumnCachePtr cache = context->getGPUColumnCache();
    if (!cache || cache->maxSizeInBytes() == 0)
        GPU_RESIDENT_REFUSE("`gpu_column_cache_size` is 0");

    /// The planner has already refused the query if the device is unusable while
    /// `allow_experimental_gpu_aggregation` is on, so this is the cached answer of a probe that
    /// has already happened - and the one thing that would otherwise be found out at execution
    /// time, where there would be nothing to do about it.
    if (!GPU::deviceProbeError().empty())
        GPU_RESIDENT_REFUSE("no usable device");

    if (!readIsOfWholeParts(*reading))
        return false;

    auto columns = matchSummedColumns(*aggregating, *reading, chain->read_names);
    if (!columns)
        return false;

    /// From here on the parts are the analyzed ones - which is what `getParts` returns once this is
    /// set, and what everything below asks about. The read step would have done this itself; doing
    /// it here is what lets the pass see the same parts the read would have produced.
    if (!reading->getAnalyzedResult())
        reading->setAnalyzedResult(reading->selectRangesToRead());

    const auto analysis = reading->getAnalyzedResult();
    if (!analysis)
        GPU_RESIDENT_REFUSE("the read has no analyzed result");

    if (!analysis->isUsable())
        GPU_RESIDENT_REFUSE("the read exceeded its row limits");

    if (!queryLimitsAllowReadingFromCache(*reading))
        return false;

    /// The parts are a projection's, the primary key left less than every mark to read, or the
    /// read is a sampled one. Each is asked of the analysis as a whole here and of every part
    /// below; this is the cheap version of the same question.
    if (analysis->readFromProjection())
        GPU_RESIDENT_REFUSE("the read is from a projection");

    if (analysis->total_marks_pk != analysis->selected_marks_pk)
        GPU_RESIDENT_REFUSE("the primary key pruned marks");

    if (analysis->sampling.use_sampling || analysis->sampling.read_nothing)
        GPU_RESIDENT_REFUSE("the read is sampled");

    auto parts = matchWholeParts(*reading, *columns);
    if (!parts)
        return false;

    LOG_DEBUG(
        getLogger("GPUResidentColumns"),
        "Reading columns from device memory: {} whole parts, {} columns",
        parts->size(),
        columns->size());

    /// The read step's header, unchanged and shared: the source produces the same columns of the
    /// same types under the same names, which is what lets every step above it - the expressions
    /// the descent came through, and the aggregation over them - stay exactly as they are.
    SharedHeader header = reading->getOutputHeader();

    /// The step above goes on reading its input as if it were the read's output, so that input has
    /// to be what the source produces. It is the same object here, so this cannot fail today; it is
    /// checked because it is the assumption that lets the whole subtree above stay untouched, and a
    /// source header computed rather than passed through would break it silently.
    const SharedHeaders & headers_above_read = chain->above_read->step->getInputHeaders();
    if (headers_above_read.size() != 1 || !blocksHaveEqualStructure(*headers_above_read.front(), *header))
        GPU_RESIDENT_REFUSE("the step above the read would see a different header");

    auto & source_node = nodes.emplace_back();
    source_node.step = std::make_unique<ReadFromGPUResidentColumns>(
        header,
        std::move(*columns),
        std::move(*parts),
        reading->getMergeTreeData(),
        reading->getStorageSnapshot(),
        std::move(cache),
        context,
        reading->getNumStreams());

    source_node.step->setStepDescription("Sums of GPU resident columns, one row per part", settings.max_step_description_length);

    /// The child of the lowest node of the chain, not of the aggregation: with an expression in
    /// between - and there always is one - the aggregation's child is that expression, which stays
    /// where it is.
    chain->above_read->children.front() = &source_node;
    return true;
}

}

#else

namespace DB::QueryPlanOptimizations
{

/// Without a GPU build there is no `GPUAggregatingStep` to match, so this could only ever refuse -
/// and it says nothing while doing so, because a build with no device path has nothing to explain.
bool optimizeAggregationFromGPUResidentColumns(QueryPlan::Node &, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &)
{
    return false;
}

}

#endif
