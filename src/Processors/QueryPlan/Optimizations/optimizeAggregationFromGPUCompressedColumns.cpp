#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include "config.h"

#if USE_GPU

#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/GPUAggregatingStep.h>
#include <Processors/QueryPlan/ReadFromGPUCompressedColumns.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Access/EnabledRowPolicies.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <GPU/GPUAggregation.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Compression/ICompressionCodec.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <set>
#include <unordered_map>

namespace DB
{

namespace
{

#define GPU_COMPRESSED_REFUSE(reason) \
    do \
    { \
        LOG_TRACE(getLogger("GPUCompressedColumns"), "Not summing compressed columns on the device: {}", (reason)); \
        return {}; \
    } while (false)

}

namespace Setting
{
    extern const SettingsBool allow_experimental_gpu_aggregation;
    extern const SettingsUInt64 gpu_aggregation_batch_bytes;
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

constexpr size_t max_rows_per_part = (1UL << 31) - 1;

std::optional<Names> collectSummedArguments(const GPUAggregatingStep & aggregating)
{
    const Aggregator::Params & params = aggregating.getParams();

    if (!params.keys.empty())
        GPU_COMPRESSED_REFUSE("the aggregation has GROUP BY keys");

    if (params.aggregates.empty())
        GPU_COMPRESSED_REFUSE("the aggregation has no aggregate functions");

    if (params.only_merge || params.overflow_row || params.max_rows_to_group_by != 0)
        GPU_COMPRESSED_REFUSE("the aggregation merges states, has an overflow row or a group limit");

    Names arguments;
    arguments.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
    {
        if (aggregate.function->getName() != "sum" || !aggregate.parameters.empty() || aggregate.argument_names.size() != 1)
            GPU_COMPRESSED_REFUSE("an aggregate that is not a single-argument `sum`");

        arguments.push_back(aggregate.argument_names.front());
    }

    return arguments;
}

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

struct MatchedChain
{
    QueryPlan::Node * above_read = nullptr;
    ReadFromMergeTree * reading = nullptr;
    std::unordered_map<String, String> read_names;
};

std::optional<MatchedChain> matchChain(QueryPlan::Node & aggregating_node, const Names & arguments)
{
    MatchedChain matched;

    for (const auto & argument : arguments)
        matched.read_names[argument] = argument;

    QueryPlan::Node * current = &aggregating_node;

    while (true)
    {
        if (current->children.size() != 1)
            GPU_COMPRESSED_REFUSE("a step with other than one input below the aggregation");

        QueryPlan::Node * child = current->children.front();
        IQueryPlanStep * step = child->step.get();

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        {
            matched.above_read = current;
            matched.reading = reading;
            return matched;
        }

        if (typeid_cast<FilterStep *>(step))
            GPU_COMPRESSED_REFUSE("a filter between the aggregation and the read");

        const auto * expression = typeid_cast<ExpressionStep *>(step);
        if (!expression)
            GPU_COMPRESSED_REFUSE("a step that is neither an expression nor the read of a MergeTree table");

        if (!expression->getTransformTraits().preserves_number_of_rows)
            GPU_COMPRESSED_REFUSE("an expression that does not preserve the number of rows");

        const ActionsDAG & dag = expression->getExpression();

        for (auto & [argument, name] : matched.read_names)
        {
            const auto input_name = passedThroughInputName(dag, name);
            if (!input_name)
                GPU_COMPRESSED_REFUSE("an expression that computes a summed column instead of passing it through");

            name = *input_name;
        }

        current = child;
    }
}

std::optional<std::vector<ReadFromGPUCompressedColumns::ColumnToSum>> matchSummedColumns(
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
            GPU_COMPRESSED_REFUSE("a summed column the descent did not translate");

        const String & name = read_name->second;

        const ColumnWithTypeAndName * read_column = read_header.findByName(name);
        if (!read_column)
            GPU_COMPRESSED_REFUSE("a summed column that is not in the read step's header");

        if (metadata->virtuals.has(name))
            GPU_COMPRESSED_REFUSE("a summed column that is a virtual column");

        if (!table_columns.hasPhysical(name))
            GPU_COMPRESSED_REFUSE("a summed column that is not a stored column of the table");

        const DataTypePtr & type = read_column->type;
        if (!table_columns.getPhysical(name).type->equals(*type))
            GPU_COMPRESSED_REFUSE("a summed column whose type differs from the table's");

        if (!aggregate.function->getResultType()->equals(*type))
            GPU_COMPRESSED_REFUSE("a sum whose type differs from its argument's");

        if (!GPU::canSumOnDevice(*type, *type))
            GPU_COMPRESSED_REFUSE("a summed column of a type the device does not sum into itself");

        if (table_columns.hasCompressionCodec(name))
            GPU_COMPRESSED_REFUSE("a summed column with a CODEC of its own");

        summed_names.insert(name);
    }

    std::vector<ReadFromGPUCompressedColumns::ColumnToSum> columns;
    columns.reserve(read_header.columns());

    for (const auto & read_column : read_header)
    {
        if (!summed_names.contains(read_column.name))
            GPU_COMPRESSED_REFUSE("the read produces a column nothing sums");

        const auto element_type = GPU::elementTypeOf(*read_column.type);
        const auto sum_type = GPU::sumTypeOf(*read_column.type);
        if (!element_type || !sum_type)
            GPU_COMPRESSED_REFUSE("a column of a type the device has no element type for");

        columns.push_back({
            .column = NameAndTypePair(read_column.name, read_column.type),
            .result_type = read_column.type,
            .element_type = *element_type,
            .sum_type = *sum_type,
        });
    }

    return columns;
}

bool readIsOfWholeParts(const ReadFromMergeTree & reading)
{
    /// Any of these filters rows, and a filtered part's sum is not the sum of its column. A filter
    /// pushed into the read is `filter_actions_dag`; `PREWHERE` and the row-level filter have
    /// places of their own, and each has a deferred form that a lazy `FINAL` plan leaves behind.
    if (reading.getFilterActionsDAG() || reading.getPrewhereInfo() || reading.getRowLevelFilter())
        GPU_COMPRESSED_REFUSE("a filter, PREWHERE or row-level filter in the read");

    if (reading.getDeferredPrewhereInfo() || reading.getDeferredRowLevelFilter())
        GPU_COMPRESSED_REFUSE("a deferred PREWHERE or row-level filter in the read");

    /// `FINAL` collapses rows across parts, so a per-part sum is not a partial result of anything.
    /// Sampling reads a fraction of each part and scales the result.
    if (reading.isQueryWithFinal() || reading.isQueryWithSampling())
        GPU_COMPRESSED_REFUSE("the read is FINAL or sampled");

    /// Reads that produce something other than the table's stored columns: a direct read from a
    /// text index adds virtual columns, a vector search replaces a column with `_distance` or
    /// filters rows by a candidate list, and a top-k read filters by a threshold.
    if (!reading.getIndexReadTasks().empty() || reading.getVectorSearchParameters() || reading.isVectorColumnReplaced())
        GPU_COMPRESSED_REFUSE("the read is from an index or a vector search");

    if (reading.isSelectedForTopKFilterOptimization())
        GPU_COMPRESSED_REFUSE("the read is filtered by a top-k threshold");

    if (!reading.getProjectionIndexReadDescription().read_ranges.empty())
        GPU_COMPRESSED_REFUSE("the read uses a projection index");

    /// A read with an output port per partition feeds several aggregations rather than one.
    if (reading.willOutputEachPartitionThroughSeparatePort())
        GPU_COMPRESSED_REFUSE("the read outputs each partition through its own port");

    /// Every replica would sum every part it is given, and the coordination that keeps them from
    /// reading the same rows twice is in the reader this replaces.
    if (reading.isParallelReadingFromReplicas() || reading.isParallelReadingEnabled() || reading.getDistributedReadBucketCount() > 0)
        GPU_COMPRESSED_REFUSE("the read is distributed across replicas");

    /// A `UNIQUE KEY` table filters rows by a delete bitmap while reading, which the sequential
    /// source that fills the cache does not apply.
    if (reading.getStorageMetadata()->hasUniqueKey())
        GPU_COMPRESSED_REFUSE("the table has a UNIQUE KEY");

    return true;
}

/// Whether the query around the read is one whose answer does not depend on how much was read -
/// because on a cache hit nothing is.
bool queryLimitsAllowReading(const ReadFromMergeTree & reading)
{
    const ContextPtr context = reading.getContext();
    const Settings & settings = context->getSettingsRef();

    /// `ReadFromGPUCompressedColumns` has no serialization, so it must not end up in a plan that is
    /// sent somewhere.
    if (settings[Setting::serialize_query_plan])
        GPU_COMPRESSED_REFUSE("`serialize_query_plan` is on");

    /// A transaction can see Outdated parts, and what it may see is decided by the reader.
    if (context->getCurrentTransaction())
        GPU_COMPRESSED_REFUSE("the query is in a transaction");

    /// These bound how much a query may read, and on a cache hit it reads nothing - so the same
    /// query would throw on the run that fills the cache and succeed on the next one. Refusing the
    /// optimization keeps the limit meaning what it says.
    if (settings[Setting::max_rows_to_read] != 0 || settings[Setting::max_bytes_to_read] != 0
        || settings[Setting::max_rows_to_read_leaf] != 0 || settings[Setting::max_bytes_to_read_leaf] != 0)
        GPU_COMPRESSED_REFUSE("a limit on how much may be read is set");

    /// With `break`, a query that runs out of time stops reading and returns what it has. A part
    /// whose read stopped half way has no partial answer here - the row the source emits claims to
    /// be that part's whole sum - so this path only takes queries whose time limit throws.
    if (settings[Setting::timeout_overflow_mode] != OverflowMode::THROW
        || settings[Setting::timeout_overflow_mode_leaf] != OverflowMode::THROW)
        GPU_COMPRESSED_REFUSE("`timeout_overflow_mode` is not `throw`");

    /// The rewrite bypasses the reader's `checkLimits`, so mirror the two things it establishes:
    /// `max_concurrent_queries`, which needs a holder kept for the length of the execution that
    /// this pass has nowhere to put, and `max_partitions_to_read`, which the reader would have
    /// thrown `TOO_MANY_PARTITIONS` for.
    const auto & data_settings = *reading.getMergeTreeData().getSettings();

    if (data_settings[MergeTreeSetting::max_concurrent_queries] > 0)
        GPU_COMPRESSED_REFUSE("the table has `max_concurrent_queries` set");

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
                GPU_COMPRESSED_REFUSE("the read is over more than `max_partitions_to_read` partitions");
        }
    }

    /// A row policy filters rows that a part's whole sum ignores. Without a database name the
    /// policy cannot be looked up, so that case is refused rather than assumed to be empty.
    const StorageID storage_id = reading.getStorageID();
    if (!storage_id.hasDatabase())
        GPU_COMPRESSED_REFUSE("the table has no database name to look a row policy up by");

    if (const auto row_policy_filter = context->getRowPolicyFilter(
            storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
        row_policy_filter && !row_policy_filter->isAlwaysTrue())
        GPU_COMPRESSED_REFUSE("a row policy applies to the table");

    return true;
}

std::optional<DataPartsVector> matchWholeParts(
    const ReadFromMergeTree & reading, const std::vector<ReadFromGPUCompressedColumns::ColumnToSum> & columns)
{
    const MergeTreeData::MutationsSnapshotPtr & mutations = reading.getMutationsSnapshot();

    if (!mutations)
        GPU_COMPRESSED_REFUSE("the read has no mutations snapshot");

    if (mutations->hasDataMutations() || mutations->hasAlterMutations() || mutations->hasMetadataMutations()
        || mutations->hasPatchParts() || mutations->hasLightweightDeletedMask())
        GPU_COMPRESSED_REFUSE("the table has mutations, patch parts or a lightweight delete mask");

    if (reading.getMergeTreeData().hasEnabledMaskingPolicies(reading.getContext()))
        GPU_COMPRESSED_REFUSE("a masking policy applies to the table");

    const Block & read_header = *reading.getOutputHeader();

    DataPartsVector parts;
    parts.reserve(reading.getParts().size());

    for (const auto & part_with_ranges : reading.getParts())
    {
        const DataPartPtr & part = part_with_ranges.data_part;
        if (!part)
            GPU_COMPRESSED_REFUSE("a part without a data part object");

        if (part->isProjectionPart() || part_with_ranges.parent_part)
            GPU_COMPRESSED_REFUSE("a projection part");

        if (part_with_ranges.getRowsCount() != part->rows_count)
            GPU_COMPRESSED_REFUSE("a part of which less than every row is read");

        if (part->hasLightweightDelete())
            GPU_COMPRESSED_REFUSE("a part with a lightweight delete mask");

        if (!mutations->getOnFlyMutationCommandsForPart(part).empty() || !mutations->getPatchesForPart(part).empty())
            GPU_COMPRESSED_REFUSE("a part with an on-the-fly mutation or a patch");

        if (part->rows_count > max_rows_per_part)
            GPU_COMPRESSED_REFUSE("a part with more rows than the device can view at once");

        if (part->rows_count == 0)
            continue;

        if (part->getType() != MergeTreeDataPartType::Wide)
            GPU_COMPRESSED_REFUSE("a part that is not wide");

        if (!part->default_codec || !GPU::codecOf(part->default_codec->getMethodByte()))
            GPU_COMPRESSED_REFUSE("a part written with a codec the device cannot expand");

        for (const auto & column : columns)
        {
            const auto part_column = part->tryGetColumn(column.column.name);
            if (!part_column || !part_column->type->equals(*read_header.getByName(column.column.name).type))
                GPU_COMPRESSED_REFUSE("a part that does not store a summed column with the table's type");

            if (!part->hasColumnFiles(*part_column))
                GPU_COMPRESSED_REFUSE("a part that has no files for a summed column");
        }

        parts.push_back(part);
    }

    return parts;
}

}

bool optimizeAggregationFromGPUCompressedColumns(
    QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
    if (settings.make_distributed_plan || settings.enable_parallel_replicas)
        GPU_COMPRESSED_REFUSE("the plan is being made distributed");

    if (settings.force_use_projection || !settings.force_projection_name.empty())
        GPU_COMPRESSED_REFUSE("the query insists on using a projection");

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

    if (!context->getSettingsRef()[Setting::allow_experimental_gpu_aggregation])
        GPU_COMPRESSED_REFUSE("`allow_experimental_gpu_aggregation` is off");

    if (!GPU::deviceProbeError().empty())
        GPU_COMPRESSED_REFUSE("no usable device");

    if (!readIsOfWholeParts(*reading))
        return false;

    auto columns = matchSummedColumns(*aggregating, *reading, chain->read_names);
    if (!columns)
        return false;

    if (!reading->getAnalyzedResult())
        reading->setAnalyzedResult(reading->selectRangesToRead());

    const auto analysis = reading->getAnalyzedResult();
    if (!analysis)
        GPU_COMPRESSED_REFUSE("the read has no analyzed result");

    if (!analysis->isUsable())
        GPU_COMPRESSED_REFUSE("the read exceeded its row limits");

    if (!queryLimitsAllowReading(*reading))
        return false;

    if (analysis->readFromProjection())
        GPU_COMPRESSED_REFUSE("the read is from a projection");

    if (analysis->total_marks_pk != analysis->selected_marks_pk)
        GPU_COMPRESSED_REFUSE("the primary key pruned marks");

    if (analysis->sampling.use_sampling || analysis->sampling.read_nothing)
        GPU_COMPRESSED_REFUSE("the read is sampled");

    auto parts = matchWholeParts(*reading, *columns);
    if (!parts)
        return false;

    LOG_DEBUG(
        getLogger("GPUCompressedColumns"),
        "Summing compressed columns on the device: {} whole parts, {} columns",
        parts->size(),
        columns->size());

    SharedHeader header = reading->getOutputHeader();

    const SharedHeaders & headers_above_read = chain->above_read->step->getInputHeaders();
    if (headers_above_read.size() != 1 || !blocksHaveEqualStructure(*headers_above_read.front(), *header))
        GPU_COMPRESSED_REFUSE("the step above the read would see a different header");

    auto & source_node = nodes.emplace_back();
    source_node.step = std::make_unique<ReadFromGPUCompressedColumns>(
        header,
        std::move(*columns),
        std::move(*parts),
        reading->getStorageSnapshot(),
        context,
        context->getSettingsRef()[Setting::gpu_aggregation_batch_bytes],
        reading->getNumStreams());

    source_node.step->setStepDescription(
        "Sums of compressed columns decompressed on the device, one row per part", settings.max_step_description_length);

    chain->above_read->children.front() = &source_node;
    return true;
}

}

#else

namespace DB::QueryPlanOptimizations
{

bool optimizeAggregationFromGPUCompressedColumns(QueryPlan::Node &, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &)
{
    return false;
}

}

#endif
