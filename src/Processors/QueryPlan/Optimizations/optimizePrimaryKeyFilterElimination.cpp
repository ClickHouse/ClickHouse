#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <Processors/QueryPlan/Optimizations/useDataParallelAggregation.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/UnionStep.h>

#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/getEffectiveRowPolicyFilter.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <unordered_map>

namespace DB
{
namespace Setting
{
    extern const SettingsBool distributed_index_analysis;
    extern const SettingsBool force_aggregate_partitions_independently;
    extern const SettingsInt64 max_partitions_to_read;
    extern const SettingsBool use_primary_key;
}
namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 max_concurrent_queries;
    extern const MergeTreeSettingsInt64 max_partitions_to_read;
}
}

namespace DB::QueryPlanOptimizations
{
namespace
{

using Node = ActionsDAG::Node;
using DirectKeyColumns = std::unordered_map<String, DataTypePtr>;

const Node * unwrapAlias(const Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

bool isSupportedKeyType(const IDataType & type)
{
    WhichDataType which(type);
    return which.isNativeInteger() || which.isDateOrDate32OrDateTimeOrDateTime64();
}

/// Exact proofs must describe the same comparison that row execution performs. In particular,
/// do not admit lossy date/time conversions or casts around a key expression.
bool isExactConstant(const Node & constant, const IDataType & key_type)
{
    if (constant.type != ActionsDAG::ActionType::COLUMN || !constant.column || !constant.is_deterministic_constant)
        return false;

    if (constant.result_type->getName() == key_type.getName())
        return true;

    if (!WhichDataType(key_type).isNativeInteger() || !WhichDataType(constant.result_type).isNativeInteger())
        return false;

    const Field value = constant.column->getField();
    if (value.getType() != Field::Types::UInt64 && value.getType() != Field::Types::Int64)
        return false;

    /// Native integer conversion cannot parse or throw; an unrepresentable bound returns NULL.
    return !convertFieldToType(value, key_type, constant.result_type.get(), {}, /*strict=*/true).isNull();
}

bool collectPredicateColumns(const Node * node, const DirectKeyColumns & key_columns, NameSet & predicate_columns)
{
    node = unwrapAlias(node);
    if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base || node->is_function_compiled)
        return false;

    const auto name = node->function_base->getName();
    if (name == "and")
    {
        if (node->children.empty())
            return false;
        for (const auto * child : node->children)
            if (!collectPredicateColumns(child, key_columns, predicate_columns))
                return false;
        return true;
    }

    if (name != "equals" && name != "less" && name != "lessOrEquals" && name != "greater" && name != "greaterOrEquals")
        return false;
    if (node->children.size() != 2)
        return false;

    const auto * input = unwrapAlias(node->children[0]);
    const auto * constant = unwrapAlias(node->children[1]);
    if (constant->type == ActionsDAG::ActionType::INPUT)
        std::swap(input, constant);
    if (input->type != ActionsDAG::ActionType::INPUT)
        return false;

    auto key = key_columns.find(input->result_name);
    if (key == key_columns.end() || input->result_type->getName() != key->second->getName()
        || !isExactConstant(*constant, *key->second))
        return false;

    predicate_columns.insert(input->result_name);
    return true;
}

ReadFromMergeTree * findRead(QueryPlan::Node & node)
{
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(node.step.get()))
        return reading;
    if (node.children.size() != 1
        || (!typeid_cast<ExpressionStep *>(node.step.get()) && !typeid_cast<FilterStep *>(node.step.get())))
        return nullptr;
    return findRead(*node.children.front());
}

bool canSplitRead(const ReadFromMergeTree & reading, const QueryPlanOptimizationSettings & settings)
{
    const auto context = reading.getContext();
    const auto metadata = reading.getStorageMetadata();
    const auto & data = reading.getMergeTreeData();

    if (settings.make_distributed_plan || settings.serialize_query_plan || settings.enable_parallel_replicas
        || settings.aggregation_in_order || context->getSettingsRef()[Setting::distributed_index_analysis]
        || reading.isParallelReadingEnabled() || reading.isParallelReadingFromReplicas()
        || reading.getDistributedReadBucketCount() || reading.hasPinnedBlockNumbers())
        return false;

    if (!context->getSettingsRef()[Setting::use_primary_key] || context->getCurrentTransaction()
        || reading.isQueryWithFinal() || reading.isQueryWithSampling() || reading.getQueryInfo().isStream()
        || reading.readsInOrder() || reading.willOutputEachPartitionThroughSeparatePort()
        || reading.getTopKFilterInfo() || reading.getVectorSearchParameters() || !reading.getIndexReadTasks().empty())
        return false;

    if (metadata->hasProjections() || metadata->hasUniqueKey() || reading.getRowLevelFilter()
        || !reading.getStorageID().hasDatabase() || getEffectiveRowPolicyFilter(data, context)
        || data.hasEnabledMaskingPolicies(context))
        return false;

    /// These limits are checked per read. Splitting the read must not turn one table access into
    /// two independent concurrency registrations or weaken a partition-count limit.
    const auto & data_settings = *data.getSettings();
    if (data_settings[MergeTreeSetting::max_concurrent_queries]
        || data_settings[MergeTreeSetting::max_partitions_to_read] > 0
        || context->getSettingsRef()[Setting::max_partitions_to_read] > 0)
        return false;

    if (const auto & mutations = reading.getMutationsSnapshot(); mutations
        && (mutations->hasDataMutations() || mutations->hasAlterMutations() || mutations->hasMetadataMutations()
            || mutations->hasPatchParts() || mutations->hasLightweightDeletedMask()))
        return false;

    for (const auto & name : reading.getAllColumnNames())
        if (!metadata->getColumns().hasPhysical(name))
            return false;

    for (const auto & part : reading.getParts())
        if (part.parent_part || part.data_part->isProjectionPart() || part.data_part->hasLightweightDelete()
            || part.read_hints.use_vector_search_result_filter)
            return false;

    return !reading.getParts().empty();
}

bool canSplitReadDefaults(const ReadFromMergeTree & reading, const RangesInDataParts & parts)
{
    const auto & columns = reading.getStorageMetadata()->getColumns();
    const auto & names = reading.getAllColumnNames();
    if (std::none_of(names.begin(), names.end(), [&](const auto & name) { return columns.hasDefault(name); }))
        return true;

    NamesAndTypesList required_columns;
    for (const auto & name : names)
        required_columns.push_back(columns.getPhysical(name));

    ContextMutablePtr default_context;
    for (const auto & part : parts)
    {
        /// Frozen type defaults can suppress the current expression even for transitive dependencies.
        if (!part.data_part->getSerializationInfos().getMissingColumns().empty())
            return false;

        bool needs_defaults = false;
        for (const auto & column : required_columns)
        {
            auto stored = part.data_part->tryGetColumn(column.name);
            if (!stored)
                needs_defaults |= columns.hasDefault(column.name);
            else if (!stored->type->equals(*column.type) || !part.data_part->hasColumnFiles(*stored))
                return false;
        }
        if (!needs_defaults)
            continue;

        /// Follow the reader's dependencies, stopping at columns already stored in this part.
        /// Combining the available columns of different parts could invent defaults that no reader evaluates.
        LoadedMergeTreeDataPartInfoForReader part_info(part.data_part, nullptr);
        Names dependency_columns = names;
        injectRequiredColumns(part_info, reading.getStorageSnapshot(), /*with_subcolumns=*/true, dependency_columns);

        Block available_columns;
        for (const auto & name : dependency_columns)
        {
            /// Keep subcolumn extraction and on-read type conversions on the ordinary read path.
            auto column = columns.tryGetPhysical(name);
            if (!column)
                return false;
            auto stored = part.data_part->tryGetColumn(name);
            if (!stored)
                continue;
            if (!stored->type->equals(*column->type) || !part.data_part->hasColumnFiles(*stored))
                return false;
            available_columns.insert(ColumnWithTypeAndName(column->type, name));
        }

        if (!default_context)
        {
            /// Match the reader's context, including settings required by existing table defaults.
            default_context = Context::createCopy(reading.getMergeTreeData().getContext());
            enableAllExperimentalSettings(default_context);
        }

        /// Missing `DEFAULT` / `MATERIALIZED` columns and their transitive defaults execute outside `query.dag`.
        /// Reuse their recursive reader DAG so the check also sees functions inside lambda bodies.
        auto defaults = evaluateMissingDefaults(available_columns, required_columns, columns, default_context);
        /// Each reader builds defaults independently, so even folded query constants such as `now64` must be deterministic.
        if (defaults && (defaults->hasNonDeterministic() || isSensitiveToEvaluationCount(*defaults)))
            return false;
    }
    return true;
}

/// Intersect with selected ranges explicitly: skip indexes can remove marks after primary-key
/// analysis, and selected ranges may bridge gaps which must never become exact ranges.
std::pair<MarkRanges, MarkRanges> splitRanges(const MarkRanges & selected, const MarkRanges & proven)
{
    MarkRanges exact;
    MarkRanges boundary;
    size_t proof_index = 0;
    for (const auto & range : selected)
    {
        size_t cursor = range.begin;
        while (proof_index < proven.size() && proven[proof_index].end <= cursor)
            ++proof_index;

        while (proof_index < proven.size() && proven[proof_index].begin < range.end)
        {
            size_t begin = std::max(cursor, proven[proof_index].begin);
            size_t end = std::min(range.end, proven[proof_index].end);
            if (cursor < begin)
                boundary.emplace_back(cursor, begin);
            if (begin < end)
                exact.emplace_back(begin, end);
            cursor = end;
            if (proven[proof_index].end > range.end)
                break;
            ++proof_index;
        }
        if (cursor < range.end)
            boundary.emplace_back(cursor, range.end);
    }
    return {std::move(exact), std::move(boundary)};
}

ReadFromMergeTree::AnalysisResultPtr withRanges(
    const ReadFromMergeTree::AnalysisResult & original, RangesInDataParts parts, const Names & columns)
{
    auto result = std::make_shared<ReadFromMergeTree::AnalysisResult>(original);
    result->parts_with_ranges = std::move(parts);
    result->split_parts = {};
    result->column_names_to_read = columns;
    result->selected_parts = result->parts_with_ranges.size();
    result->selected_marks = 0;
    result->selected_ranges = 0;
    result->selected_rows = 0;
    result->has_exact_ranges = false;
    for (const auto & part : result->parts_with_ranges)
    {
        result->selected_marks += part.getMarksCount();
        result->selected_ranges += part.ranges.size();
        result->selected_rows += part.getRowsCount();
    }
    return result;
}

}

bool optimizePrimaryKeyFilterElimination(
    QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    const auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating || node.children.size() != 1 || aggregating->getParams().only_merge || aggregating->isMergingSkipped())
        return false;

    auto * original_root = node.children.front();
    auto * reading = findRead(*original_root);
    if (!reading || !canSplitRead(*reading, optimization_settings))
        return false;

    const auto metadata = reading->getStorageMetadata();
    const auto & primary_key = metadata->getPrimaryKey();
    if (!primary_key.expression)
        return false;

    DirectKeyColumns key_columns;
    for (const auto * output : primary_key.expression->getActionsDAG().getOutputs())
    {
        const auto * input = unwrapAlias(output);
        if (input->type != ActionsDAG::ActionType::INPUT || !isSupportedKeyType(*input->result_type)
            || !metadata->getColumns().hasPhysical(input->result_name))
            return false;
        key_columns.emplace(input->result_name, input->result_type);
    }
    if (key_columns.empty())
        return false;

    QueryDAG query;
    if (!query.build(*original_root, /*remove_trivial_wrappers=*/false) || !query.filter_node
        || query.dag->hasNonDeterministic() || query.dag->hasCorrelatedColumns())
        return false;

    /// Splitting the read changes block boundaries and expression evaluation counts, including inside lambdas.
    if (isSensitiveToEvaluationCount(*query.dag))
        return false;

    NameSet predicate_columns;
    if (!collectPredicateColumns(query.filter_node, key_columns, predicate_columns))
        return false;

    /// Analyze exactly the collected runtime filter, with fresh index conditions. Reusing the
    /// original read's indexes could prove only its weaker pruning expression.
    auto proof_dag = ActionsDAG::cloneSubDAG({query.filter_node}, /*remove_aliases=*/true);
    auto payload_dag = std::move(*query.dag);
    auto & outputs = payload_dag.getOutputs();
    outputs.erase(std::find(outputs.begin(), outputs.end(), query.filter_node));

    /// A step's header can also carry columns passed through unchanged by its DAG. Until those
    /// dependencies are represented explicitly, keep such plans on the ordinary read path.
    NameSet output_names;
    for (const auto * output : outputs)
        output_names.insert(output->result_name);
    NamesWithAliases projection;
    for (const auto & name : original_root->step->getOutputHeader()->getNames())
    {
        if (!output_names.contains(name))
            return false;
        projection.emplace_back(name, name);
    }
    payload_dag.project(projection);
    const auto payload_columns = payload_dag.getRequiredColumnsNames();
    if (payload_columns.empty())
        return false;

    const NameSet payload_column_set(payload_columns.begin(), payload_columns.end());
    if (std::none_of(predicate_columns.begin(), predicate_columns.end(),
            [&](const auto & name) { return !payload_column_set.contains(name); }))
        return false;

    /// Preserve the ordinary read's cache pruning and row-limit checks. Its original `PREWHERE`
    /// and filter can have separate cache entries that the reconstructed proof predicate cannot use.
    /// A declined rewrite must leave this analysis installed, not replace it with speculative ranges.
    const auto ordinary_analysis = reading->getOrCreateAnalyzedResult();
    if (!ordinary_analysis->isUsable() || ordinary_analysis->read_type != ReadFromMergeTree::ReadType::Default
        || ordinary_analysis->sampling.use_sampling || !ordinary_analysis->split_parts.layers.empty()
        || ordinary_analysis->selected_marks < 8)
        return false;

    const auto context = reading->getContext();
    const auto & data = reading->getMergeTreeData();

    /// The per-partition pass runs later and cannot see through a `Union`. Preserve potentially
    /// applicable partition aggregation without requesting ports or changing the aggregate early.
    if (optimization_settings.aggregate_partitions_independently && !aggregating->isGroupingSets()
        && aggregating->getParams().max_rows_to_group_by == 0
        && isPartitionKeyFunctionOfKeys(metadata->getPartitionKey(), payload_dag, aggregating->getParams().keys)
        && (context->getSettingsRef()[Setting::force_aggregate_partitions_independently]
            || reading->isPartitionIndependentProcessingProfitable(ReadFromMergeTree::ProcessorKind::Aggregation)))
        return false;

    auto query_info = reading->getQueryInfo();
    query_info.prewhere_info = nullptr;
    query_info.row_level_filter = nullptr;
    query_info.filter_actions_dag = std::make_shared<const ActionsDAG>(std::move(proof_dag));
    auto logger = getLogger("optimizePrimaryKeyFilterElimination");

    /// A previous analysis may have attached proofs for a different filter. Primary-key analysis
    /// appends exact ranges, so both the conditions and these result vectors must start fresh.
    auto proof_parts = std::make_shared<RangesInDataParts>(ordinary_analysis->parts_with_ranges);
    for (auto & part : *proof_parts)
    {
        part.exact_ranges.clear();
        part.ranges_snapshot_after_pk_analysis.reset();
    }
    ReadFromMergeTree proof_read(
        std::move(proof_parts), reading->getMutationsSnapshot(),
        reading->getAllColumnNames(), data, data.getSettings(), query_info, reading->getStorageSnapshot(), context,
        reading->getMaxBlockSize(), reading->getNumStreams(), nullptr, logger, nullptr, false);
    proof_read.disableQueryConditionCache();
    auto proof_analysis = proof_read.selectRangesToReadForPredicateElimination();
    if (!proof_analysis->isUsable() || proof_analysis->read_type != ReadFromMergeTree::ReadType::Default
        || proof_analysis->sampling.use_sampling || !proof_analysis->split_parts.layers.empty())
        return false;

    std::unordered_map<const IMergeTreeDataPart *, const RangesInDataPart *> proofs_by_part;
    for (const auto & part : proof_analysis->parts_with_ranges)
        proofs_by_part.emplace(part.data_part.get(), &part);

    RangesInDataParts exact_parts;
    RangesInDataParts boundary_parts;
    size_t selected_marks = 0;
    for (const auto & part : ordinary_analysis->parts_with_ranges)
    {
        auto proof = proofs_by_part.find(part.data_part.get());
        if (proof == proofs_by_part.end())
            continue;

        /// Index analysis can bridge gaps to avoid seeks. Intersect its selection with the
        /// ordinary ranges so that proof collection cannot restore marks pruned by the cache.
        auto selected = splitRanges(part.ranges, proof->second->ranges).first;
        selected_marks += selected.getNumberOfMarks();
        auto [exact, boundary] = splitRanges(selected, proof->second->exact_ranges);
        auto append = [&](RangesInDataParts & destination, MarkRanges ranges)
        {
            if (ranges.empty())
                return;
            auto & copy = destination.emplace_back(part);
            copy.ranges = std::move(ranges);
            copy.exact_ranges.clear();
            copy.ranges_snapshot_after_pk_analysis.reset();
        };
        append(exact_parts, std::move(exact));
        append(boundary_parts, std::move(boundary));
    }

    auto exact_analysis = withRanges(*ordinary_analysis, std::move(exact_parts), payload_columns);
    /// Avoid creating a second read for a handful of granules or a predominantly uncertain scan.
    /// This conservative prototype threshold is deliberately expressed in granules, not rows.
    if (exact_analysis->selected_marks < 8
        || exact_analysis->selected_marks < selected_marks - exact_analysis->selected_marks)
        return false;

    if (!canSplitReadDefaults(*reading, ordinary_analysis->parts_with_ranges))
        return false;

    query_info.filter_actions_dag = nullptr;
    auto exact_read = std::make_unique<ReadFromMergeTree>(
        std::make_shared<RangesInDataParts>(exact_analysis->parts_with_ranges), reading->getMutationsSnapshot(),
        payload_columns, data, data.getSettings(), query_info, reading->getStorageSnapshot(), context,
        reading->getMaxBlockSize(), reading->getNumStreams(), nullptr, logger, exact_analysis, false);
    exact_read->disableQueryConditionCache();
    exact_read->setStepDescription("Primary key proven filter", optimization_settings.max_step_description_length);
    auto exact_expression = std::make_unique<ExpressionStep>(exact_read->getOutputHeader(), std::move(payload_dag));
    if (!blocksHaveEqualStructure(*exact_expression->getOutputHeader(), *original_root->step->getOutputHeader()))
        return false;

    auto & exact_read_node = nodes.emplace_back(QueryPlan::Node{.step = std::move(exact_read)});
    auto & exact_root = nodes.emplace_back(QueryPlan::Node{.step = std::move(exact_expression), .children = {&exact_read_node}});
    if (boundary_parts.empty())
        node.children.front() = &exact_root;
    else
    {
        reading->setAnalyzedResult(withRanges(*ordinary_analysis, std::move(boundary_parts), reading->getAllColumnNames()));
        auto & union_node = nodes.emplace_back(QueryPlan::Node{
            .step = std::make_unique<UnionStep>(SharedHeaders{original_root->step->getOutputHeader(), exact_root.step->getOutputHeader()}),
            .children = {original_root, &exact_root}});
        node.children.front() = &union_node;
    }

    LOG_DEBUG(logger, "Eliminated primary key filter on {} of {} marks, reading {} instead of {} columns",
        exact_analysis->selected_marks, selected_marks, payload_columns.size(), reading->getAllColumnNames().size());
    return true;
}

}
