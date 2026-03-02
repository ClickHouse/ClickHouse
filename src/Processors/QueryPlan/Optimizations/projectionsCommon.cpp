#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsLogical.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/Context.h>
#include <Storages/StorageReplicatedMergeTree.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool aggregate_functions_null_for_empty;
    extern const SettingsBool allow_experimental_query_deduplication;
    extern const SettingsBool apply_mutations_on_fly;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 select_sequential_consistency;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsBool parallel_replicas_support_projection;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool optimize_aggregation_in_order;
    extern const SettingsBool force_aggregation_in_order;
    extern const SettingsUInt64 max_projection_rows_to_use_projection_index;
    extern const SettingsUInt64 min_table_rows_to_use_projection_index;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

namespace QueryPlanOptimizations
{

bool canUseProjectionForReadingStep(ReadFromMergeTree * reading)
{
    if (reading->getAnalyzedResult() && reading->getAnalyzedResult()->readFromProjection())
        return false;

    if (reading->isQueryWithFinal())
        return false;

    if (reading->isQueryWithSampling())
        return false;

    if (reading->readsInOrder())
        return false;

    const auto & query_settings = reading->getContext()->getSettingsRef();

    if (reading->isParallelReadingEnabled())
    {
        bool support_projection = query_settings[Setting::allow_experimental_analyzer]
            && query_settings[Setting::parallel_replicas_local_plan]
            && query_settings[Setting::parallel_replicas_support_projection];

        /// AggregationInOrder may cause local and remote replicas to use different CoordinationModes, which is currently unsupported.
        bool enable_aggregation_in_order = query_settings[Setting::optimize_aggregation_in_order]
            || query_settings[Setting::force_aggregation_in_order];

        if (!support_projection || enable_aggregation_in_order)
            return false;
    }

    // Currently projection don't support deduplication when moving parts between shards.
    if (query_settings[Setting::allow_experimental_query_deduplication])
        return false;

    // Currently projection don't support settings which implicitly modify aggregate functions.
    if (query_settings[Setting::aggregate_functions_null_for_empty])
        return false;

    auto mutations_snapshot = reading->getMutationsSnapshot();

    /// Don't use projections if have mutations to apply
    /// because we need to apply them on original data.
    if (mutations_snapshot->hasDataMutations() || mutations_snapshot->hasPatchParts())
        return false;

    return true;
}

PartitionIdToMaxBlockPtr getMaxAddedBlocks(ReadFromMergeTree * reading)
{
    ContextPtr context = reading->getContext();

    if (context->getSettingsRef()[Setting::select_sequential_consistency])
    {
        if (const auto * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(&reading->getMergeTreeData()))
            return std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
    }

    return {};
}

void QueryDAG::appendExpression(const ActionsDAG & expression)
{
    auto cloned = expression.clone();
    cloned.removeTrivialWrappers();

    if (dag)
        dag->mergeInplace(std::move(cloned));
    else
        dag = std::move(cloned);
}

const ActionsDAG::Node * findInOutputs(ActionsDAG & dag, const std::string & name, bool remove)
{
    auto & outputs = dag.getOutputs();
    for (auto it = outputs.begin(); it != outputs.end(); ++it)
    {
        if ((*it)->result_name == name)
        {
            const auto * node = *it;

            /// We allow to use Null as a filter.
            /// In this case, result is empty. Ignore optimizations.
            if (node->result_type->onlyNull())
                return nullptr;

            if (!isUInt8(removeNullable(removeLowCardinality(node->result_type))))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                    "Illegal type {} of column {} for filter. Must be UInt8 or Nullable(UInt8).",
                    node->result_type->getName(), name);

            if (remove)
            {
                outputs.erase(it);
            }
            else
            {
                ColumnWithTypeAndName col;
                col.name = node->result_name;
                col.type = node->result_type;
                col.column = col.type->createColumnConst(1, 1);
                *it = &dag.addColumn(std::move(col));
            }

            return node;
        }
    }

    return nullptr;
}

bool QueryDAG::buildImpl(QueryPlan::Node & node, ActionsDAG::NodeRawConstPtrs & filter_nodes)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (const auto & row_level_filter = reading->getRowLevelFilter())
        {
            appendExpression(row_level_filter->actions);
            if (const auto * filter_expression = findInOutputs(*dag, row_level_filter->column_name, row_level_filter->do_remove_column))
                filter_nodes.push_back(filter_expression);
            else
                return false;
        }
        if (const auto & prewhere_info = reading->getPrewhereInfo())
        {
            appendExpression(prewhere_info->prewhere_actions);
            if (const auto * filter_expression
                = findInOutputs(*dag, prewhere_info->prewhere_column_name, prewhere_info->remove_prewhere_column))
                filter_nodes.push_back(filter_expression);
            else
                return false;
        }
        return true;
    }

    if (node.children.size() != 1)
        return false;

    if (!buildImpl(*node.children.front(), filter_nodes))
        return false;

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        const auto & actions = expression->getExpression();
        if (actions.hasArrayJoin())
            return false;

        appendExpression(actions);
        return true;
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        const auto & actions = filter->getExpression();
        if (actions.hasArrayJoin())
            return false;

        appendExpression(actions);
        const auto * filter_expression = findInOutputs(*dag, filter->getFilterColumnName(), filter->removesFilterColumn());
        if (!filter_expression)
            return false;

        filter_nodes.push_back(filter_expression);
        return true;
    }

    return false;
}

bool QueryDAG::build(QueryPlan::Node & node)
{
    ActionsDAG::NodeRawConstPtrs filter_nodes;
    if (!buildImpl(node, filter_nodes))
        return false;

    if (!filter_nodes.empty())
    {
        filter_node = filter_nodes.back();

        if (filter_nodes.size() > 1)
        {
            /// Add a conjunction of all the filters.

            FunctionOverloadResolverPtr func_builder_and =
                std::make_unique<FunctionToOverloadResolverAdaptor>(
                    std::make_shared<FunctionAnd>());

            filter_node = &dag->addFunction(func_builder_and, std::move(filter_nodes), {});
        }
        else
        {
            /// Add an alias node around existing filter node.
            ///
            /// This simplifies downstream handling: regardless of whether the filter node is newly constructed
            /// or comes from the original query, we consistently wrap it so that it can be removed uniformly.
            ///
            /// We only remove nodes we added explicitly. Since original filter nodes may be reused elsewhere,
            /// we avoid altering them directly.
            ///
            /// The alias name is prefixed with an underscore to minimize collision risk, as such names
            /// are uncommon and not guaranteed to work in user queries.
            filter_node = &dag->addAlias(*filter_node, "_projection_filter");
        }

        auto & outputs = dag->getOutputs();
        outputs.insert(outputs.begin(), filter_node);
    }

    return true;
}

size_t filterPartsByProjection(
    ReadFromMergeTree::AnalysisResult & reading_select_result, const std::unordered_set<const IMergeTreeDataPart *> & valid_parts)
{
    size_t filtered_parts = 0;
    auto & parts_with_ranges = reading_select_result.parts_with_ranges;
    auto out = parts_with_ranges.begin();

    for (auto it = parts_with_ranges.begin(); it != parts_with_ranges.end(); ++it)
    {
        const auto & part = *it;
        if (valid_parts.contains(part.data_part.get()))
        {
            if (out != it)
                *out = std::move(*it);
            ++out;
        }
        else
        {
            reading_select_result.selected_parts -= 1;
            reading_select_result.selected_marks -= part.getMarksCount();
            reading_select_result.selected_rows -= part.getRowsCount();
            reading_select_result.selected_ranges -= part.ranges.size();
            ++filtered_parts;
        }
    }

    parts_with_ranges.erase(out, parts_with_ranges.end());
    return filtered_parts;
}

bool analyzeProjectionCandidate(
    ProjectionCandidate & candidate,
    const MergeTreeDataSelectExecutor & reader,
    MergeTreeData::MutationsSnapshotPtr empty_mutations_snapshot,
    const Names & required_column_names,
    ReadFromMergeTree::AnalysisResult & parent_reading_select_result,
    const SelectQueryInfo & projection_query_info,
    const ContextPtr & context)
{
    RangesInDataParts projection_parts;
    size_t parent_parts_sum_marks = 0;
    for (const auto & part_with_ranges : parent_reading_select_result.parts_with_ranges)
    {
        const auto & created_projections = part_with_ranges.data_part->getProjectionParts();
        auto it = created_projections.find(candidate.projection->name);
        if (it != created_projections.end() && !it->second->is_broken)
        {
            projection_parts.push_back(RangesInDataPart(
                it->second,
                part_with_ranges.data_part,
                part_with_ranges.part_index_in_query,
                part_with_ranges.part_starting_offset_in_query));
        }
        else
        {
            candidate.parent_parts.emplace(part_with_ranges.data_part.get());
            parent_parts_sum_marks += part_with_ranges.getMarksCount();
        }
    }

    if (projection_parts.empty())
        return false;

    ReadFromMergeTree::AnalysisResultPtr projection_result_ptr = reader.estimateNumMarksToRead(
        std::move(projection_parts),
        empty_mutations_snapshot,
        required_column_names,
        candidate.projection->metadata,
        projection_query_info,
        context,
        context->getSettingsRef()[Setting::max_threads]);

    /// If projection analysis exceeded limits, skip this candidate
    if (!projection_result_ptr->isUsable())
        return false;

    std::unordered_set<const IMergeTreeDataPart *> valid_parts = candidate.parent_parts;
    for (auto & part : projection_result_ptr->parts_with_ranges)
        valid_parts.emplace(part.data_part->getParentPart());

    /// Remove ranges whose data parts are fully filtered by projection.
    size_t filtered_parts = filterPartsByProjection(parent_reading_select_result, valid_parts);

    candidate.selected_parts = projection_result_ptr->selected_parts;
    candidate.selected_marks = projection_result_ptr->selected_marks;
    candidate.selected_ranges = projection_result_ptr->selected_ranges;
    candidate.selected_rows = projection_result_ptr->selected_rows;
    candidate.filtered_parts = filtered_parts;

    candidate.merge_tree_projection_select_result_ptr = std::move(projection_result_ptr);
    candidate.sum_marks += candidate.merge_tree_projection_select_result_ptr->selected_marks + parent_parts_sum_marks;

    return true;
}

void filterPartsAndCollectProjectionCandidates(
    ReadFromMergeTree & reading,
    const ProjectionDescription & projection,
    const MergeTreeDataSelectExecutor & reader,
    MergeTreeData::MutationsSnapshotPtr empty_mutations_snapshot,
    ReadFromMergeTree::AnalysisResult & parent_reading_select_result,
    const SelectQueryInfo & projection_query_info,
    const ActionsDAG::Node * filter_node,
    const ContextPtr & context)
{
    RangesInDataParts projection_parts;
    std::unordered_set<const IMergeTreeDataPart *> valid_parts;

    for (const auto & part_with_ranges : parent_reading_select_result.parts_with_ranges)
    {
        const auto & created_projections = part_with_ranges.data_part->getProjectionParts();
        auto it = created_projections.find(projection.name);
        if (it != created_projections.end() && !it->second->is_broken)
        {
            RangesInDataPart projection_part(
                it->second,
                part_with_ranges.data_part,
                part_with_ranges.part_index_in_query,
                part_with_ranges.part_starting_offset_in_query);

            projection_part.parent_ranges.reserve(part_with_ranges.ranges.size());
            for (const auto & range : part_with_ranges.ranges)
            {
                size_t begin = part_with_ranges.data_part->index_granularity->getMarkStartingRow(range.begin);
                size_t end = part_with_ranges.data_part->index_granularity->getMarkStartingRow(range.end);
                projection_part.parent_ranges.emplace_back(begin, end);
                projection_part.parent_ranges.total_rows += end - begin;
            }
            projection_part.parent_ranges.max_part_offset = part_with_ranges.data_part->rows_count - 1;
            projection_parts.push_back(std::move(projection_part));
        }
        else
        {
            valid_parts.emplace(part_with_ranges.data_part.get());
        }
    }

    if (projection_parts.empty())
        return;

    auto projection_marks_to_read = projection_parts.getMarksCountAllParts();

    /// Always request `_parent_part_offset` for projection analysis, even if the column does not exist. This does not
    /// affect the analysis itself. Later code will not use it as an index if the column is missing.
    static Names required_column_names = {"_parent_part_offset"};
    auto projection_result_ptr = reader.estimateNumMarksToRead(
        std::move(projection_parts),
        empty_mutations_snapshot,
        required_column_names,
        projection.metadata,
        projection_query_info,
        context,
        context->getSettingsRef()[Setting::max_threads],
        nullptr);

    /// Projection has no filtering effect, skip it
    if (projection_result_ptr->selected_marks == projection_marks_to_read)
        return;

    size_t max_projection_rows_to_use_projection_index = context->getSettingsRef()[Setting::max_projection_rows_to_use_projection_index];
    size_t min_table_rows_to_use_projection_index = context->getSettingsRef()[Setting::min_table_rows_to_use_projection_index];
    auto & desc = reading.getProjectionIndexReadDescription();
    bool in_use = false;
    for (auto & part : projection_result_ptr->parts_with_ranges)
    {
        valid_parts.emplace(part.data_part->getParentPart());
        if (projection.sample_block.has("_parent_part_offset") && part.getRowsCount() <= max_projection_rows_to_use_projection_index
            && part.parent_ranges.total_rows >= min_table_rows_to_use_projection_index)
        {
            desc.read_ranges[part.part_index_in_query].push_back(std::move(part));
            in_use = true;
        }
    }

    /// Remove ranges whose data parts are fully filtered by projection.
    size_t filtered_parts = filterPartsByProjection(parent_reading_select_result, valid_parts);

    if (in_use)
    {
        NameSet available_inputs;
        available_inputs.reserve(projection.sample_block.columns());
        for (const auto & column : projection.sample_block)
            available_inputs.emplace(column.name);

        auto prewhere_info = std::make_shared<PrewhereInfo>();
        prewhere_info->prewhere_actions
            = projection_query_info.filter_actions_dag->restrictFilterDAGToInputs(filter_node, available_inputs);
        prewhere_info->need_filter = true;
        prewhere_info->prewhere_column_name = prewhere_info->prewhere_actions.getOutputs().front()->result_name;
        prewhere_info->remove_prewhere_column = true;

        const ActionsDAG::Node * parent_part_offset_node = nullptr;
        for (const auto * input : prewhere_info->prewhere_actions.getInputs())
        {
            if (input->result_name == "_parent_part_offset")
            {
                parent_part_offset_node = input;
                break;
            }
        }

        if (parent_part_offset_node == nullptr)
            parent_part_offset_node = &prewhere_info->prewhere_actions.addInput("_parent_part_offset", std::make_shared<DataTypeUInt64>());

        prewhere_info->prewhere_actions.getOutputs().emplace_back(parent_part_offset_node);
        desc.read_infos.emplace_back(&projection, std::move(prewhere_info));
    }

    if (in_use || filtered_parts > 0)
    {
        auto & stats = parent_reading_select_result.projection_stats.emplace_back();
        stats.name = projection.name;
        if (in_use)
            stats.description = "Projection has been analyzed and will be applied during reading";
        else
            stats.description = "Projection has been analyzed and is used for part-level filtering";
        for (const auto & stat : projection_result_ptr->index_stats)
        {
            if (stat.type == ReadFromMergeTree::IndexType::PrimaryKey)
            {
                stats.condition = stat.condition;
                stats.search_algorithm = stat.search_algorithm;
            }
        }
        stats.selected_parts = projection_result_ptr->selected_parts;
        stats.selected_marks = projection_result_ptr->selected_marks;
        stats.selected_ranges = projection_result_ptr->selected_ranges;
        stats.selected_rows = projection_result_ptr->selected_rows;
        stats.filtered_parts = filtered_parts;
    }
}

void fallbackToLocalProjectionReading(const QueryPlanStepPtr & projection_reading)
{
    /// When parallel replicas is enabled, if the result may contains both the projection stream and the parent part stream.
    /// -------------------------------------------------------------------------------------------
    ///                                                 AggregatingProjection
    ///  ReadFromMergeTree  ---is replaced by--->           ReadFromMergeTree (part)
    ///                                                     ReadFromMergeTree (projection)
    /// -------------------------------------------------------------------------------------------
    /// The coordinator does not support reading from two streams at the moment, so read projections are performed directly on the initial replica.
    auto * reading_from_projection = typeid_cast<ReadFromMergeTree *>(projection_reading.get());
    if (reading_from_projection && reading_from_projection->isParallelReadingEnabled())
        reading_from_projection->clearParallelReadingExtension();
}

}
}
