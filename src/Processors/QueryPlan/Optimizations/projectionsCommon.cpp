#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Common/logger_useful.h>
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

    if (reading->isParallelReadingEnabled())
        return false;

    if (reading->readsInOrder())
        return false;

    const auto & query_settings = reading->getContext()->getSettingsRef();

    // Currently projection don't support deduplication when moving parts between shards.
    if (query_settings[Setting::allow_experimental_query_deduplication])
        return false;

    // Currently projection don't support settings which implicitly modify aggregate functions.
    if (query_settings[Setting::aggregate_functions_null_for_empty])
        return false;

    /// Don't use projections if have mutations to apply
    /// because we need to apply them on original data.
    if (query_settings[Setting::apply_mutations_on_fly] && reading->getMutationsSnapshot()->hasDataMutations())
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
    if (dag)
        dag->mergeInplace(expression.clone());
    else
        dag = expression.clone();
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
        if (const auto & prewhere_info = reading->getPrewhereInfo())
        {
            if (prewhere_info->row_level_filter)
            {
                appendExpression(*prewhere_info->row_level_filter);
                if (const auto * filter_expression = findInOutputs(*dag, prewhere_info->row_level_column_name, false))
                    filter_nodes.push_back(filter_expression);
                else
                    return false;
            }

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

    auto projection_result_ptr = reader.estimateNumMarksToRead(
        std::move(projection_parts),
        empty_mutations_snapshot,
        required_column_names,
        candidate.projection->metadata,
        projection_query_info,
        context,
        context->getSettingsRef()[Setting::max_threads]);

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

void filterPartsUsingProjection(
    const ProjectionDescription & projection,
    const MergeTreeDataSelectExecutor & reader,
    MergeTreeData::MutationsSnapshotPtr empty_mutations_snapshot,
    ReadFromMergeTree::AnalysisResult & parent_reading_select_result,
    const SelectQueryInfo & projection_query_info,
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
    auto projection_result_ptr = reader.estimateNumMarksToRead(
        std::move(projection_parts),
        empty_mutations_snapshot,
        {},
        projection.metadata,
        projection_query_info,
        context,
        context->getSettingsRef()[Setting::max_threads],
        nullptr);

    /// Projection has no filtering effect, skip it
    if (projection_result_ptr->selected_marks == projection_marks_to_read)
        return;

    for (auto & part : projection_result_ptr->parts_with_ranges)
        valid_parts.emplace(part.data_part->getParentPart());

    /// Remove ranges whose data parts are fully filtered by projection.
    size_t filtered_parts = filterPartsByProjection(parent_reading_select_result, valid_parts);

    if (filtered_parts > 0)
    {
        auto & stats = parent_reading_select_result.projection_stats.emplace_back();
        stats.name = projection.name;
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

}
}
