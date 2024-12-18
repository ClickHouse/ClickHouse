#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsLogical.h>
#include <Interpreters/InterpreterSelectQuery.h>
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

std::shared_ptr<PartitionIdToMaxBlock> getMaxAddedBlocks(ReadFromMergeTree * reading)
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
            filter_node = &dag->addAlias(*filter_node, "_projection_filter");

        auto & outputs = dag->getOutputs();
        outputs.insert(outputs.begin(), filter_node);
    }

    return true;
}

bool analyzeProjectionCandidate(
    ProjectionCandidate & candidate,
    const ReadFromMergeTree & reading,
    const MergeTreeDataSelectExecutor & reader,
    const Names & required_column_names,
    const RangesInDataParts & parts_with_ranges,
    const SelectQueryInfo & query_info,
    const ContextPtr & context,
    const std::shared_ptr<PartitionIdToMaxBlock> & max_added_blocks,
    const ActionsDAG * dag)
{
    MergeTreeData::DataPartsVector projection_parts;
    MergeTreeData::DataPartsVector normal_parts;

    for (const auto & part_with_ranges : parts_with_ranges)
    {
        const auto & created_projections = part_with_ranges.data_part->getProjectionParts();
        auto it = created_projections.find(candidate.projection->name);
        if (it != created_projections.end() && !it->second->is_broken)
            projection_parts.push_back(it->second);
        else
            normal_parts.push_back(part_with_ranges.data_part);
    }

    if (projection_parts.empty())
        return false;

    auto projection_query_info = query_info;
    projection_query_info.prewhere_info = nullptr;
    if (dag)
        projection_query_info.filter_actions_dag = std::make_unique<ActionsDAG>(dag->clone());

    auto projection_result_ptr = reader.estimateNumMarksToRead(
        std::move(projection_parts),
        reading.getMutationsSnapshot()->cloneEmpty(),
        required_column_names,
        candidate.projection->metadata,
        projection_query_info,
        context,
        context->getSettingsRef()[Setting::max_threads],
        max_added_blocks);

    candidate.merge_tree_projection_select_result_ptr = std::move(projection_result_ptr);
    candidate.sum_marks += candidate.merge_tree_projection_select_result_ptr->selected_marks;

    if (!normal_parts.empty())
    {
        /// TODO: We can reuse existing analysis_result by filtering out projection parts
        auto normal_result_ptr = reading.selectRangesToRead(std::move(normal_parts));

        if (normal_result_ptr->selected_marks != 0)
        {
            candidate.sum_marks += normal_result_ptr->selected_marks;
            candidate.merge_tree_ordinary_select_result_ptr = std::move(normal_result_ptr);
        }
    }

    return true;
}

}
}
