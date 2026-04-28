#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromCountByGranularity.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/logger_useful.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

ReadFromMergeTree * findReadingStepNoFilter(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();

    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        return reading;

    if (node.children.empty())
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step))
        return findReadingStepNoFilter(*node.children.front());

    return nullptr;
}

}

void optimizeCountByGranularity(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings &)
{
    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return;

    if (aggregating->isGroupingSets())
        return;

    const auto & params = aggregating->getParams();

    if (params.aggregates.size() != 1 || params.keys.empty())
        return;

    const auto * count_func = typeid_cast<const AggregateFunctionCount *>(params.aggregates[0].function.get());
    if (!count_func)
        return;

    if (!params.aggregates[0].argument_names.empty())
        return;

    if (node.children.empty())
        return;

    ReadFromMergeTree * reading = findReadingStepNoFilter(*node.children.front());
    if (!reading)
        return;

    if (reading->isQueryWithFinal() || reading->isQueryWithSampling())
        return;

    if (reading->willOutputEachPartitionThroughSeparatePort())
        return;

    if (reading->isParallelReadingFromReplicas())
        return;

    auto metadata = reading->getStorageMetadata();
    const auto & primary_key = metadata->getPrimaryKey();
    if (primary_key.column_names.empty())
        return;

    /// Bail out if any PK column is an expression (not a direct identifier).
    /// We read PK columns by name from parts; expression-based keys would require
    /// evaluating the expression, which we don't do.
    const auto & pk_dag = primary_key.expression->getActionsDAG();
    for (const auto * output : pk_dag.getOutputs())
    {
        if (output->type != ActionsDAG::ActionType::INPUT)
            return;
    }

    /// Bail out if any part has lightweight deletes — getRowsCountInRange counts
    /// physical rows, not visible rows, so we'd overcount.
    for (const auto & part : reading->getParts())
    {
        if (part.data_part->hasLightweightDelete())
            return;
    }

    const auto & sorting_key = metadata->getSortingKey();
    if (sorting_key.column_names.empty())
        return;

    /// Handle PREWHERE on PK columns (includes WHERE pushed to PREWHERE by earlier passes).
    ExpressionActionsPtr prewhere_expression;
    String prewhere_column_name;
    bool has_prewhere = reading->getPrewhereInfo() != nullptr;

    if (has_prewhere)
    {
        const auto & prewhere_info = *reading->getPrewhereInfo();
        std::unordered_set<std::string> pk_columns_set(
            primary_key.column_names.begin(), primary_key.column_names.end());

        for (const auto & dag_node : prewhere_info.prewhere_actions.getNodes())
        {
            if (dag_node.type == ActionsDAG::ActionType::INPUT
                && !pk_columns_set.contains(dag_node.result_name))
                return;
        }

        auto prewhere_dag = prewhere_info.prewhere_actions.clone();
        prewhere_expression = std::make_shared<ExpressionActions>(std::move(prewhere_dag));
        prewhere_column_name = prewhere_info.prewhere_column_name;

        if (!reading->getAnalyzedResult() || !reading->getAnalyzedResult()->has_exact_ranges)
            reading->selectRangesToRead(/*find_exact_ranges=*/true);

        bool has_any_exact = false;
        for (const auto & part : reading->getParts())
        {
            if (!part.exact_ranges.empty())
            {
                has_any_exact = true;
                break;
            }
        }
        if (!has_any_exact)
            return;
    }

    QueryPlan::Node * child_of_aggregating = node.children.front();
    const ActionsDAG * group_by_dag = nullptr;
    if (auto * expr_step = typeid_cast<ExpressionStep *>(child_of_aggregating->step.get()))
        group_by_dag = &expr_step->getExpression();

    if (!group_by_dag)
        return;

    const auto & sorting_key_dag = sorting_key.expression->getActionsDAG();
    auto matches = matchTrees(sorting_key_dag.getOutputs(), *group_by_dag);
    const auto & sorting_outputs = sorting_key_dag.getOutputs();

    for (size_t i = 0; i < params.keys.size(); ++i)
    {
        if (i >= sorting_outputs.size())
            return;

        const ActionsDAG::Node * group_by_output = nullptr;
        for (const auto * output : group_by_dag->getOutputs())
        {
            if (output->result_name == params.keys[i])
            {
                group_by_output = output;
                break;
            }
        }

        if (!group_by_output)
            return;

        auto match_it = matches.find(group_by_output);
        if (match_it == matches.end() || !match_it->second.node)
            return;

        if (match_it->second.node != sorting_outputs[i])
            return;
    }

    /// Re-read parts after selectRangesToRead may have updated analyzed_result_ptr.
    const auto & parts_with_ranges = reading->getParts();
    if (parts_with_ranges.empty())
        return;

    auto bucket_dag = group_by_dag->clone();

    /// Normalize bucket DAG input names: replace analyzer-qualified names
    /// (__tableN.col_name) with bare PK column names so the bucket expression
    /// can be executed directly on blocks built from PK index columns.
    for (const auto * input_node : bucket_dag.getInputs())
    {
        auto & input_name = const_cast<ActionsDAG::Node *>(input_node)->result_name;
        bool found = false;
        for (const auto & pk_name : primary_key.column_names)
        {
            if (input_name == pk_name)
            {
                found = true;
                break;
            }
            auto dot_pos = input_name.find('.');
            if (dot_pos != String::npos && input_name.substr(dot_pos + 1) == pk_name)
            {
                input_name = pk_name;
                found = true;
                break;
            }
        }
        if (!found)
            return;
    }

    auto bucket_expression = std::make_shared<ExpressionActions>(std::move(bucket_dag));

    Block header_block;
    for (const auto & key_name : params.keys)
    {
        const ActionsDAG::Node * key_output = nullptr;
        for (const auto * output : group_by_dag->getOutputs())
        {
            if (output->result_name == key_name)
            {
                key_output = output;
                break;
            }
        }
        header_block.insert({key_output->result_type->createColumn(), key_output->result_type, key_name});
    }

    auto agg_type = std::make_shared<DataTypeAggregateFunction>(params.aggregates[0].function, DataTypes{}, Array{});
    header_block.insert({agg_type->createColumn(), agg_type, params.aggregates[0].column_name});

    auto output_header = std::make_shared<const Block>(std::move(header_block));

    auto & source_node = nodes.emplace_back();
    source_node.step = std::make_unique<ReadFromCountByGranularity>(
        output_header,
        RangesInDataParts(parts_with_ranges),
        std::move(bucket_expression),
        params.keys,
        primary_key,
        params.aggregates[0].function,
        reading->getStorageSnapshot(),
        reading->getMergeTreeData().getSettings(),
        reading->getContext(),
        reading->getNumStreams(),
        std::move(prewhere_expression),
        prewhere_column_name,
        has_prewhere);

    source_node.step->setStepDescription("Optimized count by primary key granularity");
    source_node.children = {};

    aggregating->requestOnlyMergeForCountByGranularity(output_header);
    node.children.front() = &source_node;

    LOG_DEBUG(getLogger("optimizeCountByGranularity"),
        "Rewrote count() + GROUP BY ({} keys{}) to ReadFromCountByGranularity, {} parts",
        params.keys.size(),
        has_prewhere ? ", with prewhere" : "",
        parts_with_ranges.size());
}

}
