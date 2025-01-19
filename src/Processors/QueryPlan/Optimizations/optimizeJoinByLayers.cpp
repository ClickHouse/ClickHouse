#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/TableJoin.h>

namespace DB
{
namespace QueryPlanOptimizations
{

ReadFromMergeTree * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (reading->isQueryWithFinal())
            return nullptr;
        if (reading->readsInOrder())
            return nullptr;

        return reading;
    }

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step) || typeid_cast<ArrayJoinStep *>(step))
        return findReadingStep(*node.children.front());

    if (auto * distinct = typeid_cast<DistinctStep *>(step); distinct && distinct->isPreliminary())
        return findReadingStep(*node.children.front());

    return nullptr;
}


void appendExpression(std::optional<ActionsDAG> & dag, const ActionsDAG & expression)
{
    if (dag)
        dag->mergeInplace(expression.clone());
    else
        dag = expression.clone();
}

/// This function builds a common DAG which is a merge of DAGs from Filter and Expression steps chain.
void buildSortingDAG(QueryPlan::Node & node, std::optional<ActionsDAG> & dag)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (const auto prewhere_info = reading->getPrewhereInfo())
        {
            // std::cerr << "====== Adding prewhere " << std::endl;
            appendExpression(dag, prewhere_info->prewhere_actions);
        }
        return;
    }

    if (node.children.size() != 1)
        return;

    buildSortingDAG(*node.children.front(), dag);

    if (typeid_cast<DistinctStep *>(step))
    {
    }

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        const auto & actions = expression->getExpression();
        appendExpression(dag, actions);
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        appendExpression(dag, filter->getExpression());
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(step))
    {
        const auto & array_joined_columns = array_join->getColumns();

        if (dag)
        {
            std::unordered_set<std::string_view> keys_set(array_joined_columns.begin(), array_joined_columns.end());

            /// Remove array joined columns from outputs.
            /// Types are changed after ARRAY JOIN, and we can't use this columns anyway.
            ActionsDAG::NodeRawConstPtrs outputs;
            outputs.reserve(dag->getOutputs().size());

            for (const auto & output : dag->getOutputs())
            {
                if (!keys_set.contains(output->result_name))
                    outputs.push_back(output);
            }

            dag->getOutputs() = std::move(outputs);
        }
    }
}

void optimizeJoinByLayers(QueryPlan::Node & node)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step)
        return;

    // std::cerr << "optimizeJoinByLayers\n";

    const auto & join = join_step->getJoin();
    auto * hash_join = typeid_cast<HashJoin *>(join.get());
    auto * concurrent_hash_join = typeid_cast<ConcurrentHashJoin *>(join.get());
    if (!hash_join && !concurrent_hash_join)
        return;

    if (!join->isCloneSupported() || join->hasDelayedBlocks())
        return;

    const auto & table_join = join->getTableJoin();
    auto kind = table_join.kind();
    if (!isLeft(kind) && !isRight(kind) && !isInner(kind) && !isFull(kind))
        return;

    if (table_join.strictness() == JoinStrictness::Asof)
        return;

    const auto & clauses = table_join.getClauses();
    if (clauses.size() != 1)
        return;

    // std::cerr << "optimizeJoinByLayers 1\n";

    const auto & clause = clauses[0];

    auto * lhs_reading = findReadingStep(*node.children.front());
    if (!lhs_reading)
        return;

    auto * rhs_reading = findReadingStep(*node.children.back());
    if (!rhs_reading)
        return;
    // std::cerr << "optimizeJoinByLayers 2\n";
    const auto & lhs_pk = lhs_reading->getStorageMetadata()->getPrimaryKey();
    if (lhs_pk.column_names.empty())
        return;

    const auto & rhs_pk = rhs_reading->getStorageMetadata()->getPrimaryKey();
    if (rhs_pk.column_names.empty())
        return;
    // std::cerr << "optimizeJoinByLayers 3\n";

    std::optional<ActionsDAG> lhs_dag;
    buildSortingDAG(*node.children.front(), lhs_dag);

    if (!lhs_dag)
        lhs_dag = ActionsDAG(lhs_reading->getOutputHeader().getColumnsWithTypeAndName());

    std::optional<ActionsDAG> rhs_dag;
    buildSortingDAG(*node.children.back(), rhs_dag);

    if (!rhs_dag)
        rhs_dag = ActionsDAG(rhs_reading->getOutputHeader().getColumnsWithTypeAndName());

    // std::cerr << "optimizeJoinByLayers 4\n";

    std::unordered_map<std::string_view, const ActionsDAG::Node *> lhs_outputs;
    std::unordered_map<std::string_view, const ActionsDAG::Node *> rhs_outputs;

    for (const auto & output : lhs_dag->getOutputs())
        lhs_outputs.emplace(output->result_name, output);
    for (const auto & output : rhs_dag->getOutputs())
        rhs_outputs.emplace(output->result_name, output);

    const auto & lhs_pk_dag = lhs_pk.expression->getActionsDAG();
    auto lhs_matches = matchTrees(lhs_pk_dag.getOutputs(), *lhs_dag, false);
    const auto & rhs_pk_dag = rhs_pk.expression->getActionsDAG();
    auto rhs_matches = matchTrees(rhs_pk_dag.getOutputs(), *rhs_dag, false);

    size_t useful_pk_columns = 0;

    for (size_t pos = 0; pos < lhs_pk_dag.getOutputs().size() && pos < rhs_pk_dag.getOutputs().size(); ++pos)
    {
        size_t keys_size = clause.key_names_left.size();

        for (size_t i = 0; i < keys_size && useful_pk_columns <= pos; ++i)
        {
            const auto & left_name = clause.key_names_left[i];
            const auto & right_name = clause.key_names_right[i];

            // std::cerr << left_name << ' ' << right_name << std::endl;

            auto it = lhs_outputs.find(left_name);
            auto jt = rhs_outputs.find(right_name);
            if (it == lhs_outputs.end() || jt == rhs_outputs.end())
                continue;

            auto lhs_match = lhs_matches.find(it->second);
            auto rhs_match = rhs_matches.find(jt->second);
            if (lhs_match == lhs_matches.end() || rhs_match == rhs_matches.end())
                continue;

            if (lhs_match->second.monotonicity || rhs_match->second.monotonicity)
                continue;

            if (lhs_match->second.node == lhs_pk_dag.getOutputs()[pos] && rhs_match->second.node == rhs_pk_dag.getOutputs()[pos])
                ++useful_pk_columns;
        }

        if (useful_pk_columns < pos)
            break;
    }

    if (!useful_pk_columns)
        return;

    // std::cerr << "optimizeJoinByLayers 5\n";

    auto lhs_analysis_result = lhs_reading->getAnalyzedResult();
    if (!lhs_analysis_result)
    {
        lhs_analysis_result = lhs_reading->selectRangesToRead();
        lhs_reading->setAnalyzedResult(lhs_analysis_result);
    }

    auto rhs_analysis_resut = rhs_reading->getAnalyzedResult();
    if (!rhs_analysis_resut)
    {
        rhs_analysis_resut = rhs_reading->selectRangesToRead();
        rhs_reading->setAnalyzedResult(rhs_analysis_resut);
    }

    auto lhs_parts = lhs_analysis_result->parts_with_ranges;
    const auto & rhs_parts = rhs_analysis_resut->parts_with_ranges;

    size_t num_lhs_parts = lhs_parts.size();
    lhs_parts.reserve(num_lhs_parts + rhs_parts.size());
    for (const auto & part : rhs_parts)
    {
        lhs_parts.push_back(part);
        lhs_parts.back().part_index_in_query += num_lhs_parts;
    }

    auto logger = getLogger("optimizeJoinByLayers");
    auto lhs_split = splitIntersectingPartsRangesIntoLayers(lhs_parts, lhs_reading->getNumStreams(), useful_pk_columns, logger);
    SplitPartsByRanges rhs_split;
    rhs_split.borders = lhs_split.borders;
    for (auto & layer : lhs_split.layers)
    {
        RangesInDataParts lhs_layer;
        RangesInDataParts rhs_layer;
        for (auto & part : layer)
            (part.part_index_in_query < num_lhs_parts ? lhs_layer : rhs_layer).push_back(std::move(part));

        layer = std::move(lhs_layer);
        for (auto & part : rhs_layer)
            part.part_index_in_query -= num_lhs_parts;

        rhs_split.layers.push_back(std::move(rhs_layer));
    }

    lhs_analysis_result->split_parts = std::move(lhs_split);
    rhs_analysis_resut->split_parts = std::move(rhs_split);
    join_step->enableJoinByLayers(useful_pk_columns);
}

}
}
