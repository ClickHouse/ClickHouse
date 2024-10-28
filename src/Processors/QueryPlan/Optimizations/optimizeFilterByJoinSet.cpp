#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/DistinctStep.h>

namespace DB
{
namespace Setting
{
}
}

namespace DB::QueryPlanOptimizations
{

ReadFromMergeTree * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step);)
        return reading;

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
            //std::cerr << "====== Adding prewhere " << std::endl;
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

void optimizeFilterByJoinSet(QueryPlan::Node & node)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step)
        return;

    const auto & join = join_step->getJoin();
    auto * hash_join = typeid_cast<HashJoin *>(join.get());
    if (!hash_join)
        return;

    const auto & table_join = join->getTableJoin();
    const auto & clauses = table_join.getClauses();
    if (clauses.size() != 1)
        return;

    auto * reading = findReadingStep(*node.children.front());
    if (!reading)
        return;

    const auto & pk = reading->getStorageMetadata()->getPrimaryKey();
    if (pk.column_names.empty())
        return;

    std::optional<ActionsDAG> dag;
    buildSortingDAG(node, dag);

    Block left_source_columns;
    if (dag)
        left_source_columns = dag->getResultColumns();
    else
        left_source_columns = reading->getOutputHeader();

    const Block & right_source_columns = node.children.back()->step->getOutputHeader();

    const auto & clause = clauses.front();

    std::vector<ColumnWithTypeAndName> left_columns;
    std::vector<ColumnWithTypeAndName> right_columns;

    size_t keys_size = clause.key_names_left.size();

    for (size_t i = 0; i < keys_size; ++i)
    {
        const auto & left_name = clause.key_names_left[i];
        const auto & right_name = clause.key_names_right[i];

        if (auto * left = left_source_columns.findByName(left_name))
        {
            left_columns.push_back(*left);
            right_columns.push_back(right_source_columns.getByName(right_name));
        }
    }

    
}

}
