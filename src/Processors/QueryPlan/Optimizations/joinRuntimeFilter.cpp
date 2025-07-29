#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>

#include <fmt/format.h>
#include "Core/ColumnWithTypeAndName.h"
#include "Functions/FunctionsLogical.h"
#include "Functions/IFunctionAdaptors.h"
#include "Interpreters/ActionsDAG.h"
#include "Interpreters/Context.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace QueryPlanOptimizations
{

const ActionsDAG::Node & createRuntimeFilterCondition(ActionsDAG & actions_dag, const ColumnWithTypeAndName & key_column, const ContextPtr & context)
{
    auto filter_function = FunctionFactory::instance().get("ignore", context);
    const auto & key_column_node = actions_dag. findInOutputs(key_column.name); //actions_dag.addInput(key_column);
    const auto & condition = actions_dag.addFunction(filter_function, {&key_column_node}, {});
    return actions_dag.addFunction(FunctionFactory::instance().get("not", context), {&condition}, {});
}

void tryAddJoinRuntimeFilter(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & /*optimization_settings*/)
{
    /// Is this a join step?
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return;

    /// Joining two sources?
    if (node.children.size() != 2)
        return;

    /// Check if join can do runtime filtering on left table
    const auto & join_info = join_step->getJoinInfo();
    if (join_info.kind != JoinKind::Inner ||
        join_info.strictness != JoinStrictness::All ||
        (join_info.locality != JoinLocality::Unspecified && join_info.locality != JoinLocality::Global) ||
        !join_info.expression.disjunctive_conditions.empty())
    {
        return;
    }

    QueryPlan::Node * source_a = node.children[0];
    QueryPlan::Node * source_b = node.children[1];

    std::vector<ColumnWithTypeAndName> join_keys_a;
    std::vector<ColumnWithTypeAndName> join_keys_b;

    const auto & join_condition = join_info.expression.condition;
    for (const auto & predicate : join_condition.predicates)
    {
        if (predicate.op != PredicateOperator::Equals)
            return;

        join_keys_a.push_back(predicate.left_node.getColumn());
        join_keys_b.push_back(predicate.right_node.getColumn());
    }

    ActionsDAG filter_dag;

    // Pass all columns from source_a
    for (const auto & column : source_a->step->getOutputHeader()->getColumnsWithTypeAndName())
        filter_dag.addOrReplaceInOutputs(filter_dag.addInput(column));

    String filter_column_name;
    {
        if (join_keys_a.size() == 1)
        {
            const ActionsDAG::Node & filter_condition = createRuntimeFilterCondition(filter_dag, join_keys_a.front(), Context::getGlobalContextInstance());
            filter_dag.addOrReplaceInOutputs(filter_condition);
            filter_column_name = filter_condition.result_name;
        }
        else if (join_keys_a.size() > 1)
        {
            ActionsDAG::NodeRawConstPtrs all_filter_conditions;
            for (const auto & join_key : join_keys_a)
                all_filter_conditions.push_back(&createRuntimeFilterCondition(filter_dag, join_key, Context::getGlobalContextInstance()));

            FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

            const auto & combined_filter_condition = filter_dag.addFunction(func_builder_and, std::move(all_filter_conditions), {});
            filter_dag.addOrReplaceInOutputs(combined_filter_condition);
            filter_column_name = combined_filter_condition.result_name;
        }
    }

//    String filter_column_name = fmt::format("__runtime_join_filter_{}", thread_local_rng());
//    filter_dag.addOrReplaceInOutputs(
//        filter_dag.addColumn(
//            ColumnWithTypeAndName(DataTypeUInt8().createColumnConst(0, 1), std::make_shared<DataTypeUInt8>(), filter_column_name)));
    std::cerr << filter_dag.dumpDAG() << "\n+++++++++++++++++++++++++++\n\n\n";


    auto * filter_a_node = &nodes.emplace_back();
    filter_a_node->step = std::make_unique<FilterStep>(source_a->step->getOutputHeader(), std::move(filter_dag), filter_column_name, true);
    filter_a_node->step->setStepDescription("Apply runtime join filter");
    filter_a_node->children = {source_a};
    node.children = {filter_a_node, source_b};
}

}

}
