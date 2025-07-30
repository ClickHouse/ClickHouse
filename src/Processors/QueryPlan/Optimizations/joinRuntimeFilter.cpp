#include <memory>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Core/Settings.h>
#include "Common/thread_local_rng.h"
#include <Common/logger_useful.h>

#include <fmt/format.h>
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeString.h"
#include "Functions/FunctionsLogical.h"
#include "Functions/IFunctionAdaptors.h"
#include "Interpreters/ActionsDAG.h"
#include "Interpreters/Context.h"
#include "Processors/QueryPlan/BuildRuntimeFilterStep.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace QueryPlanOptimizations
{

const ActionsDAG::Node & createRuntimeFilterCondition(ActionsDAG & actions_dag, const String & filter_name, const ColumnWithTypeAndName & key_column)
{
    const auto & filter_name_node = actions_dag.addColumn(
        ColumnWithTypeAndName(
            DataTypeString().createColumnConst(0, filter_name),
            std::make_shared<DataTypeString>(),
            filter_name));

    const auto & key_column_node = actions_dag. findInOutputs(key_column.name);

    auto filter_function = FunctionFactory::instance().get("filterContains", /*query_context*/nullptr);
    const auto & condition = actions_dag.addFunction(filter_function, {&filter_name_node, &key_column_node}, {});
    return condition;
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

    const String filter_name_prefix = fmt::format("_runtime_filter_{}", thread_local_rng());

    QueryPlan::Node * apply_filter_a_node = nullptr;
    QueryPlan::Node * build_fliter_b_node = source_b;
    {
        ActionsDAG filter_dag;

        /// Pass all columns from source_a
        for (const auto & column : source_a->step->getOutputHeader()->getColumnsWithTypeAndName())
            filter_dag.addOrReplaceInOutputs(filter_dag.addInput(column));

        String filter_column_name;
        ActionsDAG::NodeRawConstPtrs all_filter_conditions;
        for (size_t i = 0; i < join_keys_a.size(); ++i)
        {
            /// Make unique filter name that will be used at runtime to "connect" build side and apply side
            const String filter_name = filter_name_prefix + "_" + toString(i);

            /// Add filter lookup to the left subtree
            {
                const auto & join_key = join_keys_a[i];
                all_filter_conditions.push_back(&createRuntimeFilterCondition(filter_dag, filter_name, join_key));
            }

            /// Add building filter to the right subtree of join
            {
                QueryPlan::Node * build_filter_node = nullptr;
                build_filter_node = &nodes.emplace_back();
                build_filter_node->step = std::make_unique<BuildRuntimeFilterStep>(source_b->step->getOutputHeader(), join_keys_b[i].name, filter_name);
                build_filter_node->step->setStepDescription(fmt::format("Build runtime join filter on {} ({})", join_keys_b[i].name, filter_name));
                build_filter_node->children = {build_fliter_b_node};

                build_fliter_b_node = build_filter_node;
            }
        }

        if (all_filter_conditions.size() == 1)
        {
            filter_dag.addOrReplaceInOutputs(*all_filter_conditions.front());
            filter_column_name = all_filter_conditions.front()->result_name;
        }
        else if (all_filter_conditions.size() > 1)
        {
            FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

            const auto & combined_filter_condition = filter_dag.addFunction(func_builder_and, std::move(all_filter_conditions), {});
            filter_dag.addOrReplaceInOutputs(combined_filter_condition);
            filter_column_name = combined_filter_condition.result_name;
        }

//        std::cerr << filter_dag.dumpDAG() << "\n+++++++++++++++++++++++++++\n\n\n";

        apply_filter_a_node = &nodes.emplace_back();
        apply_filter_a_node->step = std::make_unique<FilterStep>(source_a->step->getOutputHeader(), std::move(filter_dag), filter_column_name, true);
        apply_filter_a_node->step->setStepDescription("Apply runtime join filter");
        apply_filter_a_node->children = {source_a};
    }

    node.children = {apply_filter_a_node, build_fliter_b_node};
}

}

}
