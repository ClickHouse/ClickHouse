#include <memory>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Settings.h>
#include <Common/thread_local_rng.h>
#include <Common/logger_useful.h>
#include <DataTypes/getLeastSupertype.h>
#include <fmt/format.h>


namespace DB
{

namespace QueryPlanOptimizations
{

const ActionsDAG::Node & createRuntimeFilterCondition(ActionsDAG & actions_dag, const String & filter_name, const ColumnWithTypeAndName & key_column, const DataTypePtr & filter_element_type)
{
    const auto & filter_name_node = actions_dag.addColumn(
        ColumnWithTypeAndName(
            DataTypeString().createColumnConst(0, filter_name),
            std::make_shared<DataTypeString>(),
            filter_name));

    const auto & key_column_node = actions_dag. findInOutputs(key_column.name);
    const auto * filter_argument = &key_column_node;

    /// Cast to the type of filter element if needed
    if (!key_column.type->equals(*filter_element_type))
        filter_argument = &actions_dag.addCast(key_column_node, filter_element_type, {});

    auto filter_function = FunctionFactory::instance().get("filterContains", /*query_context*/nullptr);
    const auto & condition = actions_dag.addFunction(filter_function, {&filter_name_node, filter_argument}, {});
    return condition;
}

bool tryAddJoinRuntimeFilter(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Is this a join step?
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return false;

    /// Joining two sources?
    if (node.children.size() != 2)
        return false;

    /// Check if join can do runtime filtering on left table
    const auto & join_info = join_step->getJoinInfo();
    if ((join_info.kind != JoinKind::Inner) ||
        (join_info.strictness != JoinStrictness::All && join_info.strictness != JoinStrictness::Any) ||
        (join_info.locality != JoinLocality::Unspecified && join_info.locality != JoinLocality::Global) ||
        !join_info.expression.disjunctive_conditions.empty())
    {
        return false;
    }

    QueryPlan::Node * apply_filter_node = node.children[0];
    QueryPlan::Node * build_filter_node = node.children[1];

    std::vector<ColumnWithTypeAndName> join_keys_probe_side;
    std::vector<ColumnWithTypeAndName> join_keys_build_side;

    const auto & join_condition = join_info.expression.condition;
    for (const auto & predicate : join_condition.predicates)
    {
        if (predicate.op != PredicateOperator::Equals)
            return false;

        join_keys_probe_side.push_back(predicate.left_node.getColumn());
        join_keys_build_side.push_back(predicate.right_node.getColumn());
    }

    /// Extract expressions for calculating join on keys
    /// Move them into separate nodes
    /// Replace pre-join actions in the join step with pass-through (no-op) actions
    {
        const auto & actions = join_step->getExpressionActions();

        /// Replaces the internals of ActionsDAG with no-op actions that just pass specified columns without any transformations
        /// This is done in-place because JoinActionRef-s store column names and pointers to ActionsDAG-s
        auto replace_with_pass_through_actions = [](ActionsDAG & actions_dag, const Block & header)
        {
            actions_dag = ActionsDAG(); /// Clear the actions DAG
            for (const auto & column : header.getColumnsWithTypeAndName())
                actions_dag.addOrReplaceInOutputs(actions_dag.addInput(column));
        };

        if (actions.left_pre_join_actions)
        {
            apply_filter_node = makeExpressionNodeOnTopOf(apply_filter_node, std::move(*actions.left_pre_join_actions), {}, nodes);
            apply_filter_node->step->setStepDescription("Compute join keys");
            replace_with_pass_through_actions(*actions.left_pre_join_actions, *apply_filter_node->step->getOutputHeader());
            join_step->updateInputHeader(apply_filter_node->step->getOutputHeader(), 0);
        }

        if (actions.right_pre_join_actions)
        {
            build_filter_node = makeExpressionNodeOnTopOf(build_filter_node, std::move(*actions.right_pre_join_actions), {}, nodes);
            apply_filter_node->step->setStepDescription("Compute join keys");
            replace_with_pass_through_actions(*actions.right_pre_join_actions, *build_filter_node->step->getOutputHeader());
            join_step->updateInputHeader(build_filter_node->step->getOutputHeader(), 1);
        }
    }

    const bool swap_join_tables = join_step->areInputsSwapped();
    if (swap_join_tables)
    {
        std::swap(build_filter_node, apply_filter_node);
        std::swap(join_keys_build_side, join_keys_probe_side);
    }

    const String filter_name_prefix = fmt::format("_runtime_filter_{}", thread_local_rng());

    {
        ActionsDAG filter_dag;

        /// Pass all columns on probe side
        for (const auto & column : apply_filter_node->step->getOutputHeader()->getColumnsWithTypeAndName())
            filter_dag.addOrReplaceInOutputs(filter_dag.addInput(column));

        String filter_column_name;
        ActionsDAG::NodeRawConstPtrs all_filter_conditions;
        for (size_t i = 0; i < join_keys_build_side.size(); ++i)
        {
            /// Make unique filter name for each of the predicates. If will be used at runtime to "connect" build side and apply side
            const String filter_name = filter_name_prefix + "_" + toString(i);

            const auto & join_key_build_side = join_keys_build_side[i];
            const auto & join_key_probe_side = join_keys_probe_side[i];

            /// If types of left and right columns do not match then we need to deduce common super type for them
            /// and add CAST-s to this type to build and apply sides
            DataTypePtr common_type;
            if (!join_key_build_side.type->equals(*join_key_probe_side.type))
            {
                try
                {
                    common_type = getLeastSupertype(DataTypes{join_key_build_side.type, join_key_probe_side.type});
                }
                catch (Exception & ex)
                {
                    ex.addMessage("JOIN cannot infer common type in ON section for keys. Left key '{}' type {}. Right key '{}' type {}",
                        join_key_probe_side.name, join_key_probe_side.type->getName(),
                        join_key_build_side.name, join_key_build_side.type->getName());
                    throw;
                }
            }
            else
            {
                common_type = join_key_build_side.type;
            }

            /// Add filter lookup to the probe subtree
            all_filter_conditions.push_back(&createRuntimeFilterCondition(filter_dag, filter_name, join_key_probe_side, common_type));

            /// Add building filter to the build subtree of join
            {
                QueryPlan::Node * new_build_filter_node = nullptr;
                new_build_filter_node = &nodes.emplace_back();
                new_build_filter_node->step = std::make_unique<BuildRuntimeFilterStep>(
                    build_filter_node->step->getOutputHeader(),
                    join_key_build_side.name,
                    common_type,
                    filter_name,
                    optimization_settings.join_runtime_bloom_filter_bytes,
                    optimization_settings.join_runtime_bloom_filter_hash_functions);
                new_build_filter_node->step->setStepDescription(fmt::format("Build runtime join filter on {} ({})", join_key_build_side.name, filter_name));
                new_build_filter_node->children = {build_filter_node};

                build_filter_node = new_build_filter_node;
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

        QueryPlan::Node * new_apply_filter_node = &nodes.emplace_back();
        new_apply_filter_node->step = std::make_unique<FilterStep>(apply_filter_node->step->getOutputHeader(), std::move(filter_dag), filter_column_name, true);
        new_apply_filter_node->step->setStepDescription("Apply runtime join filter");
        new_apply_filter_node->children = {apply_filter_node};

        apply_filter_node = new_apply_filter_node;
    }

    if (swap_join_tables)
        node.children = {build_filter_node, apply_filter_node};
    else
        node.children = {apply_filter_node, build_filter_node};

    return true;
}

}

}
