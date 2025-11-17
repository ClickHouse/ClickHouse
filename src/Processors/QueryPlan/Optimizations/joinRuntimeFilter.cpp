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
        filter_argument = &actions_dag.addCast(key_column_node, filter_element_type, {}, nullptr);

    auto filter_function = FunctionFactory::instance().get("__filterContains", /*query_context*/nullptr);
    const auto & condition = actions_dag.addFunction(filter_function, {&filter_name_node, filter_argument}, {});
    return condition;
}

static bool supportsRuntimeFilter(JoinAlgorithm join_algorithm)
{
    /// Runtime filter can only be applied to join algorithms that first read the right side and only after that read the left side.
    return
        join_algorithm == JoinAlgorithm::HASH ||
        join_algorithm == JoinAlgorithm::PARALLEL_HASH;
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
    const auto & join_operator = join_step->getJoinOperator();
    auto & join_algorithms = join_step->getJoinSettings().join_algorithms;
    const bool can_use_runtime_filter =
        (
            (join_operator.kind == JoinKind::Inner && (join_operator.strictness == JoinStrictness::All || join_operator.strictness == JoinStrictness::Any)) ||
            ((join_operator.kind == JoinKind::Left || join_operator.kind == JoinKind::Right) && join_operator.strictness == JoinStrictness::Semi)
        ) &&
        (join_operator.locality == JoinLocality::Unspecified || join_operator.locality == JoinLocality::Local) &&
        std::find_if(join_algorithms.begin(), join_algorithms.end(), supportsRuntimeFilter) != join_algorithms.end();

    if (!can_use_runtime_filter)
        return false;

    QueryPlan::Node * apply_filter_node = node.children[0];
    QueryPlan::Node * build_filter_node = node.children[1];

    ColumnsWithTypeAndName join_keys_probe_side;
    ColumnsWithTypeAndName join_keys_build_side;

    /// Check that there are only equality predicates
    for (const auto & condition : join_operator.expression)
    {
        auto predicate = condition.asBinaryPredicate();
        if (get<0>(predicate) != JoinConditionOperator::Equals)
            return false;
    }

    {
        /// Extract expressions for calculating join on keys
        auto key_dags = join_step->preCalculateKeys(apply_filter_node->step->getOutputHeader(), build_filter_node->step->getOutputHeader());
        if (key_dags)
        {
            auto get_node_column_with_type_and_name = [](const auto * e) { return ColumnWithTypeAndName(e->result_type, e->result_name); };
            join_keys_probe_side = std::ranges::to<ColumnsWithTypeAndName>(key_dags->first.keys | std::views::transform(get_node_column_with_type_and_name));
            join_keys_build_side = std::ranges::to<ColumnsWithTypeAndName>(key_dags->second.keys | std::views::transform(get_node_column_with_type_and_name));
            if (!isPassthroughActions(key_dags->first.actions_dag))
                makeExpressionNodeOnTopOf(*apply_filter_node, std::move(key_dags->first.actions_dag), nodes, makeDescription("Calculate left join keys"));
            if (!isPassthroughActions(key_dags->second.actions_dag))
                makeExpressionNodeOnTopOf(*build_filter_node, std::move(key_dags->second.actions_dag), nodes, makeDescription("Calculate right join keys"));
        }
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
                    optimization_settings.join_runtime_filter_exact_values_limit,
                    optimization_settings.join_runtime_bloom_filter_bytes,
                    optimization_settings.join_runtime_bloom_filter_hash_functions);
                new_build_filter_node->step->setStepDescription(fmt::format("Build runtime join filter on {} ({})", join_key_build_side.name, filter_name), 200);
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

    node.children = {apply_filter_node, build_filter_node};

    /// Remove algorithms that are not compatible with runtime filters
    std::erase_if(join_algorithms, [](auto join_algorithm) { return !supportsRuntimeFilter(join_algorithm); });

    return true;
}

}

}
