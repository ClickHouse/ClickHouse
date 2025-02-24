#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/tuple.h>
#include <Interpreters/ActionsDAG.h>


namespace DB::Setting
{
    extern const SettingsBool transform_null_in;
    extern const SettingsUInt64 use_index_for_in_with_subqueries_max_values;
}

namespace DB::QueryPlanOptimizations
{

size_t tryConvertJoinToIn(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & /*settings*/)
{
    auto & parent = parent_node->step;
    auto * join = typeid_cast<JoinStepLogical *>(parent.get());
    if (!join)
        return 0;
    auto & join_info = join->getJoinInfo();
    if (join_info.strictness != JoinStrictness::All)
        return 0;
    /// Todo: investigate
    if (join->getJoinSettings().join_use_nulls)
        return 0;

    const auto & left_input_header = join->getInputHeaders().front();
    const auto & right_input_header = join->getInputHeaders().back();
    Header left_predicate_header;
    Header right_predicate_header;
    /// column in predicate is null, so use input's
    for (const auto & predicate : join_info.expression.condition.predicates)
    {
        left_predicate_header.insert(
            left_input_header.getByName(predicate.left_node.getColumn().name));
        right_predicate_header.insert(
            right_input_header.getByName(predicate.right_node.getColumn().name));
    }

    const auto & output_header = join->getOutputHeader();

    bool left = false;
    bool right = false;
    for (const auto & column_with_type_and_name : output_header)
    {
        left |= left_predicate_header.has(column_with_type_and_name.name);
        right |= right_predicate_header.has(column_with_type_and_name.name);
    }

    if (left && right)
        return 0;

    ActionsDAG actions(left_predicate_header.getColumnsWithTypeAndName());

    /// left parameter of IN function
    std::vector<const ActionsDAG::Node *> left_columns = actions.getOutputs();
    FunctionOverloadResolverPtr func_tuple_builder =
        std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());
    const ActionsDAG::Node * in_lhs_arg = left_columns.size() == 1 ?
        left_columns.front() :
        &actions.addFunction(func_tuple_builder, std::move(left_columns), {});

    /// right parameter of IN function
    auto context = join->getContext();
    const auto & settings = context->getSettingsRef();
    auto future_set = std::make_shared<FutureSetFromSubquery>(
        CityHash_v1_0_2::uint128(),
        right_predicate_header.getColumnsWithTypeAndName(),
        settings[Setting::transform_null_in],
        PreparedSets::getSizeLimitsForSet(settings),
        settings[Setting::use_index_for_in_with_subqueries_max_values]);

    chassert(future_set->get() == nullptr);
    ColumnPtr set_col = ColumnSet::create(1, future_set);
    const ActionsDAG::Node * in_rhs_arg = &actions.addColumn({set_col, std::make_shared<DataTypeSet>(), "set column"});

    /// IN function
    auto func_in = FunctionFactory::instance().get("in", context);
    auto & in_node = actions.addFunction(func_in, {in_lhs_arg, in_rhs_arg}, "");
    actions.getOutputs().push_back(&in_node);

    /// Attach IN to FilterStep
    auto filter_step = std::make_unique<FilterStep>(left_predicate_header,
        std::move(actions),
        in_node.result_name,
        false);
    filter_step->setStepDescription("WHERE");

    /// CreatingSetsStep as root
    Headers input_headers{output_header};
    auto creating_sets_step = std::make_unique<CreatingSetsStep>(input_headers);
    creating_sets_step->setStepDescription("Create sets before main query execution");

    /// creating_set_step as right subtree
    auto creating_set_step = future_set->build(right_predicate_header, context);

    /// Replace JoinStepLogical with FilterStep, but keep left subtree and remove right subtree
    parent_node->step = std::move(filter_step);
    QueryPlan::Node * right_tree = parent_node->children[1];
    parent_node->children.pop_back();

    /// CreatingSetsStep should be the root, use swap
    auto & new_root = nodes.back();
    auto & old_root = nodes.emplace_back();
    std::swap(new_root, old_root);
    new_root.step = std::move(creating_sets_step);
    new_root.children.push_back(&old_root);

    /// Attach CreatingSetStep node to the CreatingSetsStep node
    auto & creating_set_node = nodes.emplace_back();
    creating_set_node.step = std::move(creating_set_step);
    creating_set_node.children.push_back(right_tree);
    new_root.children.push_back(&creating_set_node);

    return 1;
}

}
