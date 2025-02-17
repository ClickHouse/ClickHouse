#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>


namespace DB::Setting
{
    extern const SettingsBool transform_null_in;
    extern const SettingsUInt64 use_index_for_in_with_subqueries_max_values;
}

namespace DB::QueryPlanOptimizations
{

size_t tryConvertJoinToIn(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes[[maybe_unused]], const Optimization::ExtraSettings & /*settings*/)
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
    const auto & output_header = join->getOutputHeader();

    bool left = false;
    bool right = false;
    for (const auto & column_with_type_and_name : output_header)
    {
        left |= left_input_header.has(column_with_type_and_name.name);
        right |= right_input_header.has(column_with_type_and_name.name);
    }

    if (left && right)
        return 0;

    return 0;

    // std::cout<<"can optimize"<<std::endl;

    // auto context = join->getContext();


    // ActionsDAG actions(left_input_header.getColumnsWithTypeAndName());

    // std::vector<const ActionsDAG::Node *> left_columns = actions.getOutputs();
    // const ActionsDAG::Node * in_lhs_arg = left_columns.front();

    // // std::vector<ColumnWithTypeAndName> right_columns;

    // // auto test_set = std::make_shared<FutureSetFromTuple>(Block(right_columns), context->getSettingsRef());
    // // auto column_set = ColumnSet::create(1, std::move(test_set));
    // // ColumnPtr set_col = ColumnConst::create(std::move(column_set), 0);

    // // const ActionsDAG::Node * in_rhs_arg = &actions.addColumn({set_col, std::make_shared<DataTypeSet>(), {}});


    // auto func_in = FunctionFactory::instance().get("in", context);
    // actions.addFunction(func_in, {in_lhs_arg, nullptr/*in_rhs_arg*/}, {});

    // QueryPlan subquery_plan;
    // auto where_step = std::make_unique<FilterStep>(subquery_plan.getCurrentHeader(),
    //     std::move(actions),
    //     "",
    //     false);
    // // appendSetsFromActionsDAG(where_step->getExpression(), useful_sets);
    // where_step->setStepDescription("WHERE");
    // subquery_plan.addStep(std::move(where_step));


    // const auto & settings = context->getSettingsRef();
    // auto future_set = std::make_shared<FutureSetFromSubquery>(
    //     /*set.hash*/CityHash_v1_0_2::uint128(), nullptr,
    //     std::make_unique<QueryPlan>(std::move(subquery_plan)),
    //     nullptr, nullptr,
    //     settings[Setting::transform_null_in],
    //     PreparedSets::getSizeLimitsForSet(settings),
    //     settings[Setting::use_index_for_in_with_subqueries_max_values]);

    // PreparedSets::Subqueries subqueries;
    // auto step = std::make_unique<DelayedCreatingSetsStep>(
    //     output_header,
    //     std::move(subqueries),
    //     context);

    // parent_node->step = std::move(step);
    // // parent_node->children.swap(child_node->children);

    // return 0;
}

}
