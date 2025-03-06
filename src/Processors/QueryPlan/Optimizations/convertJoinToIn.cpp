#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/tuple.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/System/StorageSystemTables.h>


namespace DB::Setting
{
    extern const SettingsBool transform_null_in;
    extern const SettingsUInt64 use_index_for_in_with_subqueries_max_values;
}

namespace DB::QueryPlanOptimizations
{

bool findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (dynamic_cast<ISourceStep *>(step))
    {
        /// might be more cases here
        if (typeid_cast<ReadFromSystemColumns *>(step) || typeid_cast<ReadFromSystemReplicas *>(step)
            || typeid_cast<ReadFromSystemOneBlock *>(step) || typeid_cast<ReadFromSystemTables *>(step))
            return false;
        return true;
    }

    if (node.children.size() != 1)
        return false;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step) || typeid_cast<ArrayJoinStep *>(step))
        return findReadingStep(*node.children.front());

    if (auto * distinct = typeid_cast<DistinctStep *>(step); distinct && distinct->isPreliminary())
        return findReadingStep(*node.children.front());

    return false;
}

size_t tryConvertJoinToIn(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & /*settings*/)
{
    auto & parent = parent_node->step;
    auto * join = typeid_cast<JoinStepLogical *>(parent.get());
    if (!join)
        return 0;
    auto & join_info = join->getJoinInfo();
    if (join_info.strictness != JoinStrictness::All)
        return 0;
    if (!isInner(join_info.kind))
        return 0;

    const auto & left_input_header = join->getInputHeaders().front();
    const auto & right_input_header = join->getInputHeaders().back();
    const auto & output_header = join->getOutputHeader();

    for (const auto & column_with_type_and_name : output_header)
    {
        const auto & column_name = column_with_type_and_name.name;
        bool left = left_input_header.has(column_name);
        bool right = right_input_header.has(column_name);

        /// All come from left side?
        if (!(left && !right))
            return 0;

        /// Check in and out type
        if (!left_input_header.getByName(column_name).type->equals(*column_with_type_and_name.type))
            return 0;
    }

    if (join_info.expression.condition.predicates.empty())
        return 0;

    /// Steps like ReadFromSystemColumns would build set in place, different from here, so return.
    /// TODO: might have stricter filter
    if (!findReadingStep(*parent_node->children.front()))
        return 0;

    /// dag used by In and Filter
    const Header & left_header = parent_node->children.front()->step->getOutputHeader();
    const Header & right_header = parent_node->children.back()->step->getOutputHeader();
    std::optional<ActionsDAG> dag = ActionsDAG(left_header.getColumnsWithTypeAndName());

    std::unordered_map<std::string_view, const ActionsDAG::Node *> outputs;
    for (const auto & output : dag->getOutputs())
        outputs.emplace(output->result_name, output);

    std::vector<const ActionsDAG::Node *> left_columns;
    Header right_predicate_header;
    for (const auto & predicate : join_info.expression.condition.predicates)
    {
        auto it = outputs.find(predicate.left_node.getColumn().name);
        if (it == outputs.end())
            return 0;
        left_columns.push_back(it->second);

        const auto * right_column = right_header.findByName(predicate.right_node.getColumn().name);
        if (!right_column)
            return 0;
        /// column in predicate is null, so get column here
        right_predicate_header.insert(*right_column);
    }

    /// left parameter of IN function
    FunctionOverloadResolverPtr func_tuple_builder =
        std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());
    const ActionsDAG::Node * in_lhs_arg = left_columns.size() == 1 ?
        left_columns.front() :
        &dag->addFunction(func_tuple_builder, std::move(left_columns), {});

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
    const ActionsDAG::Node * in_rhs_arg =
        &dag->addColumn({set_col, std::make_shared<DataTypeSet>(), "set column"});

    /// IN function
    auto func_in = FunctionFactory::instance().get("in", context);
    auto & in_node = dag->addFunction(func_in, {in_lhs_arg, in_rhs_arg}, "");
    dag->getOutputs().push_back(&in_node);

    /// Attach IN to FilterStep
    auto filter_step = std::make_unique<FilterStep>(left_header,
        std::move(*dag),
        in_node.result_name,
        false);
    filter_step->setStepDescription("WHERE");

    /// CreatingSetsStep as root
    Headers input_headers{output_header};
    auto creating_sets_step = std::make_unique<CreatingSetsStep>(input_headers);
    creating_sets_step->setStepDescription("Create sets before main query execution");

    /// creating_set_step as right subtree
    auto creating_set_step = future_set->build(right_predicate_header, context);

    /// Modify the plan tree
    QueryPlan::Node * left_tree = parent_node->children[0];
    QueryPlan::Node * right_tree = parent_node->children[1];
    parent_node->children.clear();
    parent_node->step = std::move(creating_sets_step);

    auto & filter_node = nodes.emplace_back();
    filter_node.step = std::move(filter_step);
    filter_node.children.push_back(left_tree);
    parent_node->children.push_back(&filter_node);

    auto & creating_set_node = nodes.emplace_back();
    creating_set_node.step = std::move(creating_set_step);
    creating_set_node.children.push_back(right_tree);
    parent_node->children.push_back(&creating_set_node);

    return 1;
}

}
