#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/tuple.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/JoinExpressionActions.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

struct InConversion
{
    ActionsDAG dag;
    std::shared_ptr<FutureSetFromSubquery> set;
};

struct NamePair
{
    std::string_view lhs_name;
    std::string_view rhs_name;
};

using NamePairs = std::vector<NamePair>;

InConversion buildInConversion(
    const SharedHeader & header,
    const NamePairs & name_pairs,
    std::unique_ptr<QueryPlan> in_source,
    bool transform_null_in,
    SizeLimits size_limits,
    size_t max_size_for_index)
{
    ActionsDAG lhs_dag(header->getColumnsWithTypeAndName());
    std::unordered_map<std::string_view, const ActionsDAG::Node *> lhs_outputs;
    for (const auto & output : lhs_dag.getOutputs())
        lhs_outputs.emplace(output->result_name, output);

    ActionsDAG rhs_dag(in_source->getCurrentHeader()->getColumnsWithTypeAndName());
    std::unordered_map<std::string_view, const ActionsDAG::Node *> rhs_outputs;
    for (const auto & output : rhs_dag.getOutputs())
        rhs_outputs.emplace(output->result_name, output);

    rhs_dag.getOutputs().clear();

    std::vector<const ActionsDAG::Node *> left_columns;
    for (const auto & name_pair : name_pairs)
    {
        auto it = lhs_outputs.find(name_pair.lhs_name);
        if (it == lhs_outputs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find left key {} in JOIN step", name_pair.lhs_name);
        left_columns.push_back(it->second);

        auto jt = rhs_outputs.find(name_pair.rhs_name);
        if (jt == rhs_outputs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find left key {} in JOIN step", name_pair.lhs_name);
        rhs_dag.getOutputs().push_back(jt->second);
    }

    auto rhs_expression = std::make_unique<ExpressionStep>(in_source->getCurrentHeader(), std::move(rhs_dag));
    rhs_expression->setStepDescription("JOIN keys");
    in_source->addStep(std::move(rhs_expression));

    /// left parameter of IN function
    FunctionOverloadResolverPtr func_tuple_builder =
        std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());
    const ActionsDAG::Node * in_lhs_arg = left_columns.size() == 1 ?
        left_columns.front() :
        &lhs_dag.addFunction(func_tuple_builder, std::move(left_columns), {});

    auto generateRandomHash =[]()
    {
        auto uuid = UUIDHelpers::generateV4();
        return FutureSet::Hash(UUIDHelpers::getLowBytes(uuid), UUIDHelpers::getHighBytes(uuid));
    };

    /// right parameter of IN function
    auto future_set = std::make_shared<FutureSetFromSubquery>(
        generateRandomHash(),
        nullptr,
        std::move(in_source),
        nullptr,
        nullptr,
        transform_null_in,
        size_limits,
        max_size_for_index);

    ColumnPtr set_col = ColumnSet::create(1, future_set);
    const ActionsDAG::Node * in_rhs_arg =
        &lhs_dag.addColumn({set_col, std::make_shared<DataTypeSet>(), "set column"});

    /// IN function
    auto func_in = FunctionFactory::instance().get("in", nullptr);
    const auto & in_node = lhs_dag.addFunction(func_in, {in_lhs_arg, in_rhs_arg}, "");
    lhs_dag.getOutputs().insert(lhs_dag.getOutputs().begin(), &in_node);

    return {std::move(lhs_dag), std::move(future_set)};
}

static void remapNodes(ActionsDAG::NodeRawConstPtrs & keys, const ActionsDAG::NodeMapping & node_map)
{
    for (const auto *& key : keys)
    {
        if (auto it = node_map.find(key); it != node_map.end())
            key = it->second;
    }
}

static ActionsDAG cloneSubDAGWithHeader(const SharedHeader & stream_header, ActionsDAG second_dag)
{
    ActionsDAG dag(stream_header->getColumnsWithTypeAndName());

    auto outputs = second_dag.getOutputs();

    ActionsDAG::NodeMapping node_map;
    dag.mergeInplace(std::move(second_dag), node_map, true);
    remapNodes(outputs, node_map);

    dag.getOutputs() = outputs;

    return dag;
}

size_t tryConvertJoinToIn(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    auto & parent = parent_node->step;

    if (parent_node->children.size() != 2)
        return 0;

    auto * join = typeid_cast<JoinStepLogical *>(parent.get());
    if (!join)
        return 0;

    /// Let's support only hash algorithm, because full sorting join may be more memory efficient than IN.
    const auto & join_algorithms = join->getJoinSettings().join_algorithms;
    if (!TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::HASH) &&
        !TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::PARALLEL_HASH))
        return 0;

    const auto & join_operator = join->getJoinOperator();

    /// Let's allow Strictness::All with a wrong result for now.
    if (join_operator.strictness != JoinStrictness::Any && join_operator.strictness != JoinStrictness::All)
        return 0;

    /// TODO: support left in the future
    if (!isInner(join_operator.kind)/*&& !isLeft(join_operator.kind) && !isRight(join_operator.kind)*/)
        return 0;

    /// Do not support many condition for now.
    if (join_operator.expression.empty())
        return 0;

    /// Only equality expressions are supported.
    std::vector<std::pair<JoinActionRef, JoinActionRef>> key_pairs;
    for (const auto & predicate : join_operator.expression)
    {
        auto [op, lhs, rhs] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            return 0;

        if (lhs.fromLeft() && rhs.fromRight())
            key_pairs.emplace_back(lhs, rhs);
        else if (lhs.fromRight() && rhs.fromLeft())
            key_pairs.emplace_back(rhs, lhs);
        else
            return 0;
    }

    bool build_set_from_left_part = false;

    auto join_output_actions = join->getOutputActions();
    /// Check output columns come from one side.
    if (!isInnerOrLeft(join_operator.kind) || std::ranges::any_of(join_output_actions, &JoinActionRef::fromRight))
        return 0;

    /// Check input and output type match
    if (!join->typeChangingSides().empty())
        return 0;

    // {
    //     WriteBufferFromOwnString buf;
    //     IQueryPlanStep::FormatSettings s{.out=buf, .write_header=true};
    //     join->describeActions(s);
    //     std::cerr << buf.stringView() << std::endl;
    // }

    if (build_set_from_left_part)
    {
        for (auto & p : key_pairs)
            std::swap(p.first, p.second);
    }

    NamePairs name_pairs;
    for (auto & [lhs, rhs] : key_pairs)
        name_pairs.emplace_back(lhs.getColumnName(), rhs.getColumnName());

    auto left_pre_join_actions = JoinExpressionActions::getSubDAG(key_pairs | std::views::transform([](const auto & key_pair) { return key_pair.first; }));
    auto right_pre_join_actions = JoinExpressionActions::getSubDAG(key_pairs | std::views::transform([](const auto & key_pair) { return key_pair.second; }));
    auto * lhs_in_node = parent_node->children.at(0);
    makeExpressionNodeOnTopOf(*lhs_in_node, std::move(left_pre_join_actions), nodes, makeDescription("Calculate join left keys"));
    auto * rhs_in_node = parent_node->children.at(1);
    makeExpressionNodeOnTopOf(*rhs_in_node, std::move(right_pre_join_actions), nodes, makeDescription("Calculate join right keys"));
    parent_node->children.pop_back();

    /// Join equality does not match Nulls.
    /// In case we support NullSafeEquals, we should set transform_null_in = true.
    /// But it would require proper support for sets with multiple keys.
    bool transform_null_in = false;

    auto in_conversion = buildInConversion(
        lhs_in_node->step->getOutputHeader(),
        name_pairs,
        std::make_unique<QueryPlan>(QueryPlan::extractSubplan(rhs_in_node, nodes)),
        transform_null_in,
        settings.network_transfer_limits,
        settings.use_index_for_in_with_subqueries_max_values);

    {
        auto filter_name = in_conversion.dag.getOutputs().front()->result_name;
        const auto & header = lhs_in_node->step->getOutputHeader();
        auto step = std::make_unique<FilterStep>(header, std::move(in_conversion.dag), filter_name, true);
        lhs_in_node = &nodes.emplace_back(QueryPlan::Node{std::move(step), {lhs_in_node}});
    }

    auto output_header = lhs_in_node->step->getOutputHeader();
    auto creating_sets_step = std::make_unique<DelayedCreatingSetsStep>(
        output_header,
        PreparedSets::Subqueries{std::move(in_conversion.set)},
        settings.network_transfer_limits,
        nullptr);

    auto join_output_actions_dag = cloneSubDAGWithHeader(output_header, JoinExpressionActions::getSubDAG(join_output_actions));
    creating_sets_step->setStepDescription("Create sets after JOIN -> IN optimiation");
    parent = std::move(creating_sets_step);
    parent_node->children = {lhs_in_node};

    makeExpressionNodeOnTopOf(*parent_node, std::move(join_output_actions_dag), nodes, makeDescription("Join output actions"));

    /// JoinLogical is replaced to [Expression(left_pre_join_actions), Expression(IN), DelayedCreatingSets, Expression(join_output_actions)]
    return 5;
}

}
