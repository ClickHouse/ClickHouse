#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
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
#include <Interpreters/TableJoin.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

FutureSet::Hash generateRandomHash()
{
    auto uuid = UUIDHelpers::generateV4();
    FutureSet::Hash hash;
    hash.low64 = UUIDHelpers::getLowBytes(uuid);
    hash.high64 = UUIDHelpers::getHighBytes(uuid);
    return hash;
}

bool hasAnyInSet(const Header & header, NameSet & set)
{
    for (const auto & column : header)
        if (set.contains(column.name))
            return true;

    return false;
}

// std::unordered_set<std::string_view> findUnusedInputs(const ActionsDAG & dag)
// {
//     std::unordered_set<const ActionsDAG::Node *> used;
//     std::stack<const ActionsDAG::Node *> stack;
//     for (const auto * output : dag.getOutputs())
//     {
//         if (!used.emplace(output).second)
//             continue;

//         stack.push(output);
//         while (!stack.empty())
//         {
//             const auto * node = stack.top();
//             stack.pop();

//             for (const auto * child : node->children)
//                 if (!used.emplace(child).second)
//                     stack.push(child);
//         }
//     }

//     std::unordered_set<std::string_view> names;
//     for (const auto * input : dag.getInputs())
//         if (used.contains(input))
//             names.insert(input->result_name);

//     return names;
// }

// ActionsDAG dropInputs(const ActionsDAG & dag, const Header & inputs)
// {
//    std::unordered_set<const ActionsDAG::Node *> split_nodes;
//     for (const auto * input : dag.getInputs())
//         if (inputs.findByName(input->result_name))
//             split_nodes.insert(input);

//     return dag.split(split_nodes).second;
// }

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
    const Header & header,
    const NamePairs & name_pairs,
    std::unique_ptr<QueryPlan> in_source,
    bool transform_null_in,
    SizeLimits size_limits,
    size_t max_size_for_index)
{
    ActionsDAG lhs_dag(header.getColumnsWithTypeAndName());
    std::unordered_map<std::string_view, const ActionsDAG::Node *> lhs_outputs;
    for (const auto & output : lhs_dag.getOutputs())
        lhs_outputs.emplace(output->result_name, output);

    ActionsDAG rhs_dag(in_source->getCurrentHeader().getColumnsWithTypeAndName());
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

bool isJoinConstant(const std::string & name)
{
    return name == "__lhs_const" || name == "__rhs_const";
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

    const auto & join_info = join->getJoinInfo();

    /// Let's allow Strictness::All with a wrong result for now.
    if (join_info.strictness != JoinStrictness::Any && join_info.strictness != JoinStrictness::All)
        return 0;

    /// TODO: support left in the future
    if (!isInner(join_info.kind)/*&& !isLeft(join_info.kind) && !isRight(join_info.kind)*/)
        return 0;

    /// Do not support many condition for now.
    if (!join_info.expression.disjunctive_conditions.empty())
        return 0;

    /// Only equality expressions are supported.
    {
        if (join_info.expression.condition.predicates.empty())
            return 0;

        if (!join_info.expression.condition.residual_conditions.empty())
            return 0;

        for (const auto & predicate : join_info.expression.condition.predicates)
        {
            if (predicate.op != PredicateOperator::Equals)
                return 0;

            /// Looks like filter-push-down works incorrectly if we have a FilterDAG like `__lhs_const IN set` before JOIN
            if (isJoinConstant(predicate.left_node.getColumnName()) || isJoinConstant(predicate.right_node.getColumnName()))
                return 0;
        }
    }

    const auto & required_output_columns = join->getRequiredOutpurColumns();
    NameSet required_output_columns_set(required_output_columns.begin(), required_output_columns.end());

    const auto & join_expression_actions = join->getExpressionActions();
    //auto unused_columns = findUnusedInputs(*join_expression_actions.post_join_actions);

    const auto & left_input_header = join->getInputHeaders().front();
    const auto & right_input_header = join->getInputHeaders().back();

    /// Apply pre-join actions to input headers before checking for required columns.
    // auto left_header = join_expression_actions.left_pre_join_actions->updateHeader(left_input_header);
    // auto right_header = join_expression_actions.right_pre_join_actions->updateHeader(right_input_header);

    bool build_set_from_left_part = false;

    if (isInnerOrLeft(join_info.kind) && !hasAnyInSet(right_input_header, required_output_columns_set))
    {
        /// Transform right to IN
    }
    // else if (isInnerOrRight(join_info.kind) && !hasAnyInSet(left_input_header, required_output_columns_set))
    // {
    //     /// Transform left to IN
    //     build_set_from_left_part = true;
    // }
    else
        return 0;

    const auto & output_header = join->getOutputHeader();

    for (const auto & column_type_and_name : output_header)
    {
        /// Check input and output type
        if (!left_input_header.getByName(column_type_and_name.name).type->equals(*column_type_and_name.type))
            return 0;
    }

    JoinActionRef unused_post_filter(nullptr);
    join->appendRequiredOutputsToActions(unused_post_filter);

    // {
    //     WriteBufferFromOwnString buf;
    //     IQueryPlanStep::FormatSettings s{.out=buf, .write_header=true};
    //     join->describeActions(s);
    //     std::cerr << buf.stringView() << std::endl;
    // }

    QueryPlan::Node * lhs_in_node = makeExpressionNodeOnTopOf(parent_node->children.at(0), std::move(*join_expression_actions.left_pre_join_actions), {}, nodes);
    QueryPlan::Node * rhs_in_node = makeExpressionNodeOnTopOf(parent_node->children.at(1), std::move(*join_expression_actions.right_pre_join_actions), {}, nodes);

    if (build_set_from_left_part)
        std::swap(lhs_in_node, rhs_in_node);

    NamePairs name_pairs;
    name_pairs.reserve(join_info.expression.condition.predicates.size());
    for (const auto & predicate : join_info.expression.condition.predicates)
    {
        name_pairs.push_back(NamePair{predicate.left_node.getColumnName(), predicate.right_node.getColumnName()});
        if (build_set_from_left_part)
            std::swap(name_pairs.back().lhs_name, name_pairs.back().rhs_name);
    }

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

    auto filter_name = in_conversion.dag.getOutputs().front()->result_name;
    lhs_in_node = makeExpressionNodeOnTopOf(lhs_in_node, std::move(in_conversion.dag), filter_name, nodes);

    //auto post_actions = dropInputs(*join_expression_actions.post_join_actions, right_header);
    lhs_in_node = makeExpressionNodeOnTopOf(lhs_in_node, std::move(*join_expression_actions.post_join_actions), {}, nodes);

    auto creating_sets_step = std::make_unique<DelayedCreatingSetsStep>(
        lhs_in_node->step->getOutputHeader(),
        PreparedSets::Subqueries{std::move(in_conversion.set)},
        settings.network_transfer_limits,
        nullptr);

    creating_sets_step->setStepDescription("Create sets after JOIN -> IN optimiation");
    parent = std::move(creating_sets_step);
    parent_node->children = {lhs_in_node};

    /// JoinLogical is replaced to [Expression(left_pre_join_actions), Expression(IN), Expression(post_join_actions), DelayedCreatingSets]
    return 4;
}

}
