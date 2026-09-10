#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Common/assert_cast.h>
#include <Core/Joins.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/getLeastSupertype.h>

#include <Functions/FunctionsLogical.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/TableJoin.h>

#include <Planner/Utils.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

#include <optional>
#include <unordered_map>
#include <vector>

namespace DB::ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace DB::QueryPlanOptimizations
{

namespace
{

auto getInputNodes(const ActionsDAG & filter_dag, const Names & allowed_inputs_names)
{
    std::unordered_set<const ActionsDAG::Node *> allowed_nodes;

    std::unordered_map<std::string_view, std::list<const ActionsDAG::Node *>> inputs_map;
    for (const auto & input_node : filter_dag.getInputs())
        inputs_map[input_node->result_name].emplace_back(input_node);

    for (const auto & name : allowed_inputs_names)
    {
        auto & inputs_list = inputs_map[name];
        if (inputs_list.empty())
            continue;

        allowed_nodes.emplace(inputs_list.front());
        inputs_list.pop_front();
    }

    return allowed_nodes;
}

enum class ExpressionSide : uint8_t
{
    UNKNOWN = 0,
    LEFT,
    RIGHT,
    /// Inputs from both streams, all of them available.
    BOTH,
};

std::unordered_set<const ActionsDAG::Node *> getExpressionInputs(const ActionsDAG::Node * expr)
{
    std::unordered_set<const ActionsDAG::Node *> result;

    std::unordered_set<const ActionsDAG::Node *> visited;
    ActionsDAG::NodeRawConstPtrs nodes_to_process = { expr };
    while (!nodes_to_process.empty())
    {
        const auto * current = nodes_to_process.back();
        nodes_to_process.pop_back();

        visited.insert(current);

        if (current->type == ActionsDAG::ActionType::INPUT)
        {
            result.insert(current);
        }
        else
        {
            for (const auto * child : current->children)
            {
                if (!visited.contains(child))
                    nodes_to_process.push_back(child);
            }
        }
    }
    return result;
}

ExpressionSide getExpressionSide(
    const ActionsDAG::Node * expr,
    const std::unordered_set<const ActionsDAG::Node *> & left_allowed_inputs,
    const std::unordered_set<const ActionsDAG::Node *> & right_allowed_inputs
)
{
    auto inputs = getExpressionInputs(expr);

    /// Whether at least one input comes from the left/right stream with an unchanged type.
    bool has_left = false;
    bool has_right = false;

    /// Whether at least one input is not available from either side (e.g. a USING column whose
    /// type was changed by the JOIN USING clause). We cannot safely assign this expression to one side.
    bool has_unavailable = false;

    for (const auto * input : inputs)
    {
        bool in_left = left_allowed_inputs.contains(input);
        bool in_right = right_allowed_inputs.contains(input);
        has_left |= in_left;
        has_right |= in_right;
        has_unavailable |= !in_left && !in_right;
    }

    if (has_unavailable)
        return ExpressionSide::UNKNOWN;
    if (has_left && !has_right)
        return ExpressionSide::LEFT;
    if (!has_left && has_right)
        return ExpressionSide::RIGHT;
    if (has_left && has_right)
        return ExpressionSide::BOTH;

    return ExpressionSide::UNKNOWN;
}

using JoinConditionParts = std::vector<ActionsDAG>;

struct ExtractedJoinConditions
{
    JoinConditionParts parts;
    /// No conjunct is left for the filter.
    bool trivial_filter = false;
    /// The remaining conjuncts as a new node that replaces the filter column, set only when the filter
    /// column is not kept. `getConjunctionNodes` looks only through a bare `and`, so an alias with the old
    /// name would hide the conjuncts from the pushdown.
    std::optional<String> new_filter_column_name;
};

/// A conjunct left alone once the others moved into the JOIN loses the boolean conversion the
/// enclosing `and` gave it, so it has to be converted explicitly.
const ActionsDAG::Node & createResultPredicate(
    ActionsDAG & filter_dag,
    const ActionsDAG::Node * original_predicate,
    const ActionsDAG::Node * new_predicate_expr)
{
    const auto & node = filter_dag.addBooleanCondition(*new_predicate_expr, original_predicate->result_type, nullptr);
    return filter_dag.addAlias(node, original_predicate->result_name);
};


/// The body of a lambda is a separate `ActionsDAG`, not reachable from the outer one: a
/// non-deterministic call that depends on a lambda argument stays inside it, and only a nullary one
/// is hoisted out. Return that inner DAG so that the walk below can descend into it.
const ActionsDAG * getLambdaBody(const IFunctionBase & function)
{
    if (const auto * expression = typeid_cast<const FunctionExpression *>(&function))
        return &expression->getAcionsDAG();

    if (const auto * capture = typeid_cast<const FunctionCapture *>(&function))
        return &capture->getAcionsDAG();

    return nullptr;
}

/// The `ColumnFunction` a `COLUMN` node holds, if it holds one. A lambda that captures nothing but
/// constants is folded into a constant, and then the lambda exists only as this column value.
const ColumnFunction * tryGetColumnFunction(const IColumn & column)
{
    const IColumn * unwrapped = &column;
    if (const auto * column_const = typeid_cast<const ColumnConst *>(unwrapped))
        unwrapped = &column_const->getDataColumn();

    return typeid_cast<const ColumnFunction *>(unwrapped);
}

/// Whether the subtree rooted at `node` contains a function that can give different results for two
/// rows with equal inputs. Such a conjunct has to stay in the filter: the join condition is evaluated
/// once per join input row rather than once per output row, and with `enable_join_runtime_filters`
/// the expression is additionally cloned into the build-side runtime filter, a second and independent
/// evaluation site. The main filter pushdown refuses to move non-deterministic conjuncts for the same
/// reason.
bool subtreeContainsNonDeterministicFunction(const ActionsDAG::Node * node)
{
    std::vector<const ActionsDAG::Node *> nodes{node};
    std::unordered_set<const ActionsDAG::Node *> visited_nodes;

    /// The columns captured by a folded lambda are not nodes of any `ActionsDAG`, so they need a
    /// worklist of their own.
    std::vector<const IColumn *> columns;
    std::unordered_set<const IColumn *> visited_columns;

    /// Whether `function` itself is non-deterministic; the nodes of its lambda body, if it has one,
    /// are queued for the walk.
    auto is_non_deterministic = [&](const IFunctionBase & function)
    {
        /// A server constant (e.g. `showCertificate`) differs between the nodes of a distributed join.
        if (!function.isDeterministicInScopeOfQuery() || function.isServerConstant())
            return true;

        if (const auto * body = getLambdaBody(function))
            for (const auto & inner : body->getNodes())
                nodes.push_back(&inner);

        return false;
    };

    while (!nodes.empty() || !columns.empty())
    {
        if (!nodes.empty())
        {
            const auto * current = nodes.back();
            nodes.pop_back();

            if (!visited_nodes.insert(current).second)
                continue;

            if (current->type == ActionsDAG::ActionType::FUNCTION && current->function_base)
            {
                if (is_non_deterministic(*current->function_base))
                    return true;
            }
            else if (current->type == ActionsDAG::ActionType::COLUMN && current->column)
            {
                /// A lambda that captures nothing takes no arguments, so it is folded into a `COLUMN`
                /// node holding a `ColumnFunction` and the `FUNCTION` branch above never sees it.
                columns.push_back(current->column.get());
            }

            for (const auto * child : current->children)
                nodes.push_back(child);

            continue;
        }

        const auto * current = columns.back();
        columns.pop_back();

        if (!visited_columns.insert(current).second)
            continue;

        const auto * column_function = tryGetColumnFunction(*current);
        if (!column_function)
            continue;

        if (is_non_deterministic(*column_function->getFunction()))
            return true;

        /// A lambda that captures nothing is hoisted to the outermost level by the planner, so an
        /// enclosing lambda *captures* it. When that enclosing lambda is folded into a constant in
        /// turn, its own child edges are gone from the DAG and the nested lambda is reachable only
        /// through the captured columns.
        for (const auto & captured : column_function->getCapturedColumns())
            if (captured.column)
                columns.push_back(captured.column.get());
    }

    return false;
}

ExtractedJoinConditions extractActionsForJoinCondition(
    ActionsDAG & filter_dag,
    const std::string & filter_name,
    const Names & left_stream_available_columns,
    const Names & right_stream_available_columns,
    const bool allow_dynamic_type_in_join_keys,
    const bool allow_hyperedges,
    const bool filter_column_is_kept
)
{
    auto * predicate = const_cast<ActionsDAG::Node *>(filter_dag.tryFindInOutputs(filter_name));
    if (!predicate)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Output nodes for ActionsDAG do not contain filter column name {}. DAG:\n{}",
            filter_name,
            filter_dag.dumpDAG());

    /// If condition is constant let's do nothing.
    /// It means there is nothing to push down or optimization was already applied.
    if (predicate->type == ActionsDAG::ActionType::COLUMN)
        return {};

    auto left_stream_allowed_nodes = getInputNodes(filter_dag, left_stream_available_columns);
    auto right_stream_allowed_nodes = getInputNodes(filter_dag, right_stream_available_columns);

    /// Extract all conjuncts from filter expression
    auto conjuncts_list = getConjunctsList(predicate);

    ExtractedJoinConditions result;
    std::unordered_set<const ActionsDAG::Node *> conjuncts_to_replace;
    ActionsDAG::NodeRawConstPtrs rejected_conjuncts;
    rejected_conjuncts.reserve(conjuncts_list.size());

    for (const auto * conjunct : conjuncts_list)
    {
        bool is_equality = conjunct->type == ActionsDAG::ActionType::FUNCTION && conjunct->function_base->getName() == "equals";
        if (is_equality)
        {
            if (subtreeContainsNonDeterministicFunction(conjunct))
            {
                rejected_conjuncts.push_back(conjunct);
                continue;
            }

            const auto * lhs = conjunct->children[0];
            const auto * rhs = conjunct->children[1];

            /// Dynamic type in join keys can lead to unexpected results
            if (!allow_dynamic_type_in_join_keys && (hasDynamicType(lhs->result_type) || hasDynamicType(rhs->result_type)))
            {
                rejected_conjuncts.push_back(conjunct);
                continue;
            }

            /// We can't push equality condition into JOIN if types do not have a common super type.
            if (!lhs->result_type->equals(*rhs->result_type)
                && !tryGetLeastSupertype(DataTypes{lhs->result_type, rhs->result_type}))
            {
                rejected_conjuncts.push_back(conjunct);
                continue;
            }

            /// We need to check if arguments are coming from different sides of JOIN
            auto lhs_side = getExpressionSide(lhs, left_stream_allowed_nodes, right_stream_allowed_nodes);
            auto rhs_side = getExpressionSide(rhs, left_stream_allowed_nodes, right_stream_allowed_nodes);

            bool is_join_key = (lhs_side == ExpressionSide::LEFT && rhs_side == ExpressionSide::RIGHT)
                || (lhs_side == ExpressionSide::RIGHT && rhs_side == ExpressionSide::LEFT);

            /// An equality whose sides mix the two inputs (e.g. `t1.a + t2.b = t4.d + t5.e` over a comma join
            /// of five tables) is no key of this join, but once the inner joins are flattened into one query
            /// graph it connects the relations of the two sides, so the join order optimizer can place a join
            /// where it does become a key. Until then it is a residual condition, which needs a hash join.
            bool is_hyperedge = allow_hyperedges && !is_join_key
                && getExpressionSide(conjunct, left_stream_allowed_nodes, right_stream_allowed_nodes) == ExpressionSide::BOTH;

            if (is_join_key || is_hyperedge)
            {
                result.parts.emplace_back(ActionsDAG::cloneSubDAG({ conjunct }, true));
                conjuncts_to_replace.insert(conjunct);
                continue;
            }
        }
        rejected_conjuncts.push_back(conjunct);
    }

    result.trivial_filter = rejected_conjuncts.empty();
    if (!result.parts.empty())
    {
        /// There's a non-empty list of extracted condition parts.
        /// After JOIN step these equalities will always evaluate to true.
        for (const auto * & output : filter_dag.getOutputs())
        {
            auto it = conjuncts_to_replace.find(output);
            if (it != conjuncts_to_replace.end())
            {
                auto const_column = output->result_type->createColumnConst(0, 1);
                output = &filter_dag.addColumn(std::move(const_column), output->result_type, output->result_name);
            }
        }

        const ActionsDAG::Node * new_predicate = nullptr;
        if (rejected_conjuncts.size() == 1)
            new_predicate = rejected_conjuncts.front();
        else if (rejected_conjuncts.size() > 1)
        {
            /// `and` of the remaining conjuncts normalizes the values itself.
            FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
            new_predicate = &filter_dag.addFunction(func_builder_and, std::move(rejected_conjuncts), {});
        }

        if (new_predicate)
        {
            /// The value of the filter column is observable only when it is kept in the output; a filter
            /// itself treats any non-zero `UInt8` as true.
            if (filter_column_is_kept || !isUInt8(removeNullable(new_predicate->result_type)))
            {
                filter_dag.addOrReplaceInOutputs(createResultPredicate(filter_dag, predicate, new_predicate));
            }
            else
            {
                for (const auto * & output : filter_dag.getOutputs())
                    if (output == predicate)
                        output = new_predicate;
                result.new_filter_column_name = new_predicate->result_name;
            }
        }

        filter_dag.removeUnusedActions(/*allow_remove_inputs=*/false);
    }

    return result;
}

}

size_t tryMergeFilterIntoJoinCondition(QueryPlan::Node * parent_node, QueryPlan::Nodes &  /*nodes*/, const Optimization::ExtraSettings &)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter_step = typeid_cast<FilterStep *>(parent.get());
    auto * join_step = typeid_cast<JoinStepLogical *>(child.get());

    if (!filter_step || !join_step)
        return 0;

    auto & join_operator = join_step->getJoinOperator();

    auto kind = join_operator.kind;
    if (kind != JoinKind::Inner && kind != JoinKind::Cross && kind != JoinKind::Comma)
        return 0;

    /// Pushing filter condition into the JOIN can affect the result in case of ANY join.
    /// In ClickHouse all JOINs return columns of both tables, but for SEMI, ANTI joins
    /// it works as ANY join.
    auto strictness = join_operator.strictness;
    if (strictness != JoinStrictness::Unspecified && strictness != JoinStrictness::All)
        return 0;

    /// Merging a condition can make a prepared join storage fail because it no longer recognizes the key.
    auto is_storage_join = child_node->children.size() == 2
        && typeid_cast<JoinStepLogicalLookup *>(child_node->children.back()->step.get()) != nullptr;
    if (is_storage_join)
        return 0;

    const auto & join_header = child->getOutputHeader();
    const auto & left_stream_header = child->getInputHeaders().front();
    const auto & right_stream_header = child->getInputHeaders().back();

    auto get_available_columns = [&join_header](const Block & input_header)
    {
        Names available_input_columns_for_filter;
        available_input_columns_for_filter.reserve(input_header.columns());

        for (const auto & input_column : input_header.getColumnsWithTypeAndName())
        {
            if (!join_header->has(input_column.name))
                continue;

            /// Skip if type is changed. Push down expression expect equal types.
            if (!input_column.type->equals(*join_header->getByName(input_column.name).type))
                continue;

            available_input_columns_for_filter.push_back(input_column.name);
        }

        return available_input_columns_for_filter;
    };

    auto left_stream_available_columns = get_available_columns(*left_stream_header);
    auto right_stream_available_columns = get_available_columns(*right_stream_header);

    const auto & join_settings = join_step->getJoinSettings();
    const bool allow_dynamic_type_in_join_keys = join_settings.allow_dynamic_type_in_join_keys;
    /// Without keys the physical join can only be a hash join with a residual filter.
    const bool allow_hyperedges = TableJoin::isEnabledAlgorithm(join_settings.join_algorithms, JoinAlgorithm::HASH);

    auto & filter_dag = filter_step->getExpression();
    auto [equality_predicates, trivial_filter, new_filter_column_name] = extractActionsForJoinCondition(
        filter_dag,
        filter_step->getFilterColumnName(),
        left_stream_available_columns,
        right_stream_available_columns,
        allow_dynamic_type_in_join_keys,
        allow_hyperedges,
        /*filter_column_is_kept=*/ !filter_step->removesFilterColumn());

    if (equality_predicates.empty())
        return 0;

    for (auto && predicate : equality_predicates)
    {
        join_step->addConditions(std::move(predicate));
    }

    if (kind == JoinKind::Cross || kind == JoinKind::Comma)
        join_operator.kind = JoinKind::Inner;

    /// Remove FilterStep if filter expression is always true
    if (trivial_filter)
    {
        if (filter_step->removesFilterColumn())
            filter_dag.removeUnusedResult(filter_step->getFilterColumnName());
        auto expression_step = std::make_unique<ExpressionStep>(filter_step->getInputHeaders().front(), std::move(filter_dag));
        expression_step->setStepDescription(*filter_step);
        parent_node->step = std::move(expression_step);
    }
    else if (new_filter_column_name)
    {
        auto new_filter_step = std::make_unique<FilterStep>(
            filter_step->getInputHeaders().front(), std::move(filter_dag), *new_filter_column_name, /*remove_filter_column_=*/ true);
        new_filter_step->setStepDescription(*filter_step);
        parent_node->step = std::move(new_filter_step);
    }

    return 2;
}

}
