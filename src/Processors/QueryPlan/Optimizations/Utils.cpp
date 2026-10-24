#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <unordered_set>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

bool isPassthroughActions(const ActionsDAG & actions_dag)
{
    return actions_dag.getOutputs() == actions_dag.getInputs() && actions_dag.trivial();
}

template <typename Step, typename ...Args>
bool makeExpressionNodeOnTopOfImpl(
    QueryPlan::Node & node, ActionsDAG actions_dag, QueryPlan::Nodes & nodes,
    DescriptionHolderPtr step_description, Args && ...args)
{
    const auto & header = node.step->getOutputHeader();
    if (!header && !actions_dag.getInputs().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create ExpressionStep on top of node without header, dag: {}", actions_dag.dumpDAG());

    QueryPlanStepPtr step = std::make_unique<Step>(header, std::move(actions_dag), std::forward<Args>(args)...);

    if (step_description)
        step_description->setStepDescription(*step);

    auto * new_node = &nodes.emplace_back(std::move(node));
    node = QueryPlan::Node{std::move(step), {new_node}};
    return true;
}

bool makeExpressionNodeOnTopOf(QueryPlan::Node & node, ActionsDAG actions_dag, QueryPlan::Nodes & nodes, DescriptionHolderPtr step_description)
{
    return makeExpressionNodeOnTopOfImpl<ExpressionStep>(node, std::move(actions_dag), nodes, std::move(step_description));
}

bool makeFilterNodeOnTopOf(
    QueryPlan::Node & node, ActionsDAG actions_dag, const String & filter_column_name, bool remove_filer,
    QueryPlan::Nodes & nodes, DescriptionHolderPtr step_description)
{
    if (filter_column_name.empty())
        return makeExpressionNodeOnTopOfImpl<ExpressionStep>(node, std::move(actions_dag), nodes, std::move(step_description));
    return makeExpressionNodeOnTopOfImpl<FilterStep>(node, std::move(actions_dag), nodes, std::move(step_description), filter_column_name, remove_filer);
}

namespace QueryPlanOptimizations
{

FilterResult getFilterResult(const ColumnWithTypeAndName & column)
{
    if (!column.column)
        return FilterResult::UNKNOWN;

    if (!column.type->canBeUsedInBooleanContext())
        return FilterResult::UNKNOWN;

    return column.column->getBool(0) ? FilterResult::TRUE : FilterResult::FALSE;
}

static bool isNotReadySetColumn(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
        return false;

    const ColumnSet * column_set = checkAndGetColumn<const ColumnSet>(&node.column->getDataColumn());
    if (!column_set)
        return false;

    auto future_set = column_set->getData();
    return !future_set || !future_set->get();
}

bool dagContainsNonReadySet(const ActionsDAG & dag)
{
    for (const auto & node : dag.getNodes())
    {
        if (isNotReadySetColumn(node))
            return true;
    }
    return false;
}

bool canHoistGatherThroughStep(const IQueryPlanStep & step)
{
    const ActionsDAG * dag = nullptr;
    if (const auto * expression = typeid_cast<const ExpressionStep *>(&step))
        dag = &expression->getExpression();
    else if (const auto * filter = typeid_cast<const FilterStep *>(&step))
        dag = &filter->getExpression();
    else if (!typeid_cast<const BuildRuntimeFilterStep *>(&step))
        return false;

    /// Per-block functions (rowNumberInAllBlocks, blockNumber, nowInBlock, ...) depend on the whole block
    /// stream; below a gather they would run per shard and produce different values.
    return !(dag && dagContainsNonDeterministicFunction(*dag));
}

bool dagContainsNonDeterministicFunction(const ActionsDAG & dag)
{
    /// We are interested in functions that are non-deterministic *within* a single query --
    /// i.e. functions whose per-row output cannot be predicted from a single plan-time
    /// evaluation. `rand`, `rowNumberInAllBlocks`, `blockNumber`, `nowInBlock` etc. fall in
    /// this group. Functions like `now`/`today`/`yesterday`/`currentUser` are not
    /// deterministic across queries (`isDeterministic() == false`) but they return the same
    /// value for all rows in a single query (`isDeterministicInScopeOfQuery() == true`), so
    /// the optimizer can soundly use their plan-time value and they should NOT block the
    /// JOIN-conversion rewrite.
    /// The walk also looks inside the lambdas of the DAG - a non-deterministic call that depends on a
    /// lambda argument lives in the lambda's own `ActionsDAG`, not in this one - which is what
    /// `allNodeFunctions` covers, including a lambda that constant folding turned into a `COLUMN` node.
    for (const auto & node : dag.getNodes())
        if (!allNodeFunctions(node, [](const IFunctionBase & function) { return function.isDeterministicInScopeOfQuery(); }))
            return true;
    return false;
}

bool isSensitiveToEvaluationCount(const ActionsDAG & dag)
{
    auto is_insensitive = [](const IFunctionBase & function)
    {
        return function.isDeterministicInScopeOfQuery() && !function.isStateful() && !function.hasObservableSideEffects();
    };

    /// A lambda without captures is constant-folded into a `COLUMN` node holding a `ColumnFunction`, which
    /// hides the functions of its body from a plain scan over the function nodes; `allNodeFunctions`
    /// descends into it.
    for (const auto & node : dag.getNodes())
    {
        if (!allNodeFunctions(node, is_insensitive))
            return true;
    }

    return false;
}

FilterResult filterResultForNotMatchedRows(
    const ActionsDAG & filter_dag,
    const String & filter_column_name,
    const Block & input_stream_header,
    bool allow_unknown_function_arguments
)
{
    /// `ActionsDAG::evaluatePartialResult` (called below) routes every function node through
    /// `IFunction::executeImplDryRun` with `input_rows_count=1`. For functions that are not
    /// deterministic within a single query (`rand`, `nowInBlock`, `rowNumberInAllBlocks`,
    /// `blockNumber`, ...) this single dry-run row is not representative of the runtime
    /// behaviour: at runtime each row may produce a different value. Functions like `now` /
    /// `today` / `currentUser` are not deterministic across queries but ARE deterministic
    /// within a single query (`isDeterministicInScopeOfQuery() == true`), so their plan-time
    /// value is faithful for all rows and they do NOT trip the guard below.
    ///
    /// Even with a fully-initialized dry-run output (e.g. `rowNumberInAllBlocks::executeImplDryRun`
    /// returning `[0]`), a filter such as `rowNumberInAllBlocks() = 1` evaluates to FALSE on the
    /// dry-run row but TRUE for the second runtime row. Without this guard the JOIN-conversion
    /// optimizer (`tryConvertAnyOuterJoinToInnerJoin` /
    /// `tryConvertAnyJoinToSemiOrAntiJoin`) concludes the filter is always FALSE for not-matched
    /// rows and silently converts `ANY OUTER JOIN` to `INNER`/`SEMI`/`ANTI`, dropping rows that
    /// would have survived. Bail out to `UNKNOWN` so the JOIN is left unchanged.
    if (dagContainsNonDeterministicFunction(filter_dag))
        return FilterResult::UNKNOWN;

    ActionsDAG::IntermediateExecutionResult filter_input;

    /// A set that is not built yet is an unknown argument: `in` must never be dry-run on it (that yields a fake 0).
    for (const auto & node : filter_dag.getNodes())
    {
        if (isNotReadySetColumn(node))
            filter_input.emplace(&node, ColumnWithTypeAndName{nullptr, node.result_type, node.result_name});
    }

    /// Create constant columns with default values for inputs of the filter DAG
    for (const auto * input : filter_dag.getInputs())
    {
        if (!input_stream_header.has(input->result_name))
            continue;

        if (input->column)
        {
            /// ActionsDAG::addColumn normalizes ColumnConst to size 0; expand to size 1
            /// because evaluatePartialResult is called below with input_rows_count == 1.
            ColumnPtr constant_column = ColumnConst::create(input->column->getDataColumnPtr(), 1);
            auto constant_column_with_type_and_name = ColumnWithTypeAndName{constant_column, input->result_type, input->result_name};
            filter_input.emplace(input, std::move(constant_column_with_type_and_name));
            continue;
        }

        /// A not-matched row holds the column's own default (`Date32`: 1970-01-01, not `getDefault`'s
        /// 1900-01-01), and where default insertion is not trivial no probe is guaranteed faithful.
        if (!input->result_type->isDefaultInsertTrivial())
            continue;

        auto constant_column = createColumnConstWithDefaultValue(input->result_type->createColumn());
        auto constant_column_with_type_and_name = ColumnWithTypeAndName{std::move(constant_column), input->result_type, input->result_name};
        filter_input.emplace(input, std::move(constant_column_with_type_and_name));
    }

    const auto * filter_node = filter_dag.tryFindInOutputs(filter_column_name);
    if (!filter_node)
        return FilterResult::UNKNOWN;

    return evaluateFilterResult(filter_input, filter_node, allow_unknown_function_arguments);
}

FilterResult evaluateFilterResult(
    ActionsDAG::IntermediateExecutionResult & input, const ActionsDAG::Node * filter_node, bool allow_unknown_function_arguments)
{
    ActionsDAG::NodeRawConstPtrs targets = {filter_node};
    auto conjunction_atoms = ActionsDAG::extractConjunctionAtoms(filter_node);
    if (conjunction_atoms.size() > 1)
        targets.insert(targets.end(), conjunction_atoms.begin(), conjunction_atoms.end());

    ColumnsWithTypeAndName filter_output;
    try
    {
        filter_output = ActionsDAG::evaluatePartialResult(
            input,
            targets,
            /*input_rows_count=*/1,
            { .skip_materialize = true, .allow_unknown_function_arguments = allow_unknown_function_arguments }
        );
    }
    catch (const Exception &)
    {
        /// If we cannot evaluate the filter expression, return UNKNOWN
        return FilterResult::UNKNOWN;
    }

    if (auto result = getFilterResult(filter_output[0]); result != FilterResult::UNKNOWN)
        return result;

    /// In filter context NULL is equivalent to false, but `and` with a constant NULL argument
    /// does not fold to a constant: the result is 0 or NULL depending on the other arguments
    /// (e.g. `NULL = 42 AND <unknown>`). Without `allow_unknown_function_arguments`, `and` is not
    /// folded at all while one of its arguments is unknown, even with a constant 0 argument.
    /// Both are falsy, so if any conjunction atom is a falsy constant, the filter cannot pass.
    for (size_t i = 1; i < filter_output.size(); ++i)
    {
        if (getFilterResult(filter_output[i]) == FilterResult::FALSE)
            return FilterResult::FALSE;
    }

    return FilterResult::UNKNOWN;
}

std::unordered_set<const ActionsDAG::Node *> collectNullPropagatingInputs(const ActionsDAG::Node * root)
{
    std::unordered_set<const ActionsDAG::Node *> result;
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<const ActionsDAG::Node *> stack{root};

    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (!visited.insert(node).second)
            continue;

        switch (node->type)
        {
            case ActionsDAG::ActionType::INPUT:
            case ActionsDAG::ActionType::PLACEHOLDER:
                /// A column that is not `Nullable` here holds no NULL, so it rejects nothing.
                if (isNullableOrLowCardinalityNullable(node->result_type))
                    result.insert(node);
                break;
            case ActionsDAG::ActionType::ALIAS:
                if (!node->children.empty())
                    stack.push_back(node->children.front());
                break;
            case ActionsDAG::ActionType::FUNCTION:
                if (node->function && node->function->isNullPropagating(node->result_type))
                    stack.insert(stack.end(), node->children.begin(), node->children.end());
                break;
            case ActionsDAG::ActionType::COLUMN:
            case ActionsDAG::ActionType::ARRAY_JOIN:
                break;
        }
    }

    return result;
}

void collectNullRejectedColumnsFromJoinConditions(const JoinStepLogical & join, NameSet & left, NameSet & right)
{
    const auto & actions = join.getActionsDAG();
    if (actions.hasStatefulFunctions() || dagContainsNonDeterministicFunction(actions))
        return;

    const auto & join_operator = join.getJoinOperator();
    /// The sides for which a null-rejecting condition proves the input column not NULL.
    auto [discards_left, discards_right] = [&]() -> std::pair<bool, bool>
    {
        const auto kind = join_operator.kind;
        switch (join_operator.strictness)
        {
            case JoinStrictness::Unspecified:
            case JoinStrictness::All:
            case JoinStrictness::Any:
            case JoinStrictness::Anti:
                return {isInnerOrRight(kind), isInnerOrLeft(kind)};
            case JoinStrictness::Semi:
                return {isLeftOrRight(kind), isLeftOrRight(kind)};
            default:
                return {false, false};
        }
    }();
    if (!discards_left && !discards_right)
        return;

    auto is_null_rejecting_operator = [](JoinConditionOperator op)
    {
        switch (op)
        {
            case JoinConditionOperator::Equals:
            case JoinConditionOperator::Less:
            case JoinConditionOperator::LessOrEquals:
            case JoinConditionOperator::Greater:
            case JoinConditionOperator::GreaterOrEquals:
                return true;
            default:
                return false;
        }
    };

    for (const auto & expression : join_operator.expression)
    {
        auto [op, lhs, rhs] = expression.asBinaryPredicate();
        if (!is_null_rejecting_operator(op))
            continue;

        for (const auto & operand : {lhs, rhs})
        {
            if (!operand)
                continue;

            NameSet * side = nullptr;
            if (discards_left && operand.fromLeft())
                side = &left;
            else if (discards_right && operand.fromRight())
                side = &right;

            if (!side)
                continue;

            for (const auto * input : collectNullPropagatingInputs(operand.getNode()))
                side->insert(input->result_name);
        }
    }
}

bool canUseLazyMaterializationForReadingStep(ReadFromMergeTree * reading)
{
    /// A STREAM read selects its parts and ranges during execution, so the range set captured
    /// here is not the one that will be read.
    if (reading->getQueryInfo().isStream())
        return false;

    /// Allow FINAL only for ReplacingMergeTree.
    if (reading->isQueryWithFinal()
        && reading->getMergeTreeData().merging_params.mode != MergeTreeData::MergingParams::Replacing)
        return false;

    if (reading->isQueryWithSampling())
        return false;

    if (reading->getMutationsSnapshot()->hasPatchParts())
        return false;

    /// A row is addressed by the virtual columns `_part_starting_offset` and `_part_offset`, which a column of the table
    /// with the same name takes the place of: the second read would fetch the rows that column's values point to.
    const auto & columns = reading->getStorageMetadata()->getColumns();
    if (columns.has("_part_offset") || columns.has("_part_starting_offset"))
        return false;

    return true;
}

}
}
