#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnSet.h>
#include <Columns/IColumn.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>

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

bool dagContainsNonReadySet(const ActionsDAG & dag)
{
    for (const auto & node : dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::COLUMN && node.column)
        {
            const ColumnSet * column_set = checkAndGetColumn<const ColumnSet>(&node.column->getDataColumn());
            if (column_set)
            {
                auto future_set = column_set->getData();
                if (!future_set || !future_set->get())
                    return true;
            }
        }
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

FilterResult filterResultForNotMatchedRows(
    const ActionsDAG & filter_dag,
    const String & filter_column_name,
    const Block & input_stream_header,
    bool allow_unknown_function_arguments
)
{
    /// If the filter DAG contains IN subquery sets that are not yet built - we cannot evaluate the filter result
    if (dagContainsNonReadySet(filter_dag))
        return FilterResult::UNKNOWN;

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

        auto constant_column = input->result_type->createColumnConst(1, input->result_type->getDefault());
        auto constant_column_with_type_and_name = ColumnWithTypeAndName{std::move(constant_column), input->result_type, input->result_name};
        filter_input.emplace(input, std::move(constant_column_with_type_and_name));
    }

    const auto * filter_node = filter_dag.tryFindInOutputs(filter_column_name);
    if (!filter_node)
        return FilterResult::UNKNOWN;

    ActionsDAG::NodeRawConstPtrs targets = {filter_node};
    auto conjunction_atoms = ActionsDAG::extractConjunctionAtoms(filter_node);
    if (conjunction_atoms.size() > 1)
        targets.insert(targets.end(), conjunction_atoms.begin(), conjunction_atoms.end());

    ColumnsWithTypeAndName filter_output;
    try
    {
        filter_output = ActionsDAG::evaluatePartialResult(
            filter_input,
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
    /// (e.g. `NULL = 42 AND <unknown>`).
    /// Both are falsy, so if any conjunction atom is a falsy constant, the filter cannot pass.
    for (size_t i = 1; i < filter_output.size(); ++i)
    {
        if (getFilterResult(filter_output[i]) == FilterResult::FALSE)
            return FilterResult::FALSE;
    }

    return FilterResult::UNKNOWN;
}
}

namespace
{

/// The body of a lambda is a separate `ActionsDAG`, not reachable from the outer one: a call that depends on a
/// lambda argument stays inside it, and only a nullary one is hoisted out.
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

}

const IFunctionBase * findFunctionInSubtrees(
    std::vector<const ActionsDAG::Node *> roots, const std::function<bool(const IFunctionBase &)> & predicate)
{
    std::vector<const ActionsDAG::Node *> nodes = std::move(roots);
    std::unordered_set<const ActionsDAG::Node *> visited_nodes;

    /// The columns captured by a folded lambda are not nodes of any `ActionsDAG`, so they need a
    /// worklist of their own.
    std::vector<const IColumn *> columns;
    std::unordered_set<const IColumn *> visited_columns;

    /// Whether `function` itself matches; the nodes of its lambda body, if it has one, are queued for the walk.
    auto matches = [&](const IFunctionBase & function)
    {
        if (predicate(function))
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
                if (matches(*current->function_base))
                    return current->function_base.get();
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

        if (matches(*column_function->getFunction()))
            return column_function->getFunction().get();

        /// A lambda that captures nothing is hoisted to the outermost level by the planner, so an
        /// enclosing lambda *captures* it. When that enclosing lambda is folded into a constant in
        /// turn, its own child edges are gone from the DAG and the nested lambda is reachable only
        /// through the captured columns.
        for (const auto & captured : column_function->getCapturedColumns())
            if (captured.column)
                columns.push_back(captured.column.get());
    }

    return nullptr;
}

}
