#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

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

namespace
{

/// Traces an output column of a pass-through expression to the input column it forwards:
/// aliases are unwrapped, anything computed disqualifies.
const String * traceThroughExpression(const ExpressionStep & expression, const String & name)
{
    const auto * node = expression.getExpression().tryFindInOutputs(name);
    if (!node)
        return nullptr;
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    if (node->type != ActionsDAG::ActionType::INPUT)
        return nullptr;
    return &node->result_name;
}

}

std::optional<AggregationTopK> findAggregationTopK(QueryPlan::Node & limit_node)
{
    /// An optimization that drops the other groups cannot serve `LIMIT WITH TIES`, whose output size is not known in
    /// advance, or an exact `rows_before_limit_at_least` (`alwaysReadTillEnd`), whose counter runs downstream of the
    /// aggregation and would report the kept groups instead of all of them.
    const auto * limit = typeid_cast<LimitStep *>(limit_node.step.get());
    if (!limit || limit_node.children.size() != 1 || limit->withTies() || limit->alwaysReadTillEnd())
        return {};

    QueryPlan::Node * sorting_node = limit_node.children.front();
    const auto * sorting = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting || sorting_node->children.size() != 1 || sorting->getLimit() == 0)
        return {};

    const auto & description = sorting->getSortDescription();
    if (description.size() != 1 || description.front().with_fill)
        return {};

    String column = description.front().column_name;
    QueryPlan::Node * node = sorting_node->children.front();
    while (const auto * expression = typeid_cast<ExpressionStep *>(node->step.get()))
    {
        /// An `arrayJoin` between the aggregation and the sort changes the rows of a group, and over an empty array
        /// produces none, so the best n groups are no longer the rows the limit needs: the groups that an optimization
        /// drops can be precisely the ones that would have survived. The gates of `optimizeGroupByTopK` and of the
        /// planner reject the same shape.
        if (expression->getExpression().hasArrayJoin())
            return {};

        const String * traced = traceThroughExpression(*expression, column);
        if (!traced || node->children.size() != 1)
            return {};
        column = *traced;
        node = node->children.front();
    }

    auto * aggregating = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating)
        return {};
    const auto & params = aggregating->getParams();
    if (!aggregating->isFinal() || aggregating->inOrder() || aggregating->isGroupingSets() || params.overflow_row
        || params.keys_size == 0)
        return {};

    for (size_t i = 0; i < params.aggregates.size(); ++i)
    {
        if (params.aggregates[i].column_name == column)
            return AggregationTopK{
                .aggregating_node = node,
                .aggregating = aggregating,
                .limit = sorting->getLimit(),
                .rank_index = i,
                .ascending = description.front().direction > 0,
                .has_collator = description.front().collator != nullptr};
    }
    return {};
}

bool isRowBoundedCount(const AggregateDescription & aggregate)
{
    /// Any lone `count`: `count()`, and `count(x)` in both of its forms (the plain one for a non-nullable argument,
    /// `AggregateFunctionCountNotNullUnary` for a nullable one), or the exact distinct count `uniqExact`, also as
    /// `uniqExactIf` over the rows its condition admits (the analyzer makes it of `uniqExact(if(...))`). No parameters:
    /// a parametric `count` is another function. An approximate `uniq` estimate could exceed the rows, and so could a
    /// combinator that takes several values from a row, like `-Array`.
    const auto & function = *aggregate.function;
    const String name = function.getName();
    if (name == "count")
        return aggregate.parameters.empty();
    return (name == "uniqExact" || name == "uniqExactIf") && aggregate.parameters.empty()
        && WhichDataType(function.getResultType()).isUInt64();
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

bool hasResultNameClash(const ActionsDAG & dag, const ActionsDAG::Node * node)
{
    for (const auto & other : dag.getNodes())
        if (&other != node && other.result_name == node->result_name)
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
