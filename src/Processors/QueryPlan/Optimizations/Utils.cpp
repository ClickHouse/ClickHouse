#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <algorithm>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

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

namespace
{

/// Names of the outputs of `dag` computed from an input that `is_marked_input` accepts.
template <typename IsMarkedInput>
NameSet outputsReadingMarkedInputs(const ActionsDAG & dag, IsMarkedInput && is_marked_input)
{
    std::unordered_map<const ActionsDAG::Node *, bool> marked;
    std::vector<std::pair<const ActionsDAG::Node *, bool>> stack;
    for (const auto * output : dag.getOutputs())
        stack.emplace_back(output, false);

    while (!stack.empty())
    {
        auto [node, expanded] = stack.back();
        stack.pop_back();

        if (marked.contains(node))
            continue;

        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            marked.emplace(node, is_marked_input(*node));
            continue;
        }

        if (!expanded)
        {
            stack.emplace_back(node, true);
            for (const auto * child : node->children)
                if (!marked.contains(child))
                    stack.emplace_back(child, false);
            continue;
        }

        marked.emplace(node, std::ranges::any_of(node->children, [&](const auto * child) { return marked.at(child); }));
    }

    NameSet result;
    for (const auto * output : dag.getOutputs())
        if (marked.at(output))
            result.insert(output->result_name);
    return result;
}

}

ConstantColumns collectConstantColumns(const QueryPlan::Node & root)
{
    ConstantColumns result;
    static const NameSet no_columns;
    auto constant_columns_of = [&](const QueryPlan::Node * node) -> const NameSet &
    {
        auto it = result.find(node);
        return it == result.end() ? no_columns : it->second;
    };

    std::unordered_set<const QueryPlan::Node *> visited;
    std::vector<std::pair<const QueryPlan::Node *, bool>> stack{{&root, false}};
    while (!stack.empty())
    {
        auto [node, expanded] = stack.back();
        stack.pop_back();

        if (!expanded)
        {
            if (!visited.insert(node).second)
                continue;
            stack.emplace_back(node, true);
            for (const auto * child : node->children)
                stack.emplace_back(child, false);
            continue;
        }

        NameSet constants;
        const auto * step = node->step.get();
        if (step && step->hasOutputHeader())
            for (const auto & column : *step->getOutputHeader())
                if (column.column && isColumnConst(*column.column))
                    constants.insert(column.name);

        const ActionsDAG * dag = nullptr;
        if (const auto * filter = typeid_cast<const FilterStep *>(step))
            dag = &filter->getExpression();
        else if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
            dag = &expression->getExpression();

        const auto * join = typeid_cast<const JoinStepLogical *>(step);
        if (dag && node->children.size() == 1)
        {
            const auto & below = constant_columns_of(node->children.front());
            constants.merge(outputsReadingMarkedInputs(*dag, [&](const auto & input) { return below.contains(input.result_name); }));

            /// A name the DAG neither produces nor consumes is forwarded by `ActionsDAG::updateHeader`.
            NameSet known_columns;
            for (const auto * output : dag->getOutputs())
                known_columns.insert(output->result_name);
            for (const auto * input : dag->getInputs())
                known_columns.insert(input->result_name);
            for (const auto & name : below)
                if (!known_columns.contains(name))
                    constants.insert(name);
        }
        else if (join && node->children.size() == 2)
        {
            const auto & left = constant_columns_of(node->children[0]);
            const auto & right = constant_columns_of(node->children[1]);
            constants.merge(outputsReadingMarkedInputs(
                join->getActionsDAG(),
                [&](const auto & input)
                {
                    JoinActionRef ref(&input, join->getExpressionActions());
                    return (ref.fromLeft() && left.contains(input.result_name)) || (ref.fromRight() && right.contains(input.result_name));
                }));
        }
        else
        {
            for (const auto * child : node->children)
            {
                const auto & below = constant_columns_of(child);
                constants.insert(below.begin(), below.end());
            }
        }

        result[node] = std::move(constants);
    }
    return result;
}

FilterResult filterResultForNotMatchedRows(
    const ActionsDAG & filter_dag,
    const String & filter_column_name,
    const Block & input_stream_header,
    bool allow_unknown_function_arguments,
    const QueryPlan::Node * side_node
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

    /// Not-matched rows reach the filter as full columns, but a constant of this side may stay constant after the rewrite.
    ActionsDAG::NodeRawConstPtrs readers;
    auto full_input = filter_input;
    std::optional<NameSet> side_constants;
    for (const auto * input : ActionsDAG::getInputsReadByRepresentation(*filter_node, &readers))
    {
        const auto * column = input_stream_header.findByName(input->result_name);
        if (!column)
            continue;
        if (column->column && isColumnConst(*column->column))
            return FilterResult::UNKNOWN;
        if (side_node)
        {
            if (!side_constants)
            {
                auto constant_columns = collectConstantColumns(*side_node);
                auto it = constant_columns.find(side_node);
                side_constants = it == constant_columns.end() ? NameSet{} : std::move(it->second);
            }
            if (side_constants->contains(input->result_name))
                return FilterResult::UNKNOWN;
        }
        if (auto it = full_input.find(input); it != full_input.end())
            it->second.column = it->second.column->convertToFullColumnIfConst();
    }

    /// The readers see the full column; above them only values matter, so their one-row results are constants.
    if (!readers.empty())
    {
        ColumnsWithTypeAndName reader_results;
        try
        {
            reader_results = ActionsDAG::evaluatePartialResult(
                full_input,
                readers,
                /*input_rows_count=*/1,
                { .skip_materialize = true, .allow_unknown_function_arguments = allow_unknown_function_arguments });
        }
        catch (const Exception &)
        {
            return FilterResult::UNKNOWN;
        }

        for (size_t i = 0; i < readers.size(); ++i)
        {
            auto & result = reader_results[i];
            if (!result.column || result.column->size() != 1 || WhichDataType(result.type).isFunction())
                result.column = nullptr;
            else if (!isColumnConst(*result.column))
                result.column = ColumnConst::create(result.column, 1);
            filter_input[readers[i]] = std::move(result);
        }
    }

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

const ReadFromMergeTree * findMergeTreeRead(const QueryPlan::Node * node)
{
    while (node)
    {
        if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(node->step.get()))
            return reading;
        if (node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }
    return nullptr;
}

bool peelPassThroughExpressions(QueryPlan::Node *& node, SortDescription & description, size_t max_peel)
{
    for (size_t peeled = 0; peeled < max_peel; ++peeled)
    {
        const auto * expression_step = typeid_cast<const ExpressionStep *>(node->step.get());
        if (!expression_step)
            return true;
        if (node->children.size() != 1)
            return false;

        /// a moved sort changes the rows this step sees, e.g. `rowNumberInAllBlocks` would give other values
        const ActionsDAG & dag = expression_step->getExpression();
        if (dag.hasArrayJoin() || isSensitiveToEvaluationCount(dag))
            return false;

        for (auto & sort_column : description)
        {
            const auto * out_node = dag.tryFindInOutputs(sort_column.column_name);
            if (!out_node)
                return false;

            while (out_node->type == ActionsDAG::ActionType::ALIAS)
                out_node = out_node->children.front();

            if (out_node->type != ActionsDAG::ActionType::INPUT)
                return false;

            sort_column.column_name = out_node->result_name;
        }

        node = node->children.front();
    }

    return true;
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
