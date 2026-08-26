#include <Processors/QueryPlan/Optimizations/Utils.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Common/typeid_cast.h>

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
    for (const auto & node : dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base)
        {
            if (!node.function_base->isDeterministicInScopeOfQuery())
                return true;
        }
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

bool peelPassThroughExpressions(QueryPlan::Node *& node, SortDescription & description, size_t max_peel)
{
    for (size_t peeled = 0; peeled < max_peel; ++peeled)
    {
        const auto * expression_step = typeid_cast<const ExpressionStep *>(node->step.get());
        if (!expression_step)
            return true;
        if (node->children.size() != 1)
            return false;

        const ActionsDAG & dag = expression_step->getExpression();
        if (dag.hasArrayJoin())
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

bool addArrayJoinEmptinessFilter(
    ArrayJoinStep & array_join,
    QueryPlan::Node *& input_node,
    QueryPlan::Nodes & nodes)
{
    const Names & array_join_columns = array_join.getColumns();
    if (array_join_columns.empty())
        return false;

    Names source_columns;
    source_columns.reserve(array_join_columns.size());
    for (const auto & column_name : array_join_columns)
        source_columns.push_back(array_join.getSourceColumnName(column_name));

    const Block & array_join_input_header = *array_join.getInputHeaders().front();

    ActionsDAG dag(input_node->step->getOutputHeader()->getColumnsWithTypeAndName());

    auto length_function = FunctionFactory::instance().get("length", nullptr);
    auto greater_function = FunctionFactory::instance().get("greater", nullptr);

    DataTypePtr zero_type = std::make_shared<DataTypeUInt8>();
    const auto * zero = &dag.addColumn(zero_type->createColumnConst(0, Field(UInt64(0))), zero_type, "0");

    ActionsDAG::NodeRawConstPtrs non_empty;
    non_empty.reserve(array_join_columns.size());

    for (size_t i = 0; i < array_join_columns.size(); ++i)
    {
        /// The filter may be inserted at different depths: immediately below `ArrayJoinStep`
        /// (limit push-down, where the header contains analyzer aliases) or below the whole
        /// `ARRAY JOIN` expression chain (top-K, where the header contains original input names).
        const auto * input = dag.tryFindInOutputs(source_columns[i]);
        if (!input)
            input = dag.tryFindInOutputs(array_join_columns[i]);
        if (!input)
        {
            if (auto pos = source_columns[i].rfind('.'); pos != String::npos && pos + 1 < source_columns[i].size())
                input = dag.tryFindInOutputs(source_columns[i].substr(pos + 1));
        }
        if (!input)
        {
            if (!array_join_input_header.has(array_join_columns[i]))
                return false;

            const auto & fallback = array_join_input_header.getByName(array_join_columns[i]);
            if (!fallback.column || !isColumnConst(*fallback.column))
                return false;

            input = &dag.addColumn(
                assert_cast<const ColumnConst &>(*fallback.column).getPtr(),
                fallback.type,
                fallback.name);
        }

        const auto & type = input->result_type;
        if (!typeid_cast<const DataTypeArray *>(type.get()) && !typeid_cast<const DataTypeMap *>(type.get()))
            return false;

        const auto & length = dag.addFunction(length_function, {input}, {});
        non_empty.push_back(&dag.addFunction(greater_function, {&length, zero}, {}));
    }

    const auto * guard = non_empty.front();
    if (non_empty.size() > 1)
    {
        /// Unlike `greatest`, `or` does not require a `Context`, which plan optimizations do not have.
        auto or_function = FunctionFactory::instance().get("or", nullptr);
        guard = &dag.addFunction(or_function, std::move(non_empty), {});
    }

    /// A constant empty array always emits zero rows, while a constant non-empty array always emits
    /// at least one. In either case restricting the input before the `ARRAY JOIN` is safe without a
    /// runtime filter.
    if (guard->column)
        return true;

    dag.getOutputs().push_back(guard);
    String filter_column_name = guard->result_name;

    auto & filter_node = nodes.emplace_back();
    filter_node.children.push_back(input_node);
    filter_node.step = std::make_unique<FilterStep>(
        input_node->step->getOutputHeader(),
        std::move(dag),
        std::move(filter_column_name),
        /*remove_filter_column_=*/ true);
    filter_node.step->setStepDescription("Non-empty arrays for ARRAY JOIN");
    input_node = &filter_node;

    return true;
}

}
}
