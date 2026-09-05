#include <Interpreters/lambdaBodySafety.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Functions/FunctionsMiscellaneous.h>

#include <unordered_set>

namespace DB
{

namespace
{

/// Collects the unsafe classes found while walking lambda body DAGs.
/// `visited_dags` / `visited_columns` make the walk cycle-safe and keep it linear when the same body
/// or captured column is reachable by several paths.
struct LambdaBodyInspector
{
    LambdaBodySafety result;
    std::unordered_set<const ActionsDAG *> visited_dags;
    std::unordered_set<const IColumn *> visited_columns;

    /// Both classes found: nothing a deeper node could add.
    bool saturated() const { return result.has_non_deterministic && result.has_stateful; }

    void inspectColumn(const IColumn * column)
    {
        if (!column || !visited_columns.insert(column).second)
            return;

        /// A folded lambda is a ColumnFunction, optionally wrapped in ColumnConst (the wrapper is
        /// absent when some captured column is not constant). Same unwrap as ActionsDAG uses when
        /// serializing DataTypeSet, minus its throws: here an unexpected shape simply is not a lambda.
        const IColumn * maybe_function = column;
        if (const auto * column_const = typeid_cast<const ColumnConst *>(maybe_function))
            maybe_function = &column_const->getDataColumn();

        const auto * column_function = typeid_cast<const ColumnFunction *>(maybe_function);
        if (!column_function)
            return;

        const auto & function = column_function->getFunction();
        if (const auto * function_expression = typeid_cast<const FunctionExpression *>(function.get()))
            inspectBody(function_expression->getAcionsDAG());

        /// A nested lambda is not in the body DAG: it arrives as one of the captured columns.
        for (const auto & captured : column_function->getCapturedColumns())
            inspectColumn(captured.column.get());
    }

    /// Walk one lambda body, collecting the unsafe classes and descending into nested lambdas.
    void inspectBody(const ActionsDAG & dag)
    {
        if (!visited_dags.insert(&dag).second)
            return;

        for (const auto & node : dag.getNodes())
        {
            if (saturated())
                return;

            if (node.type == ActionsDAG::ActionType::FUNCTION)
            {
                if (!node.function_base->isDeterministicInScopeOfQuery())
                    result.has_non_deterministic = true;
                if (node.function_base->isStateful())
                    result.has_stateful = true;

                /// A capturing lambda keeps its body in its own DAG.
                if (const auto * capture = typeid_cast<const FunctionCapture *>(node.function_base.get()))
                    inspectBody(capture->getAcionsDAG());
            }
            else if (node.type == ActionsDAG::ActionType::COLUMN)
            {
                inspectColumn(node.column.get());
            }
        }
    }

    /// Enter the lambda bodies held by one node of an outer DAG.
    void inspectNode(const ActionsDAG::Node & node)
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION)
        {
            if (const auto * capture = typeid_cast<const FunctionCapture *>(node.function_base.get()))
                inspectBody(capture->getAcionsDAG());
        }
        else if (node.type == ActionsDAG::ActionType::COLUMN)
        {
            inspectColumn(node.column.get());
        }
    }
};

}

LambdaBodySafety inspectLambdaBodies(const ActionsDAG::Node & node)
{
    LambdaBodyInspector inspector;
    inspector.inspectNode(node);
    return inspector.result;
}

bool hasStatefulFunctionsInLambdaBodies(const ActionsDAG & dag)
{
    LambdaBodyInspector inspector;
    for (const auto & node : dag.getNodes())
    {
        inspector.inspectNode(node);
        if (inspector.result.has_stateful)
            return true;
    }
    return false;
}

bool hasNonDeterministicFunctionsInLambdaBodies(const ActionsDAG & dag)
{
    /// One inspector for the whole DAG: reusing its visited sets across nodes only ever skips a body
    /// already answered for, which is sound for a single yes/no question about the DAG as a whole.
    LambdaBodyInspector inspector;
    for (const auto & node : dag.getNodes())
    {
        inspector.inspectNode(node);
        if (inspector.result.has_non_deterministic)
            return true;
    }
    return false;
}

}
