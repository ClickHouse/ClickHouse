#include <Storages/MergeTree/MergeTreeIndexTextPrePostProcessorUtils.h>

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

void replaceExpressionToIdentifier(ASTPtr & ast, const String & expression_name, const String & identifier_name)
{
    if (!ast)
        return;

    if ((ast->as<ASTIdentifier>() || ast->as<ASTFunction>()) && ast->getColumnName() == expression_name)
    {
        ast = make_intrusive<ASTIdentifier>(identifier_name);
        return;
    }

    for (auto & child : ast->children)
        replaceExpressionToIdentifier(child, expression_name, identifier_name);
}

ActionsDAG buildActionsDAGFromAST(ASTPtr expression_ast, const NamesAndTypesList & source_columns)
{
    auto context = Context::getGlobalContextInstance();
    auto syntax_result = TreeRewriter(context).analyze(expression_ast, source_columns);
    auto actions_dag = ExpressionAnalyzer(expression_ast, syntax_result, context).getActionsDAG(false, true);

    auto expression_name = expression_ast->getColumnName();
    actions_dag.project({{expression_name, expression_name}});
    actions_dag.removeUnusedActions();

    return actions_dag;
}

void validateTransformActionsDAG(const ActionsDAG & actions_dag, const String & transform_name, const String & source_name)
{
    const ActionsDAG::NodeRawConstPtrs & outputs = actions_dag.getOutputs();
    if (outputs.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} expression must return a single column. Got {} output columns", transform_name, outputs.size());

    if (outputs.front()->type != ActionsDAG::ActionType::FUNCTION)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} expression must be a function. Got '{}' action type", transform_name, outputs.front()->type);

    if (outputs.front()->result_name == source_name)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} must have at least one expression on top of the source column. Got '{}'", transform_name, outputs.front()->result_name);

    if (actions_dag.hasNonDeterministic())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} expression must not contain non-deterministic functions", transform_name);

    if (actions_dag.hasArrayJoin())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} expression must not contain arrayJoin", transform_name);
}

ColumnPtr executeUnaryExpressionActions(
    const ExpressionActions & actions,
    ColumnPtr column,
    const DataTypePtr & type,
    const String & column_name,
    size_t n_rows)
{
    Block block({ColumnWithTypeAndName(column, type, column_name)});
    actions.execute(block, n_rows);
    return block.safeGetByPosition(0).column;
}

}
