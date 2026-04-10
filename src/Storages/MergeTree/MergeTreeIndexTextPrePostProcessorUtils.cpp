#include <Storages/MergeTree/MergeTreeIndexTextPrePostProcessorUtils.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

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

}
