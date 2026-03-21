#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ActionsDAG;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Analyze a standalone expression AST into ActionsDAG using the Analyzer framework.
/// This is the Analyzer equivalent of:
///   TreeRewriter(context).analyze(ast, columns) + ExpressionAnalyzer(ast, ...).getActionsDAG(add_aliases)
///
/// When add_aliases is true and project_result is true (default), the DAG projects
/// to only the expression columns.  When add_aliases is true but project_result is
/// false, aliases are added but source columns are preserved in the output — matching
/// the old ExpressionAnalyzer::getActionsDAG(true, false) behavior.
ActionsDAG analyzeExpressionToActionsDAG(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases = false,
    bool project_result = true);

/// Same but returns ExpressionActionsPtr (wraps ActionsDAG in ExpressionActions).
ExpressionActionsPtr analyzeExpressionToActions(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases = false,
    CompileExpressions compile_expressions = CompileExpressions::no);

}
