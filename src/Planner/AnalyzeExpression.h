#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Analyze a standalone expression AST into ActionsDAG using the Analyzer framework.
/// This is the Analyzer equivalent of:
///   TreeRewriter(context).analyze(ast, columns) + ExpressionAnalyzer(ast, ...).getActionsDAG(add_aliases)
ActionsDAG analyzeExpressionToActionsDAG(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases = false);

/// Same but returns ExpressionActionsPtr (wraps ActionsDAG in ExpressionActions).
ExpressionActionsPtr analyzeExpressionToActions(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases = false);

}
