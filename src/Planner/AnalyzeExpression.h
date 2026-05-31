#pragma once

#include <Core/Block.h>
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
///
/// When build_subquery_sets is true (default), `IN (subquery)` sets are executed
/// eagerly while the DAG is built, so the resulting actions can be evaluated
/// standalone.  Callers that build the sets later themselves (e.g. constraint checks
/// via `VirtualColumnUtils::buildSetsForDAG` at insert time) must pass false: eagerly
/// executing a subquery here would run a nested pipeline that reports progress on the
/// outer query's context, which breaks the INSERT protocol handshake (a stray
/// `Progress` packet arrives before the sample block).
ActionsDAG analyzeExpressionToActionsDAG(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases = false,
    bool project_result = true,
    bool build_subquery_sets = true);

/// Same but returns ExpressionActionsPtr (wraps ActionsDAG in ExpressionActions).
ExpressionActionsPtr analyzeExpressionToActions(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases = false,
    CompileExpressions compile_expressions = CompileExpressions::no,
    bool build_subquery_sets = true);

struct AnalyzedExpressionWithSampleBlock
{
    /// Full ExpressionActions: source columns are preserved alongside the expression
    /// results in the output, so callers can run `expression->execute(block)` to add
    /// the expression columns to an input block.
    ExpressionActionsPtr expression;
    /// Sample block of just the projected expression result columns (no source columns).
    Block sample_block;
};

/// Analyze a standalone expression AST and return both an `ExpressionActions` (with
/// source columns preserved) and a sample block of just the expression result columns.
///
/// Equivalent to calling `analyzeExpressionToActions(expr, columns, context)` and
/// `analyzeExpressionToActions(expr, columns, context, /*add_aliases=*/true)->getSampleBlock()`,
/// but the underlying DAG is built only once.  Avoids running `IN (subquery)` sets
/// twice via `buildSetInplace`, which the analyzer executes eagerly when the DAG is built.
AnalyzedExpressionWithSampleBlock analyzeExpressionToActionsAndSampleBlock(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    CompileExpressions compile_expressions = CompileExpressions::no);

}
