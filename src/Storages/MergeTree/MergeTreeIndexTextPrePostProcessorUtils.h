#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Replaces all AST subtrees whose canonical name equals `expression_name` with a plain identifier
/// `identifier_name`. Works for both plain identifiers and function-expression nodes.
void replaceExpressionToIdentifier(ASTPtr & ast, const String & expression_name, const String & identifier_name);

/// Builds an ActionsDAG for `expression_ast` over `source_columns`, projects to the single output,
/// and removes unused actions.
ActionsDAG buildActionsDAGFromAST(ASTPtr expression_ast, const NamesAndTypesList & source_columns);

/// Validates common text-index transform requirements shared by preprocessor and postprocessor
/// expressions: a single functional output, at least one expression on top of the source column,
/// and absence of non-deterministic functions or `arrayJoin`.
void validateTransformActionsDAG(const ActionsDAG & actions_dag, const String & transform_name, const String & source_name);

/// Executes `actions` on top of a single input column and returns the resulting output column.
ColumnPtr executeUnaryExpressionActions(
    const ExpressionActions & actions,
    ColumnPtr column,
    const DataTypePtr & type,
    const String & column_name,
    size_t n_rows);

}
