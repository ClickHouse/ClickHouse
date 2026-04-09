#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Replaces all AST subtrees whose canonical name equals `expression_name` with a plain identifier
/// `identifier_name`. Works for both plain identifiers and function-expression nodes.
void replaceExpressionToIdentifier(ASTPtr & ast, const String & expression_name, const String & identifier_name);

/// Builds an ActionsDAG for `expression_ast` over `source_columns`, projects to the single output,
/// and removes unused actions.
ActionsDAG buildActionsDAGFromAST(ASTPtr expression_ast, const NamesAndTypesList & source_columns);

}
