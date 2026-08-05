#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

/// Replace subcolumns to getSubcolumn() function.
void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns);

/// Required source columns of `expression_ast` with subcolumns rewritten to getSubcolumn(),
/// i.e. resolved to their top-level columns (`t.a` -> `t`). Used to compare an index/expression's
/// dependencies against whole columns tracked by a mutation/ALTER.
Names getRequiredColumnsWithSubcolumnsReplaced(
    const ASTPtr & expression_ast, const NamesAndTypesList & all_columns, const ContextPtr & context);

}

