#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

class Context;

/// Part of of Tree Rewriter (SyntaxAnalyzer) that optimizes AST.
/// Query should be ready to execute either before either after it. But resulting query could be faster.
class TreeOptimizer
{
public:
    static void apply(ASTPtr & query, Aliases & aliases, const NameSet & source_columns_set,
                      const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns,
                      const Context & context, bool & rewrite_subqueries);

    static void optimizeIf(ASTPtr & query, Aliases & aliases, bool if_chain_to_multiif);
};

}
