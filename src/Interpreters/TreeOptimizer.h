#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Storages/IStorage_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct TreeRewriterResult;

/// Part of of Tree Rewriter (SyntaxAnalyzer) that optimizes AST.
/// Query should be ready to execute either before either after it. But resulting query could be faster.
class TreeOptimizer
{
public:
    /// For any AST
    static void optimizeExpression(
        ASTPtr & query,
        TreeRewriterResult & result,
        ContextPtr context);

    /// For ASTSelectQuery only
    static void optimizeSelect(
        ASTPtr & query,
        TreeRewriterResult & result,
        const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns,
        ContextPtr context);
};

}
