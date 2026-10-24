#pragma once

#include <map>

#include <base/types.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class ExpandedASTBudget;

/// Propagate every WITH alias expression to its descendant subqueries, with correct scoping visibility.
class ApplyWithAliasVisitor
{
public:
    struct Data
    {
        std::map<String, ASTPtr> exprs;
        ExpandedASTBudget * budget = nullptr;
    };

    /// Throws `TOO_BIG_AST` when the propagated copies exceed `max_expanded_ast_elements` (zero means no limit).
    static void visit(ASTPtr & ast, size_t max_expanded_ast_elements);

private:
    static void visit(ASTPtr & ast, const Data & data);
};

}
