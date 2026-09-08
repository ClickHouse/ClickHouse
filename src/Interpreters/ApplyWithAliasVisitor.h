#pragma once

#include <map>

#include <base/types.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
/// Propagate every WITH alias expression to its descendant subqueries, with correct scoping visibility.
class ApplyWithAliasVisitor
{
public:
    struct Data
    {
        std::map<String, ASTPtr> exprs;
        /// Propagation clones every visible alias into every descendant subquery, so a query with `k`
        /// aliases nested `d` levels deep expands to `k ^ d` nodes - `WITH (SELECT … FROM (EXPLAIN …))`
        /// repeated three times is already enough to exhaust the server's memory. Bound the expansion the
        /// way `QueryNormalizer` bounds alias substitution, so such a query fails with `TOO_BIG_AST`.
        size_t max_expanded_ast_elements = 0;
    };

    static void visit(ASTPtr & ast, size_t max_expanded_ast_elements)
    {
        visit(ast, Data{.exprs = {}, .max_expanded_ast_elements = max_expanded_ast_elements});
    }

private:
    static void visit(ASTPtr & ast, const Data & data);
};

}
