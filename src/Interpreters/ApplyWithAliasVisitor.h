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
        /// Propagation is meant to run once, on a query that is not itself a subquery. An alias whose
        /// subquery is interpreted as a fresh query - `view()`, or an `EXPLAIN` kind that interprets its
        /// argument - is analysed by a new non-subquery interpreter, which applies the propagation again
        /// to an AST that already carries the injected aliases. `k` such aliases therefore compound to
        /// `k ^ d` nodes over `d` levels. Bound the expansion the way `QueryNormalizer` bounds alias
        /// substitution, so such a query fails with `TOO_BIG_AST` instead of exhausting memory.
        size_t max_expanded_ast_elements = 0;
    };

    static void visit(ASTPtr & ast, size_t max_expanded_ast_elements);

private:
    static void visit(ASTPtr & ast, const Data & data);
};

}
