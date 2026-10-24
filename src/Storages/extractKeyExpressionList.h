#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{
    ASTPtr extractKeyExpressionList(const ASTPtr & node);

    /// Throws BAD_ARGUMENTS if the AST contains any subqueries or an `IN` operator with a table on the right-hand side.
    void checkExpressionDoesntContainSubqueries(const IAST & ast);

    /// Returns true if `checkExpressionDoesntContainSubqueries` would throw for the AST.
    bool expressionContainsSubqueries(const IAST & ast);
}
