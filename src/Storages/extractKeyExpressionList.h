#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{
    ASTPtr extractKeyExpressionList(const ASTPtr & node);

    /// Throws BAD_ARGUMENTS if the AST contains any subqueries.
    void checkExpressionDoesntContainSubqueries(const IAST & ast);

    /// Throws BAD_ARGUMENTS if the AST contains wildcards or column matchers
    /// (`*`, `t.*`, `COLUMNS(...)`).
    void checkExpressionDoesntContainMatchers(const IAST & ast);
}
