#pragma once

#include <Parsers/IAST_fwd.h>

#include <functional>

namespace DB
{
    ASTPtr extractKeyExpressionList(const ASTPtr & node);

    class ASTIdentifier;

    /// Throws BAD_ARGUMENTS if the AST contains any subqueries, or a table on the right-hand side of `IN`.
    /// An identifier on the right-hand side of `IN` for which `is_allowed_in_rhs_identifier` returns true
    /// is not treated as a table, e.g. when it names an array column of the table.
    void checkExpressionDoesntContainSubqueries(
        const IAST & ast, const std::function<bool(const ASTIdentifier &)> & is_allowed_in_rhs_identifier = {});

    /// Throws BAD_ARGUMENTS if the AST contains wildcards or column matchers
    /// (`*`, `t.*`, `COLUMNS(...)`).
    void checkExpressionDoesntContainMatchers(const IAST & ast);
}
