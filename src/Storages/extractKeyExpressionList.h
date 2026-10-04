#pragma once

#include <Parsers/IAST_fwd.h>

#include <string_view>

namespace DB
{
    ASTPtr extractKeyExpressionList(const ASTPtr & node);

    /// Throws BAD_ARGUMENTS if the AST contains any subqueries. `subject` opens the message, as in "Key expressions".
    void checkExpressionDoesntContainSubqueries(const IAST & ast, std::string_view subject);
}
