#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Rewrite 'any' and 'anyLast' functions pushing them inside original function.
/// any(f(x, y, g(z))) -> f(any(x), any(y), g(any(z)))
class RewriteAnyFunctionMatcher
{
public:
    struct Data {};

    static void visit(ASTPtr & ast, Data & data);
    static void visit(const ASTFunction &, ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};
using RewriteAnyFunctionVisitor = InDepthNodeVisitor<RewriteAnyFunctionMatcher, true>;

}
