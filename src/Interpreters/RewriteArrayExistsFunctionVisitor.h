#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST.h>

namespace DB
{
class ASTFunction;

/// Rewrite possible 'arrayExists(func, arr)' to 'has(arr, elem)' to improve performance
/// arrayExists(x -> x = 1, arr) -> has(arr, 1)
class RewriteArrayExistsFunctionMatcher
{
public:
    struct Data
    {
    };

    static void visit(ASTPtr & ast, Data &);
    static void visit(const ASTFunction &, ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr & ast, const ASTPtr &);
};

using RewriteArrayExistsFunctionVisitor = InDepthNodeVisitor<RewriteArrayExistsFunctionMatcher, false>;
}
