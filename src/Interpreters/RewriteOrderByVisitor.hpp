#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{
///
/// Remove outer braces in ORDER BY
/// For example, rewrite (1) to (2)
/// (1) ... ORDER BY (a, b)
/// │    ExpressionList (children 1)              │
/// │     OrderByElement (children 1)             │
/// │      Function tuple (children 1)            │
/// │       ExpressionList (children 2)           │
/// │        Identifier CounterID                 │
/// │        Identifier EventDate                 │
/// (2) ... ORDER BY a,b
/// │    ExpressionList (children 2)              │
/// │     OrderByElement (children 1)             │
/// │      Identifier CounterID                   │
/// │     OrderByElement (children 1)             │
/// │      Identifier EventDate                   │
///
class RewriteOrderBy
{
public:
    struct Data {};
    static void visit(ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RewriteOrderByVisitor = InDepthNodeVisitor<RewriteOrderBy, true>;
}

