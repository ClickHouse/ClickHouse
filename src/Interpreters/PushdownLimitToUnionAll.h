#pragma once

#include <unordered_set>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{
class ASTSelectQuery;
class ASTSelectWithUnionQuery;

/// LIMIT pushdown to UNION ALL queries,
/// SELECT * FROM (SELECT * FROM numbers(100000000) ORDER BY number) LIMIT 10 becomes
/// SELECT * FROM (SELECT * FROM numbers(100000000) ORDER BY number LIMIT 10) LIMIT 10.
/// This visitor must be down after NormalizeSelectWithUnionQueryVisitor.
class PushdownLimitToUnionAllMatcher
{
public:
    struct Data
    {
    };

    static void visit(ASTPtr & ast, Data &);
    static void visit(ASTSelectQuery &, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using PushdownLimitToUnionAllVisitor = InDepthNodeVisitor<PushdownLimitToUnionAllMatcher, true>;
}
