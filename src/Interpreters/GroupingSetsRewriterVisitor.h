#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectQuery;

/// Rewrite GROUPING SETS with only one group, to GROUP BY.
///
/// Examples:
/// - GROUPING SETS (foo) -> GROUP BY foo
/// - GROUPING SETS ((foo, bar)) -> GROUP BY foo, bar
///
/// But not the following:
/// - GROUPING SETS (foo, bar) (since it has two groups (foo) and (bar))
class GroupingSetsRewriterData
{
public:
    using TypeToVisit = ASTSelectQuery;

    static void visit(ASTSelectQuery & select_query, ASTPtr &);
};

using GroupingSetsRewriterMatcher = OneTypeMatcher<GroupingSetsRewriterData>;
using GroupingSetsRewriterVisitor = InDepthNodeVisitor<GroupingSetsRewriterMatcher, true>;

}
