#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include "Interpreters/TreeRewriter.h"

namespace DB
{

class ASTFunction;

/** Optimize `uniq` into `count` over subquery.
 *     Example: 'SELECT uniq(x ...) FROM (SELECT DISTINCT x ...)' to
 *     Result: 'SELECT count() FROM (SELECT DISTINCT x ...)'
 *
 *     Example: 'SELECT uniq(x ...) FROM (SELECT x ... GROUP BY x ...)' to
 *     Result: 'SELECT count() FROM (SELECT x ... GROUP BY x ...)'
 *
 *     Note that we can rewrite all uniq variants except uniqUpTo.
 */
class RewriteUniqToCountMatcher
{
public:
    struct Data {};
    static void visit(ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RewriteUniqToCountVisitor = InDepthNodeVisitor<RewriteUniqToCountMatcher, true>;
}
