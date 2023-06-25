#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include "Interpreters/TreeRewriter.h"

namespace DB
{

class ASTFunction;

/// Simple rewrite:
///     'SELECT uniq(x) FROM (SELECT DISTINCT x ...)' to
///     'SELECT count() FROM (SELECT DISTINCT x ...)'
///
///     'SELECT uniq() FROM (SELECT x ... GROUP BY x)' to
///     'SELECT count() FROM (SELECT x ... GROUP BY x)'
///
/// Note we can rewrite all uniq variants except uniqUpTo.
class RewriteUniqToCountMatcher
{
public:
    struct Data {};
    static void visit(ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RewriteUniqToCountVisitor = InDepthNodeVisitor<RewriteUniqToCountMatcher, true>;
}
