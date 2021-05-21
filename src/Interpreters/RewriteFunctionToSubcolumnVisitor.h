#pragma once

#include <Parsers/ASTFunction.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

/// Rewrites functions to subcolumns, if possible, to reduce amount of read data.
/// E.g. 'length(arr)' -> 'arr.size0', 'col IS NULL' -> 'col.null'
class RewriteFunctionToSubcolumnData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast);

    const NameSet & columns_to_rewrite;
};

using RewriteFunctionToSubcolumnMatcher = OneTypeMatcher<RewriteFunctionToSubcolumnData>;
using RewriteFunctionToSubcolumnVisitor = InDepthNodeVisitor<RewriteFunctionToSubcolumnMatcher, true>;

}
