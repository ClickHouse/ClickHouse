#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTSelectQuery;

/// Erases unnecessary ORDER BY from subquery
class DuplicateOrderByFromSubqueriesData
{
public:
    using TypeToVisit = ASTSelectQuery;

    bool done = false;

    void visit(ASTSelectQuery & select_query, ASTPtr &);
};

using DuplicateOrderByFromSubqueriesMatcher = OneTypeMatcher<DuplicateOrderByFromSubqueriesData>;
using DuplicateOrderByFromSubqueriesVisitor = InDepthNodeVisitor<DuplicateOrderByFromSubqueriesMatcher, true>;


/// Finds SELECT that can be optimized
class DuplicateOrderByData
{
public:
    using TypeToVisit = ASTSelectQuery;

    ContextPtr context;

    void visit(ASTSelectQuery & select_query, ASTPtr &);
};

using DuplicateOrderByMatcher = OneTypeMatcher<DuplicateOrderByData>;
using DuplicateOrderByVisitor = InDepthNodeVisitor<DuplicateOrderByMatcher, true>;

}
