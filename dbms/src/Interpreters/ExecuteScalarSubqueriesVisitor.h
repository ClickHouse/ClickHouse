#pragma once

#include <Common/typeid_cast.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class Context;
class ASTSubquery;
class ASTFunction;
struct ASTTableExpression;

/** Replace subqueries that return exactly one row
    * ("scalar" subqueries) to the corresponding constants.
    *
    * If the subquery returns more than one column, it is replaced by a tuple of constants.
    *
    * Features
    *
    * A replacement occurs during query analysis, and not during the main runtime.
    * This means that the progress indicator will not work during the execution of these requests,
    *  and also such queries can not be aborted.
    *
    * But the query result can be used for the index in the table.
    *
    * Scalar subqueries are executed on the request-initializer server.
    * The request is sent to remote servers with already substituted constants.
    */
class ExecuteScalarSubqueriesMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ExecuteScalarSubqueriesMatcher, true>;

    struct Data
    {
        const Context & context;
        size_t subquery_depth;
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr &);
    static void visit(ASTPtr & ast, Data & data);

private:
    static void visit(const ASTSubquery & subquery, ASTPtr & ast, Data & data);
    static void visit(const ASTFunction & func, ASTPtr & ast, Data & data);
};

using ExecuteScalarSubqueriesVisitor = ExecuteScalarSubqueriesMatcher::Visitor;

}
