#pragma once

#include <Common/typeid_cast.h>
#include <Parsers/DumpASTNode.h>

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
class ExecuteScalarSubqueriesVisitor
{
public:
    ExecuteScalarSubqueriesVisitor(const Context & context_, size_t subquery_depth_, std::ostream * ostr_ = nullptr)
    :   context(context_),
        subquery_depth(subquery_depth_),
        visit_depth(0),
        ostr(ostr_)
    {}

    void visit(ASTPtr & ast) const
    {
        DumpASTNode dump(*ast, ostr, visit_depth, "executeScalarSubqueries");

        if (!tryVisit<ASTSubquery>(ast, dump) &&
            !tryVisit<ASTTableExpression>(ast, dump) &&
            !tryVisit<ASTFunction>(ast, dump))
            visitChildren(ast);
    }

private:
    const Context & context;
    size_t subquery_depth;
    mutable size_t visit_depth;
    std::ostream * ostr;

    void visit(const ASTSubquery * subquery, ASTPtr & ast, const DumpASTNode & dump) const;
    void visit(const ASTFunction * func, ASTPtr & ast, const DumpASTNode &) const;
    void visit(const ASTTableExpression *, ASTPtr &, const DumpASTNode &) const;

    void visitChildren(ASTPtr & ast) const
    {
        for (auto & child : ast->children)
            visit(child);
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast, const DumpASTNode & dump) const
    {
        if (const T * t = typeid_cast<const T *>(ast.get()))
        {
            visit(t, ast, dump);
            return true;
        }
        return false;
    }
};

}
