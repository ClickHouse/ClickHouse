#pragma once

#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

/// Visitors consist of functions with unified interface 'void visit(Casted & x, ASTPtr & y)', there x is y, successfully casted to Casted.
/// Both types and fuction could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

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
    struct Data
    {
        const Context & context;
        size_t subquery_depth;
    };

    static constexpr const char * label = "ExecuteScalarSubqueries";

    static bool needChildVisit(ASTPtr & node, const ASTPtr &)
    {
        /// Processed
        if (typeid_cast<ASTSubquery *>(node.get()) ||
            typeid_cast<ASTFunction *>(node.get()))
            return false;

        /// Don't descend into subqueries in FROM section
        if (typeid_cast<ASTTableExpression *>(node.get()))
            return false;

        return true;
    }

    static std::vector<ASTPtr *> visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = typeid_cast<ASTSubquery *>(ast.get()))
            visit(*t, ast, data);
        if (auto * t = typeid_cast<ASTFunction *>(ast.get()))
            return visit(*t, ast, data);
        return {};
    }

private:
    static void visit(const ASTSubquery & subquery, ASTPtr & ast, Data & data);
    static std::vector<ASTPtr *> visit(const ASTFunction & func, ASTPtr & ast, Data & data);
};

using ExecuteScalarSubqueriesVisitor = InDepthNodeVisitor<ExecuteScalarSubqueriesMatcher, true>;

}
