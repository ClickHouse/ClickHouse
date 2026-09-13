#pragma once

#include <map>

#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTFunction;
class ASTSelectQuery;
class ASTSelectWithUnionQuery;
struct ASTTableExpression;

class ApplyWithSubqueryVisitor
{
public:
    struct Data
    {
        std::map<String, ASTPtr> subqueries;
        std::map<String, ASTPtr> literals;
        /// When set, each subquery's own settings are applied while descending, so that an inherited
        /// `subqueries` element is not substituted into a subquery whose settings hide it. Inherited
        /// `literals` are substituted either way.
        ContextPtr context;
    };

    static void visit(ASTPtr & ast) { visit(ast, Data{}); }
    static void visit(ASTPtr & ast, ContextPtr context)
    {
        Data data;
        data.context = std::move(context);
        visit(ast, data);
    }
    static void visit(ASTSelectQuery & select) { visit(select, {}); }
    static void visit(ASTSelectWithUnionQuery & select) { visit(select, {}); }

private:
    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTSelectQuery & ast, const Data & data);
    static void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
};

}
