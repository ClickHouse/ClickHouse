#pragma once

#include <map>

#include <base/types.h>
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
    };

    static void visit(ASTPtr & ast) { visit(ast, {}); }
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
