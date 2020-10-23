#pragma once

#include <map>

#include <Parsers/IAST.h>

namespace DB
{
// TODO After we support `union_with_global`, this visitor should also be extended to match ASTSelectQueryWithUnion.
class ASTSelectQuery;
class ASTFunction;
struct ASTTableExpression;

class ApplyWithSubqueryVisitor
{
public:
    struct Data
    {
        std::map<String, ASTPtr> subqueries;
    };

    static void visit(ASTPtr & ast) { visit(ast, {}); }

private:
    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
};

}
