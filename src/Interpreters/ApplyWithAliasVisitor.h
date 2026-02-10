#pragma once

#include <map>

#include <Parsers/IAST.h>

namespace DB
{
/// Propagate every WITH alias expression to its descendent subqueries, with correct scoping visibility.
class ApplyWithAliasVisitor
{
public:
    struct Data
    {
        std::map<String, ASTPtr> exprs;
    };

    static void visit(ASTPtr & ast) { visit(ast, {}); }

private:
    static void visit(ASTPtr & ast, const Data & data);
};

}
