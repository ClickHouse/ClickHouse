#pragma once

#include <map>

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
    explicit ApplyWithSubqueryVisitor(ContextPtr context_);

    struct Data
    {
        std::map<String, ASTPtr> subqueries;
    };

    void visit(ASTPtr & ast) { visit(ast, {}); }
    void visit(ASTSelectQuery & select) { visit(select, {}); }
    void visit(ASTSelectWithUnionQuery & select) { visit(select, {}); }

private:
    void visit(ASTPtr & ast, const Data & data);
    void visit(ASTSelectQuery & ast, const Data & data);
    void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    void visit(ASTTableExpression & table, const Data & data);
    void visit(ASTFunction & func, const Data & data);

    const bool use_analyzer;
};

}
