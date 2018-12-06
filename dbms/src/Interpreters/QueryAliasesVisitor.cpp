#include <ostream>
#include <sstream>
#include <Common/typeid_cast.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSubquery.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
}

void QueryAliasesVisitor::visit(const ASTPtr & ast) const
{
    /// Bottom-up traversal. We do not go into subqueries.
    visitChildren(ast);

    if (!tryVisit<ASTSubquery>(ast))
    {
        DumpASTNode dump(*ast, ostr, visit_depth, "getQueryAliases");
        visitOther(ast);
    }
}

/// The top-level aliases in the ARRAY JOIN section have a special meaning, we will not add them
/// (skip the expression list itself and its children).
void QueryAliasesVisitor::visit(const ASTArrayJoin &, const ASTPtr & ast) const
{
    for (auto & child1 : ast->children)
        for (auto & child2 : child1->children)
            for (auto & child3 : child2->children)
                visit(child3);
}

/// set unique aliases for all subqueries. this is needed, because:
/// 1) content of subqueries could change after recursive analysis, and auto-generated column names could become incorrect
/// 2) result of different scalar subqueries can be cached inside expressions compilation cache and must have different names
void QueryAliasesVisitor::visit(ASTSubquery & subquery, const ASTPtr & ast) const
{
    static std::atomic_uint64_t subquery_index = 0;

    if (subquery.alias.empty())
    {
        String alias;
        do
        {
            alias = "_subquery" + std::to_string(++subquery_index);
        }
        while (aliases.count(alias));

        subquery.setAlias(alias);
        subquery.prefer_alias_to_column_name = true;
        aliases[alias] = ast;
    }
    else
        visitOther(ast);
}

void QueryAliasesVisitor::visitOther(const ASTPtr & ast) const
{
    String alias = ast->tryGetAlias();
    if (!alias.empty())
    {
        if (aliases.count(alias) && ast->getTreeHash() != aliases[alias]->getTreeHash())
            throw Exception(wrongAliasMessage(ast, alias), ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

        aliases[alias] = ast;
    }
}

void QueryAliasesVisitor::visitChildren(const ASTPtr & ast) const
{
    for (auto & child : ast->children)
    {
        /// Don't descent into table functions and subqueries and special case for ArrayJoin.
        if (!tryVisit<ASTTableExpression>(ast) &&
            !tryVisit<ASTSelectWithUnionQuery>(ast) &&
            !tryVisit<ASTArrayJoin>(ast))
            visit(child);
    }
}

String QueryAliasesVisitor::wrongAliasMessage(const ASTPtr & ast, const String & alias) const
{
    std::stringstream message;
    message << "Different expressions with the same alias " << backQuoteIfNeed(alias) << ":" << std::endl;
    formatAST(*ast, message, false, true);
    message << std::endl << "and" << std::endl;
    formatAST(*aliases[alias], message, false, true);
    message << std::endl;
    return message.str();
}

}
