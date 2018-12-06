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

static String wrongAliasMessage(const ASTPtr & ast, const ASTPtr & prev_ast, const String & alias)
{
    std::stringstream message;
    message << "Different expressions with the same alias " << backQuoteIfNeed(alias) << ":" << std::endl;
    formatAST(*ast, message, false, true);
    message << std::endl << "and" << std::endl;
    formatAST(*prev_ast, message, false, true);
    message << std::endl;
    return message.str();
}


bool QueryAliasesMatcher::needChildVisit(ASTPtr & node, const ASTPtr &)
{
    /// Don't descent into table functions and subqueries and special case for ArrayJoin.
    if (typeid_cast<ASTTableExpression *>(node.get()) ||
        typeid_cast<ASTSelectWithUnionQuery *>(node.get()) ||
        typeid_cast<ASTArrayJoin *>(node.get()))
        return false;
    return true;
}

std::vector<ASTPtr> QueryAliasesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = typeid_cast<ASTSubquery *>(ast.get()))
        return visit(*t, ast, data);
    if (auto * t = typeid_cast<ASTArrayJoin *>(ast.get()))
        return visit(*t, ast, data);

    visitOther(ast, data);
    return {};
}

/// The top-level aliases in the ARRAY JOIN section have a special meaning, we will not add them
/// (skip the expression list itself and its children).
std::vector<ASTPtr> QueryAliasesMatcher::visit(const ASTArrayJoin &, const ASTPtr & ast, Data & data)
{
    visitOther(ast, data);

    /// @warning It breaks botom-to-top order (childs processed after node here), could lead to some effects.
    /// It's possible to add ast back to result vec to save order. It will need two phase ASTArrayJoin visit (setting phase in data).
    std::vector<ASTPtr> out;
    for (auto & child1 : ast->children)
        for (auto & child2 : child1->children)
            for (auto & child3 : child2->children)
                out.push_back(child3);
    return out;
}

/// set unique aliases for all subqueries. this is needed, because:
/// 1) content of subqueries could change after recursive analysis, and auto-generated column names could become incorrect
/// 2) result of different scalar subqueries can be cached inside expressions compilation cache and must have different names
std::vector<ASTPtr> QueryAliasesMatcher::visit(ASTSubquery & subquery, const ASTPtr & ast, Data & data)
{
    Aliases & aliases = data.aliases;

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
        visitOther(ast, data);
    return {};
}

void QueryAliasesMatcher::visitOther(const ASTPtr & ast, Data & data)
{
    Aliases & aliases = data.aliases;

    String alias = ast->tryGetAlias();
    if (!alias.empty())
    {
        if (aliases.count(alias) && ast->getTreeHash() != aliases[alias]->getTreeHash())
            throw Exception(wrongAliasMessage(ast, aliases[alias], alias), ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

        aliases[alias] = ast;
    }
}

}
