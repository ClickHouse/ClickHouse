#include <ostream>
#include <sstream>

#include <Common/typeid_cast.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSubquery.h>
#include <Common/quoteString.h>

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


bool QueryAliasesWithSubqueries::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    /// Don't descent into table functions and subqueries and special case for ArrayJoin.
    return !(node->as<ASTTableExpression>() || node->as<ASTSelectWithUnionQuery>() || node->as<ASTArrayJoin>());
}

bool QueryAliasesNoSubqueries::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    if (node->as<ASTSubquery>())
        return false;
    return QueryAliasesWithSubqueries::needChildVisit(node, child);
}

template <typename T>
void QueryAliasesMatcher<T>::visit(const ASTPtr & ast, Data & data)
{
    if (auto * s = ast->as<ASTSubquery>())
        visit(*s, ast, data);
    else if (auto * q = ast->as<ASTSelectQuery>())
        visit(*q, ast, data);
    else if (auto * aj = ast->as<ASTArrayJoin>())
        visit(*aj, ast, data);
    else
        visitOther(ast, data);
}

template <typename T>
void QueryAliasesMatcher<T>::visit(const ASTSelectQuery & select, const ASTPtr &, Data &)
{
    ASTPtr with = select.with();
    if (!with)
        return;

    for (auto & child : with->children)
        if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(child.get()))
            ast_with_alias->prefer_alias_to_column_name = true;
}

/// The top-level aliases in the ARRAY JOIN section have a special meaning, we will not add them
/// (skip the expression list itself and its children).
template <typename T>
void QueryAliasesMatcher<T>::visit(const ASTArrayJoin &, const ASTPtr & ast, Data & data)
{
    visitOther(ast, data);

    std::vector<ASTPtr> grand_children;
    for (auto & child1 : ast->children)
        for (auto & child2 : child1->children)
            for (auto & child3 : child2->children)
                grand_children.push_back(child3);

    /// create own visitor to run bottom to top
    for (auto & child : grand_children)
        Visitor(data).visit(child);
}

/// set unique aliases for all subqueries. this is needed, because:
/// 1) content of subqueries could change after recursive analysis, and auto-generated column names could become incorrect
/// 2) result of different scalar subqueries can be cached inside expressions compilation cache and must have different names
template <typename T>
void QueryAliasesMatcher<T>::visit(const ASTSubquery & const_subquery, const ASTPtr & ast, Data & data)
{
    auto & aliases = data;
    ASTSubquery & subquery = const_cast<ASTSubquery &>(const_subquery);

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
        aliases[alias] = ast;
    }
    else
        visitOther(ast, aliases);

    subquery.prefer_alias_to_column_name = true;
}

template <typename T>
void QueryAliasesMatcher<T>::visitOther(const ASTPtr & ast, Data & data)
{
    auto & aliases = data;
    String alias = ast->tryGetAlias();
    if (!alias.empty())
    {
        if (aliases.count(alias) && ast->getTreeHash() != aliases[alias]->getTreeHash())
            throw Exception(wrongAliasMessage(ast, aliases[alias], alias), ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

        aliases[alias] = ast;
    }
}

/// Explicit template instantiations
template class QueryAliasesMatcher<QueryAliasesWithSubqueries>;
template class QueryAliasesMatcher<QueryAliasesNoSubqueries>;

}
