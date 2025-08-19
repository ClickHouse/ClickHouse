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

namespace
{

    constexpr auto dummy_subquery_name_prefix = "_subquery";

    PreformattedMessage wrongAliasMessage(const ASTPtr & ast, const ASTPtr & prev_ast, const String & alias)
    {
        return PreformattedMessage::create("Different expressions with the same alias {}:\n{}\nand\n{}\n",
                                           backQuoteIfNeed(alias), serializeAST(*ast), serializeAST(*prev_ast));
    }

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
            alias = dummy_subquery_name_prefix + std::to_string(++subquery_index);
        }
        while (aliases.contains(alias));

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
        if (aliases.contains(alias) && ast->getTreeHash(/*ignore_aliases=*/ true) != aliases[alias]->getTreeHash(/*ignore_aliases=*/ true))
            throw Exception(wrongAliasMessage(ast, aliases[alias], alias), ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

        aliases[alias] = ast;
    }

    /** QueryAliasesVisitor is executed before ExecuteScalarSubqueriesVisitor.
        For example we have subquery in our query (SELECT sum(number) FROM numbers(10)).

        After running QueryAliasesVisitor it will be (SELECT sum(number) FROM numbers(10)) as _subquery_1
        and prefer_alias_to_column_name for this subquery will be true.

        After running ExecuteScalarSubqueriesVisitor it will be converted to (45 as _subquery_1)
        and prefer_alias_to_column_name for ast literal will be true.

        But if we send such query on remote host with Distributed engine for example we cannot send prefer_alias_to_column_name
        information for our ast node with query string. And this alias will be dropped because prefer_alias_to_column_name for ASTWIthAlias
        by default is false.

        It is important that subquery can be converted to literal during ExecuteScalarSubqueriesVisitor.
        And code below check if we previously set for subquery alias as _subquery, and if it is true
        then set prefer_alias_to_column_name = true for node that was optimized during ExecuteScalarSubqueriesVisitor.
     */

    if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(ast.get()))
    {
        if (startsWith(alias, dummy_subquery_name_prefix))
            ast_with_alias->prefer_alias_to_column_name = true;
    }
}

/// Explicit template instantiations
template class QueryAliasesMatcher<QueryAliasesWithSubqueries>;
template class QueryAliasesMatcher<QueryAliasesNoSubqueries>;

}
