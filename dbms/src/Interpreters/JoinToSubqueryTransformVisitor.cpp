#include <Common/typeid_cast.h>
#include <Interpreters/JoinToSubqueryTransformVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_AST;
}

#if 0
/// Attach additional semantic info to generated select.
struct AppendSemanticVisitorData
{
    using TypeToVisit = ASTSelectQuery;

    const SemanticPtr & semantic;
    bool done = false;

    void visit(ASTSelectQuery & select, ASTPtr &)
    {
        if (done || !semantic)
            return;
        select.semantic = semantic->clone();
        done = true;
    }
};
#endif

/// Replaces one table element with pair.
struct RewriteTablesVisitorData
{
    using TypeToVisit = ASTTablesInSelectQuery;

    const ASTPtr & left;
    const ASTPtr & right;
    bool done = false;

    void visit(ASTTablesInSelectQuery &, ASTPtr & ast)
    {
        if (done)
            return;
        ast->children.clear();
        ast->children.push_back(left);
        ast->children.push_back(right);
        done = true;
    }
};

static bool needRewrite(ASTSelectQuery & select)
{
    if (!select.tables)
        return false;

    auto tables = typeid_cast<const ASTTablesInSelectQuery *>(select.tables.get());
    if (!tables)
        return false;

    size_t num_tables = tables->children.size();
    if (num_tables <= 2)
        return false;

    return true;
}

static void appendTableNameAndAlias(std::vector<String> & hidden, const ASTPtr & table_element)
{
    auto element = typeid_cast<const ASTTablesInSelectQueryElement *>(table_element.get());
    if (!element || element->children.empty())
        throw Exception("Expected TablesInSelectQueryElement with at least one child", ErrorCodes::LOGICAL_ERROR);

    auto table_expression = typeid_cast<const ASTTableExpression *>(element->children[0].get());
    if (!table_expression || table_expression->children.empty())
        throw Exception("Expected TableExpression with at least one child", ErrorCodes::LOGICAL_ERROR);

    String alias = table_expression->children[0]->tryGetAlias();
    if (!alias.empty())
        hidden.push_back(alias);

    if (auto opt_name = getIdentifierName(table_expression->children[0]))
        hidden.push_back(*opt_name);
    else if (alias.empty())
        throw Exception("Expected Identifier or subquery with alias", ErrorCodes::LOGICAL_ERROR);
}


std::vector<ASTPtr *> JoinToSubqueryTransformMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = typeid_cast<ASTSelectQuery *>(ast.get()))
        visit(*t, ast, data);
    return {};
}

void JoinToSubqueryTransformMatcher::visit(ASTSelectQuery & select, ASTPtr & ast, Data & data)
{
    static String alias_prefix = "__join"; /// FIXME

    if (!needRewrite(select))
        return;

    auto tables = typeid_cast<const ASTTablesInSelectQuery *>(select.tables.get());
    if (!tables)
        throw Exception("TablesInSelectQuery expected", ErrorCodes::LOGICAL_ERROR);

    size_t num_tables = tables->children.size();
    ASTPtr left = tables->children[0];

    for (size_t i = 1; i < num_tables - 1; ++i)
    {
        ASTPtr right = tables->children[i];
        std::vector<String> hidden_names;
        appendTableNameAndAlias(hidden_names, left);
        appendTableNameAndAlias(hidden_names, right);

        String subquery_name = alias_prefix + toString(i);

        left = replaceJoin(select, left, right, subquery_name);
        if (!left)
            return;

        //SemanticSelectQuery::hideNames(select, hidden_names, subquery_name);
    }

    select.tables = std::make_shared<ASTTablesInSelectQuery>();
    select.tables->children.push_back(left);
    select.tables->children.push_back(tables->children.back());

    ast = ast->clone(); /// rewrite AST in right manner
    data.done = true;
}

ASTPtr JoinToSubqueryTransformMatcher::replaceJoin(ASTSelectQuery &, ASTPtr ast_left, ASTPtr ast_right, const String & subquery_alias)
{
#if 0
    using RewriteMatcher = LinkedMatcher<
        OneTypeMatcher<RewriteTablesVisitorData>,
        OneTypeMatcher<AppendSemanticVisitorData>>;
#else
    using RewriteMatcher = OneTypeMatcher<RewriteTablesVisitorData>;
#endif
    using RewriteVisitor = InDepthNodeVisitor<RewriteMatcher, true>;

    auto left = typeid_cast<const ASTTablesInSelectQueryElement *>(ast_left.get());
    auto right = typeid_cast<const ASTTablesInSelectQueryElement *>(ast_right.get());
    if (!left || !right)
        throw Exception("Two TablesInSelectQueryElements expected", ErrorCodes::LOGICAL_ERROR);

    if (!right->table_join || right->array_join)
        return {};

    auto table_join = typeid_cast<const ASTTableJoin *>(right->table_join.get());
    if (table_join->kind != ASTTableJoin::Kind::Inner)
        return {};

    ParserTablesInSelectQueryElement parser(true);
    String subquery = "(select * from _t) as " + subquery_alias;
    ASTPtr res = parseQuery(parser, subquery, 0);
    if (!res)
        throw Exception("Cannot parse rewrite query", ErrorCodes::LOGICAL_ERROR);

#if 0
    RewriteVisitor::Data visitor_data =
        std::make_pair<RewriteTablesVisitorData, AppendSemanticVisitorData>({ast_left, ast_right}, {select.semantic});
#else
    RewriteVisitor::Data visitor_data{ast_left, ast_right};
#endif
    RewriteVisitor(visitor_data).visit(res);
    return res;
}

}
