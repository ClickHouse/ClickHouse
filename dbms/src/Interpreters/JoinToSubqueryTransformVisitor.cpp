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

/// Attaches 'with' section to the first visited ASTSelectQuery
struct AppendWithSectionVisitorData
{
    using TypeToVisit = ASTSelectQuery;

    const ASTPtr & with;
    bool done = false;

    void visit(ASTSelectQuery & select, ASTPtr &)
    {
        if (done || !with)
            return;

        if (select.with_expression_list)
        {
            for (auto & expr : with->children)
                select.with_expression_list->children.push_back(expr->clone());
        }
        else
            select.with_expression_list = with->clone();
        done = true;
    }
};

/// Replaces one table element with pair
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


static String getTableNameOrAlias(const ASTPtr & table_element)
{
    auto element = static_cast<const ASTTablesInSelectQueryElement *>(table_element.get());
    if (!element || element->children.empty())
        throw Exception("Expected TablesInSelectQueryElement with at least one child", ErrorCodes::LOGICAL_ERROR);

    auto table_expression = static_cast<const ASTTableExpression *>(element->children[0].get());
    if (!table_expression || table_expression->children.empty())
        throw Exception("Expected TableExpression with at least one child", ErrorCodes::LOGICAL_ERROR);

    String result = table_expression->children[0]->tryGetAlias();
    if (!result.empty())
        return result;

    auto identifier = static_cast<const ASTIdentifier *>(table_expression->children[0].get());
    if (!identifier)
        throw Exception("Expected Identifier or subquery with alias", ErrorCodes::LOGICAL_ERROR);
    return identifier->name;
}

static void addHiddenNames(ASTPtr & with_expression_list, const std::vector<String> & hidden_names, const String & new_name)
{
    if (!with_expression_list)
        with_expression_list = std::make_shared<ASTExpressionList>();

    ParserExpression parser;
    for (auto & name : hidden_names)
    {
        if (name.empty())
            continue;

        String str_expression = "nameCoalesce(" + name + "," + new_name + ")";
        ASTPtr expr = parseQuery(parser, str_expression, 0);
        if (!expr)
            throw Exception("Cannot parse expression", ErrorCodes::LOGICAL_ERROR);

        with_expression_list->children.push_back(expr);
    }
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
    static const size_t max_joins = 64; /// TODO: settings.max_subquery_depth

    auto tables = static_cast<const ASTTablesInSelectQuery *>(select.tables.get());
    if (!tables)
        throw Exception("TablesInSelectQuery expected", ErrorCodes::LOGICAL_ERROR);

    size_t num_tables = tables->children.size();
    if (num_tables <= 2)
        return;

    if (num_tables > max_joins)
        throw Exception("Too much joins", ErrorCodes::TOO_DEEP_AST);

    ASTPtr left = tables->children[0];

    for (size_t i = 1; i < num_tables - 1; ++i)
    {
        ASTPtr right = tables->children[i];
        std::vector<String> hidden_names = {getTableNameOrAlias(left), getTableNameOrAlias(right)};
        String subquery_name = alias_prefix + toString(i);

        left = replaceJoin(left, right, select.with_expression_list, subquery_name);
        if (!left)
            return;

        addHiddenNames(select.with_expression_list, hidden_names, subquery_name);
    }

    select.tables = std::make_shared<ASTTablesInSelectQuery>();
    select.tables->children.push_back(left);
    select.tables->children.push_back(tables->children.back());

    ast = ast->clone(); /// rewrite AST in right manner
    data.done = true;
}

ASTPtr JoinToSubqueryTransformMatcher::replaceJoin(ASTPtr ast_left, ASTPtr ast_right, ASTPtr with, const String & subquery_alias)
{
    using RewriteMatcher = LinkedMatcher<
        OneTypeMatcher<RewriteTablesVisitorData>,
        OneTypeMatcher<AppendWithSectionVisitorData>>;
    using RewriteVisitor = InDepthNodeVisitor<RewriteMatcher, true>;

    auto left = static_cast<const ASTTablesInSelectQueryElement *>(ast_left.get());
    auto right = static_cast<const ASTTablesInSelectQueryElement *>(ast_right.get());
    if (!left || !right)
        throw Exception("Two TablesInSelectQueryElements expected", ErrorCodes::LOGICAL_ERROR);

    if (!right->table_join || right->array_join)
        return {};

    auto table_join = static_cast<const ASTTableJoin *>(right->table_join.get());
    if (table_join->kind != ASTTableJoin::Kind::Inner)
        return {};

    ParserTablesInSelectQueryElement parser(true);
    String subquery = "(select * from _t) as " + subquery_alias;
    ASTPtr res = parseQuery(parser, subquery, 0);
    if (!res)
        throw Exception("Cannot parse rewrite query", ErrorCodes::LOGICAL_ERROR);

    RewriteVisitor::Data visitor_data =
        std::make_pair<RewriteTablesVisitorData, AppendWithSectionVisitorData>({ast_left, ast_right}, {with});
    RewriteVisitor(visitor_data).visit(res);
    return res;
}

}
