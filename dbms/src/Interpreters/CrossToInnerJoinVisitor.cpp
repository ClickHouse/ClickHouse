#include <Common/typeid_cast.h>
#include <Interpreters/CrossToInnerJoinVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
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
}

/// TODO: array join aliases?
struct CheckColumnsVisitorData
{
    using TypeToVisit = ASTIdentifier;

    const std::vector<DatabaseAndTableWithAlias> & tables;
    size_t visited;
    size_t found;

    size_t allMatch() const { return visited == found; }

    void visit(ASTIdentifier & node, ASTPtr &)
    {
        ++visited;
        for (const auto & t : tables)
            if (IdentifierSemantic::canReferColumnToTable(node, t))
                ++found;
    }
};


static bool extractTableName(const ASTTableExpression & expr, std::vector<DatabaseAndTableWithAlias> & names)
{
    /// Subselects are not supported.
    if (!expr.database_and_table_name)
        return false;

    names.emplace_back(DatabaseAndTableWithAlias(expr));
    return true;
}


static ASTPtr getCrossJoin(ASTSelectQuery & select, std::vector<DatabaseAndTableWithAlias> & table_names)
{
    if (!select.tables)
        return {};

    auto tables = typeid_cast<const ASTTablesInSelectQuery *>(select.tables.get());
    if (!tables)
        return {};

    size_t num_tables = tables->children.size();
    if (num_tables != 2)
        return {};

    auto left = typeid_cast<const ASTTablesInSelectQueryElement *>(tables->children[0].get());
    auto right = typeid_cast<const ASTTablesInSelectQueryElement *>(tables->children[1].get());
    if (!left || !right || !right->table_join)
        return {};

    if (auto join = typeid_cast<const ASTTableJoin *>(right->table_join.get()))
    {
        if (join->kind == ASTTableJoin::Kind::Cross)
        {
            if (!join->children.empty())
                throw Exception("Logical error: CROSS JOIN has expressions", ErrorCodes::LOGICAL_ERROR);

            auto & left_expr = typeid_cast<const ASTTableExpression &>(*left->table_expression);
            auto & right_expr = typeid_cast<const ASTTableExpression &>(*right->table_expression);

            table_names.reserve(2);
            if (extractTableName(left_expr, table_names) &&
                extractTableName(right_expr, table_names))
                return right->table_join;
        }
    }

    return {};
}


std::vector<ASTPtr *> CrossToInnerJoinMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = typeid_cast<ASTSelectQuery *>(ast.get()))
        visit(*t, ast, data);
    return {};
}

void CrossToInnerJoinMatcher::visit(ASTSelectQuery & select, ASTPtr & ast, Data & data)
{
    using CheckColumnsMatcher = OneTypeMatcher<CheckColumnsVisitorData>;
    using CheckColumnsVisitor = InDepthNodeVisitor<CheckColumnsMatcher, true>;

    std::vector<DatabaseAndTableWithAlias> table_names;
    ASTPtr ast_join = getCrossJoin(select, table_names);
    if (!ast_join)
        return;

    /// check Identifier names from where expression
    CheckColumnsVisitor::Data columns_data{table_names, 0, 0};
    CheckColumnsVisitor(columns_data).visit(select.where_expression);

    if (!columns_data.allMatch())
        return;

    auto & join = typeid_cast<ASTTableJoin &>(*ast_join);
    join.kind = ASTTableJoin::Kind::Inner;
    join.strictness = ASTTableJoin::Strictness::All; /// TODO: do we need it?

    join.on_expression.swap(select.where_expression);
    join.children.push_back(join.on_expression);

    ast = ast->clone(); /// rewrite AST in right manner
    data.done = true;
}

}
