#include <Common/typeid_cast.h>
#include <Interpreters/CrossToInnerJoinVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
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

/// It checks if where expression could be moved to JOIN ON expression partially or entirely.
class CheckExpressionVisitorData
{
public:
    using TypeToVisit = const ASTFunction;

    CheckExpressionVisitorData(const std::vector<DatabaseAndTableWithAlias> & tables_)
        : tables(tables_)
        , save_where(false)
        , flat_ands(true)
    {}

    void visit(const ASTFunction & node, ASTPtr & ast)
    {
        if (node.name == "and")
        {
            if (!node.arguments || node.arguments->children.empty())
                throw Exception("Logical error: function requires argiment", ErrorCodes::LOGICAL_ERROR);

            for (auto & child : node.arguments->children)
            {
                if (auto func = typeid_cast<const ASTFunction *>(child.get()))
                {
                    if (func->name == "and")
                        flat_ands = false;
                    visit(*func, child);
                }
                else
                    save_where = true;
            }
        }
        else if (node.name == "equals")
        {
            if (checkEquals(node))
                asts_to_join_on.push_back(ast);
            else
                save_where = true;
        }
        else
            save_where = true;
    }

    bool matchAny() const { return !asts_to_join_on.empty(); }
    bool matchAll() const { return matchAny() && !save_where; }
    bool canReuseWhere() const { return matchAll() && flat_ands; }

    ASTPtr makeOnExpression()
    {
        if (asts_to_join_on.size() == 1)
            return asts_to_join_on[0]->clone();

        std::vector<ASTPtr> arguments;
        arguments.reserve(asts_to_join_on.size());
        for (auto & ast : asts_to_join_on)
            arguments.emplace_back(ast->clone());

        return makeASTFunction("and", std::move(arguments));
    }

private:
    const std::vector<DatabaseAndTableWithAlias> & tables;
    std::vector<ASTPtr> asts_to_join_on;
    bool save_where;
    bool flat_ands;

    bool checkEquals(const ASTFunction & node)
    {
        if (!node.arguments)
            throw Exception("Logical error: function requires argiment", ErrorCodes::LOGICAL_ERROR);
        if (node.arguments->children.size() != 2)
            return false;

        auto left = typeid_cast<const ASTIdentifier *>(node.arguments->children[0].get());
        auto right = typeid_cast<const ASTIdentifier *>(node.arguments->children[1].get());
        if (!left || !right)
            return false;

        return checkIdentifiers(*left, *right);
    }

    /// Check if the identifiers are from different joined tables. If it's a self joint, tables should have aliases.
    /// select * from t1 a cross join t2 b where a.x = b.x
    bool checkIdentifiers(const ASTIdentifier & left, const ASTIdentifier & right)
    {
        /// {best_match, berst_table_pos}
        std::pair<size_t, size_t> left_best{0, 0};
        std::pair<size_t, size_t> right_best{0, 0};

        for (size_t i = 0; i < tables.size(); ++i)
        {
            size_t match = IdentifierSemantic::canReferColumnToTable(left, tables[i]);
            if (match > left_best.first)
            {
                left_best.first = match;
                left_best.second = i;
            }

            match = IdentifierSemantic::canReferColumnToTable(right, tables[i]);
            if (match > right_best.first)
            {
                right_best.first = match;
                right_best.second = i;
            }
        }

        return left_best.first && right_best.first && (left_best.second != right_best.second);
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
        if (join->kind == ASTTableJoin::Kind::Cross ||
            join->kind == ASTTableJoin::Kind::Comma)
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
    using CheckExpressionMatcher = OneTypeMatcher<CheckExpressionVisitorData, false>;
    using CheckExpressionVisitor = InDepthNodeVisitor<CheckExpressionMatcher, true>;

    std::vector<DatabaseAndTableWithAlias> table_names;
    ASTPtr ast_join = getCrossJoin(select, table_names);
    if (!ast_join)
        return;

    CheckExpressionVisitor::Data visitor_data{table_names};
    CheckExpressionVisitor(visitor_data).visit(select.where_expression);

    if (visitor_data.matchAny())
    {
        auto & join = typeid_cast<ASTTableJoin &>(*ast_join);
        join.kind = ASTTableJoin::Kind::Inner;
        join.strictness = ASTTableJoin::Strictness::All;

        if (visitor_data.canReuseWhere())
            join.on_expression.swap(select.where_expression);
        else
            join.on_expression = visitor_data.makeOnExpression();

        if (visitor_data.matchAll())
            select.where_expression.reset();

        join.children.push_back(join.on_expression);
    }

    ast = ast->clone(); /// rewrite AST in right manner
    data.done = true;
}

}
