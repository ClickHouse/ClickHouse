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
    extern const int NOT_IMPLEMENTED;
}

namespace
{

struct JoinedTable
{
    DatabaseAndTableWithAlias table;
    ASTTablesInSelectQueryElement * element = nullptr;
    ASTTableJoin * join = nullptr;

    JoinedTable(ASTPtr table_element)
    {
        element = typeid_cast<ASTTablesInSelectQueryElement *>(table_element.get());
        if (!element)
            throw Exception("Logical error: TablesInSelectQueryElement expected", ErrorCodes::LOGICAL_ERROR);

        if (element->table_join)
        {
            join = typeid_cast<ASTTableJoin *>(element->table_join.get());
            if (join->kind == ASTTableJoin::Kind::Cross ||
                join->kind == ASTTableJoin::Kind::Comma)
            {
                if (!join->children.empty())
                    throw Exception("Logical error: CROSS JOIN has expressions", ErrorCodes::LOGICAL_ERROR);
            }

            if (join->using_expression_list)
                throw Exception("Multiple CROSS/COMMA JOIN do not support USING", ErrorCodes::NOT_IMPLEMENTED);
        }

        auto & expr = typeid_cast<const ASTTableExpression &>(*element->table_expression);
        table = DatabaseAndTableWithAlias(expr);
    }

    void rewriteCommaToCross()
    {
        if (join)
            join->kind = ASTTableJoin::Kind::Cross;
    }

    bool canAttachOnExpression() const { return join && !join->on_expression; }
};

/// It checks if where expression could be moved to JOIN ON expression partially or entirely.
class CheckExpressionVisitorData
{
public:
    using TypeToVisit = const ASTFunction;

    CheckExpressionVisitorData(const std::vector<JoinedTable> & tables_)
        : tables(tables_)
        , ands_only(true)
    {}

    void visit(const ASTFunction & node, ASTPtr & ast)
    {
        if (!ands_only)
            return;

        if (node.name == "and")
        {
            if (!node.arguments || node.arguments->children.empty())
                throw Exception("Logical error: function requires argiment", ErrorCodes::LOGICAL_ERROR);

            for (auto & child : node.arguments->children)
            {
                if (auto func = typeid_cast<const ASTFunction *>(child.get()))
                    visit(*func, child);
                else
                    ands_only = false;
            }
        }
        else if (node.name == "equals")
        {
            if (size_t min_table = canMoveEqualsToJoinOn(node))
                asts_to_join_on[min_table].push_back(ast);
        }
        else
        {
            ands_only = false;
            asts_to_join_on.clear();
        }
    }

    bool complex() const { return !ands_only; }
    bool matchAny(size_t t) const { return asts_to_join_on.count(t); }

    ASTPtr makeOnExpression(size_t table_pos)
    {
        if (!asts_to_join_on.count(table_pos))
            return {};

        std::vector<ASTPtr> & expressions = asts_to_join_on[table_pos];

        if (expressions.size() == 1)
            return expressions[0]->clone();

        std::vector<ASTPtr> arguments;
        arguments.reserve(expressions.size());
        for (auto & ast : expressions)
            arguments.emplace_back(ast->clone());

        return makeASTFunction("and", std::move(arguments));
    }

private:
    const std::vector<JoinedTable> & tables;
    std::map<size_t, std::vector<ASTPtr>> asts_to_join_on;
    bool ands_only;

    size_t canMoveEqualsToJoinOn(const ASTFunction & node)
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
    /// @return table position to attach expression to or 0.
    size_t checkIdentifiers(const ASTIdentifier & left, const ASTIdentifier & right)
    {
        /// {best_match, berst_table_pos}
        std::pair<size_t, size_t> left_best{0, 0};
        std::pair<size_t, size_t> right_best{0, 0};

        for (size_t i = 0; i < tables.size(); ++i)
        {
            size_t match = IdentifierSemantic::canReferColumnToTable(left, tables[i].table);
            if (match > left_best.first)
            {
                left_best.first = match;
                left_best.second = i;
            }

            match = IdentifierSemantic::canReferColumnToTable(right, tables[i].table);
            if (match > right_best.first)
            {
                right_best.first = match;
                right_best.second = i;
            }
        }

        if (left_best.first && right_best.first && (left_best.second != right_best.second))
        {
            size_t table_pos = std::max(left_best.second, right_best.second);
            if (tables[table_pos].canAttachOnExpression())
                return table_pos;
        }
        return 0;
    }
};

using CheckExpressionMatcher = OneTypeMatcher<CheckExpressionVisitorData, false>;
using CheckExpressionVisitor = InDepthNodeVisitor<CheckExpressionMatcher, true>;


bool getTables(ASTSelectQuery & select, std::vector<JoinedTable> & joined_tables, size_t & num_comma)
{
    if (!select.tables)
        return false;

    auto tables = typeid_cast<const ASTTablesInSelectQuery *>(select.tables.get());
    if (!tables)
        return false;

    size_t num_tables = tables->children.size();
    if (num_tables < 2)
        return false;

    joined_tables.reserve(num_tables);
    for (auto & child : tables->children)
    {
        joined_tables.emplace_back(JoinedTable(child));

        if (ASTTableJoin * join = joined_tables.back().join)
            if (join->kind == ASTTableJoin::Kind::Comma)
                ++num_comma;
    }
    return true;
}

}


void CrossToInnerJoinMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = typeid_cast<ASTSelectQuery *>(ast.get()))
        visit(*t, ast, data);
}

void CrossToInnerJoinMatcher::visit(ASTSelectQuery & select, ASTPtr &, Data & data)
{
    size_t num_comma = 0;
    std::vector<JoinedTable> joined_tables;
    if (!getTables(select, joined_tables, num_comma))
        return;

    /// COMMA to CROSS

    if (num_comma)
    {
        if (num_comma != (joined_tables.size() - 1))
            throw Exception("Mix of COMMA and other JOINS is not supported", ErrorCodes::NOT_IMPLEMENTED);

        for (auto & table : joined_tables)
            table.rewriteCommaToCross();
    }

    /// CROSS to INNER

    if (!select.where_expression)
        return;

    CheckExpressionVisitor::Data visitor_data{joined_tables};
    CheckExpressionVisitor(visitor_data).visit(select.where_expression);

    if (visitor_data.complex())
        return;

    for (size_t i = 1; i < joined_tables.size(); ++i)
    {
        if (visitor_data.matchAny(i))
        {
            ASTTableJoin & join = *joined_tables[i].join;
            join.kind = ASTTableJoin::Kind::Inner;
            join.strictness = ASTTableJoin::Strictness::All;

            join.on_expression = visitor_data.makeOnExpression(i);
            join.children.push_back(join.on_expression);
            data.done = true;
        }
    }
}

}
