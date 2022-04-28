#include <Common/typeid_cast.h>
#include <Parsers/queryToString.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/CrossToInnerJoinVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

struct JoinedElement
{
    explicit JoinedElement(const ASTTablesInSelectQueryElement & table_element)
        : element(table_element)
    {
        if (element.table_join)
            join = element.table_join->as<ASTTableJoin>();
    }

    void checkTableName(const DatabaseAndTableWithAlias & table, const String & current_database) const
    {
        if (!element.table_expression)
            throw Exception("Not a table expression in JOIN (ARRAY JOIN?)", ErrorCodes::LOGICAL_ERROR);

        ASTTableExpression * table_expression = element.table_expression->as<ASTTableExpression>();
        if (!table_expression)
            throw Exception("Wrong table expression in JOIN", ErrorCodes::LOGICAL_ERROR);

        if (!table.same(DatabaseAndTableWithAlias(*table_expression, current_database)))
            throw Exception("Inconsistent table names", ErrorCodes::LOGICAL_ERROR);
    }

    void rewriteCommaToCross()
    {
        if (join && join->kind == ASTTableJoin::Kind::Comma)
            join->kind = ASTTableJoin::Kind::Cross;
    }

    bool rewriteCrossToInner(ASTPtr on_expression)
    {
        if (join->kind != ASTTableJoin::Kind::Cross)
            return false;

        join->kind = ASTTableJoin::Kind::Inner;
        join->strictness = ASTTableJoin::Strictness::All;

        join->on_expression = on_expression;
        join->children.push_back(join->on_expression);
        return true;
    }

    ASTPtr arrayJoin() const { return element.array_join; }
    const ASTTableJoin * tableJoin() const { return join; }

    bool canAttachOnExpression() const { return join && !join->on_expression; }
    bool hasUsing() const { return join && join->using_expression_list; }

private:
    const ASTTablesInSelectQueryElement & element;
    ASTTableJoin * join = nullptr;
};

bool isAllowedToRewriteCrossJoin(const ASTPtr & node, const Aliases & aliases)
{
    if (node->as<ASTFunction>())
    {
        auto idents = IdentifiersCollector::collect(node);
        for (const auto * ident : idents)
        {
            if (ident->isShort() && aliases.contains(ident->shortName()))
                return false;
        }
        return true;
    }
    return node->as<ASTIdentifier>() || node->as<ASTLiteral>();
}

/// Return mapping table_no -> expression with expression that can be moved into JOIN ON section
std::map<size_t, std::vector<ASTPtr>> moveExpressionToJoinOn(
    const ASTPtr & ast,
    const std::vector<JoinedElement> & joined_tables,
    const std::vector<TableWithColumnNamesAndTypes> & tables,
    const Aliases & aliases)
{
    std::map<size_t, std::vector<ASTPtr>> asts_to_join_on;
    for (const auto & node : collectConjunctions(ast))
    {
        if (const auto * func = node->as<ASTFunction>(); func && func->name == NameEquals::name)
        {
            if (!func->arguments || func->arguments->children.size() != 2)
                return {};

            /// Check if the identifiers are from different joined tables.
            /// If it's a self joint, tables should have aliases.
            auto left_table_pos = IdentifierSemantic::getIdentsMembership(func->arguments->children[0], tables, aliases);
            auto right_table_pos = IdentifierSemantic::getIdentsMembership(func->arguments->children[1], tables, aliases);

            /// Identifiers from different table move to JOIN ON
            if (left_table_pos && right_table_pos && *left_table_pos != *right_table_pos)
            {
                size_t table_pos = std::max(*left_table_pos, *right_table_pos);
                if (joined_tables[table_pos].canAttachOnExpression())
                    asts_to_join_on[table_pos].push_back(node);
                else
                    return {};
            }
        }

        if (!isAllowedToRewriteCrossJoin(node, aliases))
            return {};
    }
    return asts_to_join_on;
}

ASTPtr makeOnExpression(const std::vector<ASTPtr> & expressions)
{
    if (expressions.size() == 1)
        return expressions[0]->clone();

    std::vector<ASTPtr> arguments;
    arguments.reserve(expressions.size());
    for (const auto & ast : expressions)
        arguments.emplace_back(ast->clone());

    return makeASTFunction(NameAnd::name, std::move(arguments));
}

std::vector<JoinedElement> getTables(const ASTSelectQuery & select)
{
    if (!select.tables())
        return {};

    const auto * tables = select.tables()->as<ASTTablesInSelectQuery>();
    if (!tables)
        return {};

    size_t num_tables = tables->children.size();
    if (num_tables < 2)
        return {};

    std::vector<JoinedElement> joined_tables;
    joined_tables.reserve(num_tables);
    bool has_using = false;

    for (const auto & child : tables->children)
    {
        const auto * table_element = child->as<ASTTablesInSelectQueryElement>();
        if (!table_element)
            throw Exception("Logical error: TablesInSelectQueryElement expected", ErrorCodes::LOGICAL_ERROR);

        JoinedElement & t = joined_tables.emplace_back(*table_element);
        t.rewriteCommaToCross();

        if (t.arrayJoin())
            return {};

        if (t.hasUsing())
        {
            if (has_using)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Multuple USING statements are not supported");
            has_using = true;
        }

        if (const auto * join = t.tableJoin(); join && isCrossOrComma(join->kind))
        {
            if (!join->children.empty())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "CROSS JOIN has {} expressions: [{}, ...]",
                    join->children.size(), queryToString(join->children[0]));
        }
    }

    return joined_tables;
}

}


bool CrossToInnerJoinMatcher::needChildVisit(ASTPtr & node, const ASTPtr &)
{
    return !node->as<ASTSubquery>();
}

void CrossToInnerJoinMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<ASTSelectQuery>())
        visit(*t, ast, data);
}

void CrossToInnerJoinMatcher::visit(ASTSelectQuery & select, ASTPtr &, Data & data)
{
    std::vector<JoinedElement> joined_tables = getTables(select);
    if (joined_tables.empty())
        return;

    /// Check if joined_tables are consistent with known tables_with_columns
    {
        if (joined_tables.size() != data.tables_with_columns.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Logical error: inconsistent number of tables: {} != {}",
                            joined_tables.size(), data.tables_with_columns.size());

        for (size_t i = 0; i < joined_tables.size(); ++i)
            joined_tables[i].checkTableName(data.tables_with_columns[i].table, data.current_database);
    }

    /// CROSS to INNER
    if (data.cross_to_inner_join_rewrite && select.where())
    {
        auto asts_to_join_on = moveExpressionToJoinOn(select.where(), joined_tables, data.tables_with_columns, data.aliases);
        for (size_t i = 1; i < joined_tables.size(); ++i)
        {
            const auto & expr_it = asts_to_join_on.find(i);
            if (expr_it != asts_to_join_on.end())
            {
                if (joined_tables[i].rewriteCrossToInner(makeOnExpression(expr_it->second)))
                    data.done = true;
            }
        }
    }
}

}
