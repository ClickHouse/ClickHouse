#include <Common/typeid_cast.h>
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

using TablesWithColumnNamesAndTypes = std::vector<TableWithColumnNamesAndTypes>;

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

/// Collect all identifiers from ast
class IdentifiersCollector
{
public:
    using ASTIdentPtr = const ASTIdentifier *;
    using ASTIdentifiers = std::vector<ASTIdentPtr>;
    struct Data
    {
        ASTIdentifiers idents;
    };

    static void visit(const ASTPtr & node, Data & data)
    {
        if (const auto * ident = node->as<ASTIdentifier>())
            data.idents.push_back(ident);
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static ASTIdentifiers collect(const ASTPtr & node)
    {
        IdentifiersCollector::Data ident_data;
        ConstInDepthNodeVisitor<IdentifiersCollector, true> ident_visitor(ident_data);
        ident_visitor.visit(node);
        return ident_data.idents;
    }
};

/// Split expression `expr_1 AND expr_2 AND ... AND expr_n` into vector `[expr_1, expr_2, ..., expr_n]`
void collectConjunctions(const ASTPtr & node, std::vector<ASTPtr> & members)
{
    if (const auto * func = node->as<ASTFunction>(); func && func->name == NameAnd::name)
    {
        for (const auto & child : func->arguments->children)
            collectConjunctions(child, members);
        return;
    }
    members.push_back(node);
}

std::optional<size_t> getIdentsMembership(const std::vector<const ASTIdentifier *> idents,
                                          const TablesWithColumnNamesAndTypes & tables,
                                          const Aliases & aliases)
{
    std::optional<size_t> result;
    for (const auto * ident : idents)
    {
        /// Moving expressions that use column aliases is not supported.
        if (ident->isShort() && aliases.count(ident->shortName()))
            return {};

        std::optional<size_t> pos = IdentifierSemantic::getMembership(*ident);
        if (!pos)
            pos = IdentifierSemantic::chooseTableColumnMatch(*ident, tables);

        if (!pos)
            return {};
        if (result && *pos != *result)
            return {};
        result = pos;
    }
    return result;
}

std::optional<std::pair<size_t, size_t>> getArgumentsMembership(
    const ASTPtr & left, const ASTPtr & right, const TablesWithColumnNamesAndTypes & tables, const Aliases & aliases, bool recursive)
{
    std::optional<size_t> left_table_pos, right_table_pos;
    if (recursive)
    {
        /// Collect all nested identifies
        left_table_pos = getIdentsMembership(IdentifiersCollector::collect(left), tables, aliases);
        right_table_pos = getIdentsMembership(IdentifiersCollector::collect(right), tables, aliases);
    }
    else
    {
        /// Use identifier only if it's on the top level
        const auto * left_ident = left->as<ASTIdentifier>();
        const auto * right_ident = right->as<ASTIdentifier>();
        if (left_ident && right_ident)
        {
            left_table_pos = getIdentsMembership({left_ident}, tables, aliases);
            right_table_pos = getIdentsMembership({right_ident}, tables, aliases);
        }
    }

    if (left_table_pos && right_table_pos)
        return std::make_pair(*left_table_pos, *right_table_pos);
    return {};
}

bool isAllowedToRewriteCrossJoin(const ASTPtr & node, const Aliases & aliases)
{
    if (node->as<ASTFunction>())
    {
        auto idents = IdentifiersCollector::collect(node);
        for (const auto * ident : idents)
        {
            if (ident->isShort() && aliases.count(ident->shortName()))
                return false;
        }
        return true;
    }
    return node->as<ASTIdentifier>() || node->as<ASTLiteral>();
}

bool canMoveExpressionToJoinOn(const ASTPtr & ast,
                               const std::vector<JoinedElement> & joined_tables,
                               const std::vector<TableWithColumnNamesAndTypes> & tables,
                               const Aliases & aliases,
                               int rewrite_mode,
                               std::map<size_t, std::vector<ASTPtr>> & asts_to_join_on)
{
    std::vector<ASTPtr> conjuncts;
    collectConjunctions(ast, conjuncts);
    for (const auto & node : conjuncts)
    {
        if (const auto * func = node->as<ASTFunction>(); func && func->name == NameEquals::name)
        {
            if (!func->arguments || func->arguments->children.size() != 2)
                return false;

            bool optimistic_rewrite = rewrite_mode >= 2;
            auto table_pos = getArgumentsMembership(func->arguments->children[0], func->arguments->children[1],
                                                    tables, aliases, optimistic_rewrite);

            /// Check if the identifiers are from different joined tables.
            /// If it's a self joint, tables should have aliases.
            if (table_pos && table_pos->first != table_pos->second)
            {
                /// Identifiers from different table move to JOIN ON
                size_t max_table_pos = std::max(table_pos->first, table_pos->second);
                if (joined_tables[max_table_pos].canAttachOnExpression())
                    asts_to_join_on[max_table_pos].push_back(node);
                else
                    return false;
            }
        }

        if (!isAllowedToRewriteCrossJoin(node, aliases))
            return false;
    }
    return true;
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

bool getTables(ASTSelectQuery & select, std::vector<JoinedElement> & joined_tables, size_t & num_comma)
{
    if (!select.tables())
        return false;

    const auto * tables = select.tables()->as<ASTTablesInSelectQuery>();
    if (!tables)
        return false;

    size_t num_tables = tables->children.size();
    if (num_tables < 2)
        return false;

    joined_tables.reserve(num_tables);
    size_t num_array_join = 0;
    size_t num_using = 0;

    // For diagnostic messages.
    std::vector<IAST *> tables_with_using;
    tables_with_using.reserve(num_tables);

    for (const auto & child : tables->children)
    {
        auto * table_element = child->as<ASTTablesInSelectQueryElement>();
        if (!table_element)
            throw Exception("Logical error: TablesInSelectQueryElement expected", ErrorCodes::LOGICAL_ERROR);

        joined_tables.emplace_back(JoinedElement(*table_element));
        JoinedElement & t = joined_tables.back();

        if (t.arrayJoin())
        {
            ++num_array_join;
            continue;
        }

        if (t.hasUsing())
        {
            ++num_using;
            tables_with_using.push_back(table_element);
            continue;
        }

        if (const auto * join = t.tableJoin())
        {
            if (join->kind == ASTTableJoin::Kind::Cross ||
                join->kind == ASTTableJoin::Kind::Comma)
            {
                if (!join->children.empty())
                    throw Exception("Logical error: CROSS JOIN has expressions", ErrorCodes::LOGICAL_ERROR);
            }

            if (join->kind == ASTTableJoin::Kind::Comma)
                ++num_comma;
        }
    }

    if (num_using && (num_tables - num_array_join) > 2)
    {
        throw Exception("Multiple CROSS/COMMA JOIN do not support USING (while "
            "processing '" + IAST::formatForErrorMessage(tables_with_using) + "')",
            ErrorCodes::NOT_IMPLEMENTED);
    }

    return !(num_array_join || num_using);
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
    size_t num_comma = 0;
    std::vector<JoinedElement> joined_tables;
    if (!getTables(select, joined_tables, num_comma))
        return;

    /// Check if joined_tables are consistent with known tables_with_columns
    {
        if (joined_tables.size() != data.tables_with_columns.size())
            throw Exception("Logical error: inconsistent number of tables", ErrorCodes::LOGICAL_ERROR);

        for (size_t i = 0; i < joined_tables.size(); ++i)
            joined_tables[i].checkTableName(data.tables_with_columns[i].table, data.current_database);
    }

    /// COMMA to CROSS

    if (num_comma)
    {
        for (auto & table : joined_tables)
            table.rewriteCommaToCross();
    }

    /// CROSS to INNER

    if (select.where() && data.cross_to_inner_join_rewrite > 0)
    {
        std::map<size_t, std::vector<ASTPtr>> asts_to_join_on;
        bool can_move_where = canMoveExpressionToJoinOn(
            select.where(), joined_tables, data.tables_with_columns, data.aliases, data.cross_to_inner_join_rewrite, asts_to_join_on);
        if (can_move_where)
        {
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

}
