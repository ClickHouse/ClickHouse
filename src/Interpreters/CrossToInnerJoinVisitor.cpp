#include <Common/typeid_cast.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <Interpreters/CrossToInnerJoinVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
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

bool isComparison(const String & name)
{
    return name == NameEquals::name ||
        name == NameNotEquals::name ||
        name == NameLess::name ||
        name == NameGreater::name ||
        name == NameLessOrEquals::name ||
        name == NameGreaterOrEquals::name;
}

/// It checks if where expression could be moved to JOIN ON expression partially or entirely.
class CheckExpressionVisitorData
{
public:
    using TypeToVisit = const ASTFunction;

    CheckExpressionVisitorData(const std::vector<JoinedElement> & tables_,
                               const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns,
                               const Aliases & aliases_)
        : joined_tables(tables_)
        , tables(tables_with_columns)
        , aliases(aliases_)
        , ands_only(true)
    {}

    void visit(const ASTFunction & node, const ASTPtr & ast)
    {
        if (!ands_only)
            return;

        if (node.name == NameAnd::name)
        {
            if (!node.arguments || node.arguments->children.empty())
                throw Exception("Logical error: function requires argument", ErrorCodes::LOGICAL_ERROR);

            for (auto & child : node.arguments->children)
            {
                if (const auto * func = child->as<ASTFunction>())
                    visit(*func, child);
                else
                    ands_only = false;
            }
        }
        else if (node.name == NameEquals::name)
        {
            if (size_t min_table = canMoveEqualsToJoinOn(node))
                asts_to_join_on[min_table].push_back(ast);
        }
        else if (isComparison(node.name))
        {
            /// leave other comparisons as is
        }
        else if (functionIsLikeOperator(node.name) || /// LIKE, NOT LIKE, ILIKE, NOT ILIKE
                 functionIsInOperator(node.name))  /// IN, NOT IN
        {
            /// leave as is. It's not possible to make push down here cause of unknown aliases and not implemented JOIN predicates.
            ///     select a as b form t1, t2 where t1.x = t2.x and b in(42)
            ///     select a as b form t1 inner join t2 on t1.x = t2.x and b in(42)
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

        return makeASTFunction(NameAnd::name, std::move(arguments));
    }

private:
    const std::vector<JoinedElement> & joined_tables;
    const std::vector<TableWithColumnNamesAndTypes> & tables;
    std::map<size_t, std::vector<ASTPtr>> asts_to_join_on;
    const Aliases & aliases;
    bool ands_only;

    size_t canMoveEqualsToJoinOn(const ASTFunction & node)
    {
        if (!node.arguments)
            throw Exception("Logical error: function requires arguments", ErrorCodes::LOGICAL_ERROR);
        if (node.arguments->children.size() != 2)
            return false;

        const auto * left = node.arguments->children[0]->as<ASTIdentifier>();
        const auto * right = node.arguments->children[1]->as<ASTIdentifier>();
        if (!left || !right)
            return false;

        /// Moving expressions that use column aliases is not supported.
        if (left->isShort() && aliases.count(left->shortName()))
            return false;
        if (right->isShort() && aliases.count(right->shortName()))
            return false;

        return checkIdentifiers(*left, *right);
    }

    /// Check if the identifiers are from different joined tables. If it's a self joint, tables should have aliases.
    /// select * from t1 a cross join t2 b where a.x = b.x
    /// @return table position to attach expression to or 0.
    size_t checkIdentifiers(const ASTIdentifier & left, const ASTIdentifier & right)
    {
        std::optional<size_t> left_table_pos = IdentifierSemantic::getMembership(left);
        if (!left_table_pos)
            left_table_pos = IdentifierSemantic::chooseTableColumnMatch(left, tables);

        std::optional<size_t> right_table_pos = IdentifierSemantic::getMembership(right);
        if (!right_table_pos)
            right_table_pos = IdentifierSemantic::chooseTableColumnMatch(right, tables);

        if (left_table_pos && right_table_pos && (*left_table_pos != *right_table_pos))
        {
            size_t table_pos = std::max(*left_table_pos, *right_table_pos);
            if (joined_tables[table_pos].canAttachOnExpression())
                return table_pos;
        }
        return 0;
    }
};

using CheckExpressionMatcher = ConstOneTypeMatcher<CheckExpressionVisitorData, NeedChild::none>;
using CheckExpressionVisitor = ConstInDepthNodeVisitor<CheckExpressionMatcher, true>;


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

    if (!select.where())
        return;

    CheckExpressionVisitor::Data visitor_data{joined_tables, data.tables_with_columns, data.aliases};
    CheckExpressionVisitor(visitor_data).visit(select.where());

    if (visitor_data.complex())
        return;

    for (size_t i = 1; i < joined_tables.size(); ++i)
    {
        if (visitor_data.matchAny(i))
        {
            if (joined_tables[i].rewriteCrossToInner(visitor_data.makeOnExpression(i)))
                data.done = true;
        }
    }
}

}
