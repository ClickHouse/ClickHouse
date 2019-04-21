#include <Common/typeid_cast.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/JoinToSubqueryTransformVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/AsteriskSemantic.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/Context.h>
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
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_IDENTIFIER;
}

NamesAndTypesList getNamesAndTypeListFromTableExpression(const ASTTableExpression & table_expression, const Context & context);

namespace
{

/// Replace asterisks in select_expression_list with column identifiers
class ExtractAsterisksMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ExtractAsterisksMatcher, true>;

    struct Data
    {
        std::unordered_map<String, NamesAndTypesList> table_columns;
        std::vector<String> tables_order;
        std::shared_ptr<ASTExpressionList> new_select_expression_list;

        Data(const Context & context, const std::vector<const ASTTableExpression *> & table_expressions)
        {
            tables_order.reserve(table_expressions.size());
            for (const auto & expr : table_expressions)
            {
                if (expr->subquery)
                {
                    table_columns.clear();
                    tables_order.clear();
                    break;
                }

                String table_name = DatabaseAndTableWithAlias(*expr, context.getCurrentDatabase()).getQualifiedNamePrefix(false);
                NamesAndTypesList columns = getNamesAndTypeListFromTableExpression(*expr, context);
                tables_order.push_back(table_name);
                table_columns.emplace(std::move(table_name), std::move(columns));
            }
        }

        void addTableColumns(const String & table_name)
        {
            auto it = table_columns.find(table_name);
            if (it == table_columns.end())
                throw Exception("Unknown qualified identifier: " + table_name, ErrorCodes::UNKNOWN_IDENTIFIER);

            for (const auto & column : it->second)
                new_select_expression_list->children.push_back(
                    std::make_shared<ASTIdentifier>(std::vector<String>{it->first, column.name}));
        }
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return false; }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTSelectQuery>())
            visit(*t, ast, data);
        if (auto * t = ast->as<ASTExpressionList>())
            visit(*t, ast, data);
    }

private:
    static void visit(ASTSelectQuery & node, ASTPtr &, Data & data)
    {
        if (data.table_columns.empty())
            return;

        Visitor(data).visit(node.refSelect());
        if (!data.new_select_expression_list)
            return;

        node.setExpression(ASTSelectQuery::Expression::SELECT, std::move(data.new_select_expression_list));
    }

    static void visit(ASTExpressionList & node, ASTPtr &, Data & data)
    {
        bool has_asterisks = false;
        data.new_select_expression_list = std::make_shared<ASTExpressionList>();
        data.new_select_expression_list->children.reserve(node.children.size());

        for (auto & child : node.children)
        {
            if (child->as<ASTAsterisk>())
            {
                has_asterisks = true;

                for (auto & table_name : data.tables_order)
                    data.addTableColumns(table_name);
            }
            else if (child->as<ASTQualifiedAsterisk>())
            {
                has_asterisks = true;

                if (child->children.size() != 1)
                    throw Exception("Logical error: qualified asterisk must have exactly one child", ErrorCodes::LOGICAL_ERROR);
                ASTIdentifier & identifier = child->children[0]->as<ASTIdentifier &>();

                data.addTableColumns(identifier.name);
            }
            else
                data.new_select_expression_list->children.push_back(child);
        }

        if (!has_asterisks)
            data.new_select_expression_list.reset();
    }
};

/// Find columns with aliases to push them into rewritten subselects.
/// Normalize table aliases: table_name.column_name -> table_alias.column_name
/// Make aliases maps (alias -> column_name, column_name -> alias)
struct ColumnAliasesMatcher
{
    struct Data
    {
        const std::vector<DatabaseAndTableWithAlias> tables;
        bool public_names;
        AsteriskSemantic::RevertedAliases rev_aliases;  /// long_name -> aliases
        std::unordered_map<String, String> aliases;     /// alias -> long_name
        std::vector<std::pair<ASTIdentifier *, bool>> compound_identifiers;
        std::set<String> allowed_long_names;            /// original names allowed as aliases '--t.x as t.x' (select expressions only).

        Data(const std::vector<DatabaseAndTableWithAlias> && tables_)
            : tables(tables_)
            , public_names(false)
        {}

        void replaceIdentifiersWithAliases()
        {
            String hide_prefix = "--"; /// @note restriction: user should not use alises like `--table.column`

            for (auto & [identifier, is_public] : compound_identifiers)
            {
                String long_name = identifier->name;

                auto it = rev_aliases.find(long_name);
                if (it == rev_aliases.end())
                {
                    bool last_table = IdentifierSemantic::canReferColumnToTable(*identifier, tables.back());
                    if (!last_table)
                    {
                        String alias = hide_prefix + long_name;
                        aliases[alias] = long_name;
                        rev_aliases[long_name].push_back(alias);

                        identifier->setShortName(alias);
                        if (is_public)
                        {
                            identifier->setAlias(long_name);
                            allowed_long_names.insert(long_name);
                        }
                    }
                    else if (is_public)
                        identifier->setAlias(long_name); /// prevent crop long to short name
                }
                else
                {
                    if (it->second.empty())
                        throw Exception("No alias for '" + long_name + "'", ErrorCodes::LOGICAL_ERROR);

                    if (is_public && allowed_long_names.count(long_name))
                        ; /// leave original name unchanged for correct output
                    else
                        identifier->setShortName(it->second[0]);
                }
            }
        }
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr &)
    {
        if (node->as<ASTQualifiedAsterisk>())
            return false;
        return true;
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTIdentifier>())
            visit(*t, ast, data);

        if (ast->as<ASTAsterisk>() || ast->as<ASTQualifiedAsterisk>())
            throw Exception("Multiple JOIN do not support asterisks for complex queries yet", ErrorCodes::NOT_IMPLEMENTED);
    }

    static void visit(ASTIdentifier & node, ASTPtr &, Data & data)
    {
        if (node.isShort())
            return;

        bool last_table = false;
        String long_name;
        for (auto & table : data.tables)
        {
            if (IdentifierSemantic::canReferColumnToTable(node, table))
            {
                if (!long_name.empty())
                    throw Exception("Cannot refer column '" + node.name + "' to one table", ErrorCodes::AMBIGUOUS_COLUMN_NAME);
                IdentifierSemantic::setColumnLongName(node, table); /// table_name.column_name -> table_alias.column_name
                long_name = node.name;
                if (&table == &data.tables.back())
                    last_table = true;
            }
        }

        if (long_name.empty())
            throw Exception("Cannot refer column '" + node.name + "' to table", ErrorCodes::AMBIGUOUS_COLUMN_NAME);

        String alias = node.tryGetAlias();
        if (!alias.empty())
        {
            data.aliases[alias] = long_name;
            data.rev_aliases[long_name].push_back(alias);

            if (!last_table)
            {
                node.setShortName(alias);
                node.setAlias("");
            }
        }
        else if (node.compound())
            data.compound_identifiers.emplace_back(&node, data.public_names);
    }
};

/// Attach additional semantic info to generated selects.
struct AppendSemanticVisitorData
{
    using TypeToVisit = ASTSelectQuery;

    AsteriskSemantic::RevertedAliasesPtr rev_aliases = {};
    bool done = false;

    void visit(ASTSelectQuery & select, ASTPtr &)
    {
        if (done || !rev_aliases || !select.select())
            return;

        for (auto & child : select.select()->children)
        {
            if (auto * node = child->as<ASTAsterisk>())
                AsteriskSemantic::setAliases(*node, rev_aliases);
            if (auto * node = child->as<ASTQualifiedAsterisk>())
                AsteriskSemantic::setAliases(*node, rev_aliases);
        }

        done = true;
    }
};


/// Replaces table elements with pair.
struct RewriteTablesVisitorData
{
    using TypeToVisit = ASTTablesInSelectQuery;

    ASTPtr left;
    ASTPtr right;
    bool done = false;

    /// @note Do not change ASTTablesInSelectQuery itself. No need to change select.tables.
    void visit(ASTTablesInSelectQuery &, ASTPtr & ast)
    {
        if (done)
            return;
        std::vector<ASTPtr> new_tables{left, right};
        ast->children.swap(new_tables);
        done = true;
    }
};

bool needRewrite(ASTSelectQuery & select, std::vector<const ASTTableExpression *> & table_expressions)
{
    if (!select.tables())
        return false;

    const auto * tables = select.tables()->as<ASTTablesInSelectQuery>();
    if (!tables)
        return false;

    size_t num_tables = tables->children.size();
    if (num_tables <= 2)
        return false;

    size_t num_array_join = 0;
    size_t num_using = 0;

    table_expressions.reserve(num_tables);
    for (size_t i = 0; i < num_tables; ++i)
    {
        const auto * table = tables->children[i]->as<ASTTablesInSelectQueryElement>();
        if (!table)
            throw Exception("Table expected", ErrorCodes::LOGICAL_ERROR);

        if (table->table_expression)
            if (const auto * expression = table->table_expression->as<ASTTableExpression>())
                table_expressions.push_back(expression);
        if (!i)
            continue;

        if (!table->table_join && !table->array_join)
            throw Exception("Joined table expected", ErrorCodes::LOGICAL_ERROR);

        if (table->array_join)
        {
            ++num_array_join;
            continue;
        }

        const auto & join = table->table_join->as<ASTTableJoin &>();
        if (isComma(join.kind))
            throw Exception("COMMA to CROSS JOIN rewriter is not enabled or cannot rewrite query", ErrorCodes::NOT_IMPLEMENTED);

        if (join.using_expression_list)
            ++num_using;
    }

    if (num_tables - num_array_join <= 2)
        return false;

    /// it's not trivial to support mix of JOIN ON & JOIN USING cause of short names
    if (num_using)
        throw Exception("Multiple JOIN does not support USING", ErrorCodes::NOT_IMPLEMENTED);
    if (num_array_join)
        throw Exception("Multiple JOIN does not support mix with ARRAY JOINs", ErrorCodes::NOT_IMPLEMENTED);
    return true;
}

using RewriteMatcher = OneTypeMatcher<RewriteTablesVisitorData>;
using RewriteVisitor = InDepthNodeVisitor<RewriteMatcher, true>;
using ExtractAsterisksVisitor = ExtractAsterisksMatcher::Visitor;
using ColumnAliasesVisitor = InDepthNodeVisitor<ColumnAliasesMatcher, true>;
using AppendSemanticMatcher = OneTypeMatcher<AppendSemanticVisitorData>;
using AppendSemanticVisitor = InDepthNodeVisitor<AppendSemanticMatcher, true>;

} /// namelesspace


void JoinToSubqueryTransformMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<ASTSelectQuery>())
        visit(*t, ast, data);
}

void JoinToSubqueryTransformMatcher::visit(ASTSelectQuery & select, ASTPtr & ast, Data & data)
{
    using RevertedAliases = AsteriskSemantic::RevertedAliases;

    std::vector<const ASTTableExpression *> table_expressions;
    if (!needRewrite(select, table_expressions))
        return;

    ExtractAsterisksVisitor::Data asterisks_data(data.context, table_expressions);
    ExtractAsterisksVisitor(asterisks_data).visit(ast);

    ColumnAliasesVisitor::Data aliases_data(getDatabaseAndTables(select, ""));
    if (select.select())
    {
        aliases_data.public_names = true;
        ColumnAliasesVisitor(aliases_data).visit(select.refSelect());
        aliases_data.public_names = false;
    }
    if (select.where())
        ColumnAliasesVisitor(aliases_data).visit(select.refWhere());
    if (select.prewhere())
        ColumnAliasesVisitor(aliases_data).visit(select.refPrewhere());
    if (select.having())
        ColumnAliasesVisitor(aliases_data).visit(select.refHaving());

    /// JOIN sections
    for (auto & child : select.tables()->children)
    {
        auto * table = child->as<ASTTablesInSelectQueryElement>();
        if (table->table_join)
        {
            auto & join = table->table_join->as<ASTTableJoin &>();
            if (join.on_expression)
                ColumnAliasesVisitor(aliases_data).visit(join.on_expression);
        }
    }

    aliases_data.replaceIdentifiersWithAliases();

    auto rev_aliases = std::make_shared<RevertedAliases>();
    rev_aliases->swap(aliases_data.rev_aliases);

    auto & src_tables = select.tables()->children;
    ASTPtr left_table = src_tables[0];

    for (size_t i = 1; i < src_tables.size() - 1; ++i)
    {
        left_table = replaceJoin(left_table, src_tables[i]);
        if (!left_table)
            throw Exception("Cannot replace tables with subselect", ErrorCodes::LOGICAL_ERROR);

        /// attach data to generated asterisk
        AppendSemanticVisitor::Data semantic_data{rev_aliases, false};
        AppendSemanticVisitor(semantic_data).visit(left_table);
    }

    /// replace tables in select with generated two-table join
    RewriteVisitor::Data visitor_data{left_table, src_tables.back()};
    RewriteVisitor(visitor_data).visit(select.refTables());

    data.done = true;
}

static ASTPtr makeSubqueryTemplate()
{
    ParserTablesInSelectQueryElement parser(true);
    ASTPtr subquery_template = parseQuery(parser, "(select * from _t)", 0);
    if (!subquery_template)
        throw Exception("Cannot parse subquery template", ErrorCodes::LOGICAL_ERROR);
    return subquery_template;
}

ASTPtr JoinToSubqueryTransformMatcher::replaceJoin(ASTPtr ast_left, ASTPtr ast_right)
{
    const auto * left = ast_left->as<ASTTablesInSelectQueryElement>();
    const auto * right = ast_right->as<ASTTablesInSelectQueryElement>();
    if (!left || !right)
        throw Exception("Two TablesInSelectQueryElements expected", ErrorCodes::LOGICAL_ERROR);

    if (!right->table_join)
        throw Exception("Table join expected", ErrorCodes::LOGICAL_ERROR);

    static ASTPtr subquery_template = makeSubqueryTemplate();

    /// replace '_t' with pair of joined tables
    ASTPtr res = subquery_template->clone();
    RewriteVisitor::Data visitor_data{ast_left, ast_right};
    RewriteVisitor(visitor_data).visit(res);
    return res;
}

}
