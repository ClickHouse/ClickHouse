#include <Common/typeid_cast.h>
#include <Interpreters/JoinToSubqueryTransformVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/AsteriskSemantic.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
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
}

namespace
{

/// Find columns with aliases to push them into rewritten subselects.
/// Normalize table aliases: table_name.column_name -> table_alias.column_name
/// Make aliases maps (alias -> column_name, column_name -> alias)
struct ColumnAliasesMatcher
{
    struct Data
    {
        const std::vector<DatabaseAndTableWithAlias> tables;
        bool public_names;
        AsteriskSemantic::RevertedAliases rev_aliases;
        std::unordered_map<String, String> aliases;
        std::vector<std::pair<ASTIdentifier *, bool>> compound_identifiers;

        Data(std::vector<DatabaseAndTableWithAlias> && tables_)
            : tables(tables_)
            , public_names(false)
        {}

        void replaceIdentifiersWithAliases()
        {
            String hide_prefix = "--"; /// @note restriction: user should not use alises like `--table.column`

            for (auto & [identifier, is_public] : compound_identifiers)
            {
                auto it = rev_aliases.find(identifier->name);
                if (it == rev_aliases.end())
                {
                    bool last_table = IdentifierSemantic::canReferColumnToTable(*identifier, tables.back());
                    if (!last_table)
                    {
                        String long_name = identifier->name;
                        String alias = hide_prefix + long_name;
                        aliases[alias] = long_name;
                        rev_aliases[long_name].push_back(alias);

                        identifier->setShortName(alias);
                        if (is_public)
                            identifier->setAlias(long_name);
                    }
                    else if (is_public)
                        identifier->setAlias(identifier->name); /// prevent crop long to short name
                }
                else
                {
                    if (it->second.empty())
                        throw Exception("No alias for '" + identifier->name + "'", ErrorCodes::LOGICAL_ERROR);
                    identifier->setShortName(it->second[0]);
                }
            }
        }
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr &)
    {
        if (typeid_cast<const ASTQualifiedAsterisk *>(node.get()))
            return false;
        return true;
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = typeid_cast<ASTIdentifier *>(ast.get()))
            visit(*t, ast, data);

        if (typeid_cast<ASTAsterisk *>(ast.get()) ||
            typeid_cast<ASTQualifiedAsterisk *>(ast.get()))
            throw Exception("Multiple JOIN do not support asterisks yet", ErrorCodes::NOT_IMPLEMENTED);
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
        else
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
        if (done || !rev_aliases || !select.select_expression_list)
            return;

        for (auto & child : select.select_expression_list->children)
        {
            if (auto * node = typeid_cast<ASTAsterisk *>(child.get()))
                AsteriskSemantic::setAliases(*node, rev_aliases);
            if (auto * node = typeid_cast<ASTQualifiedAsterisk *>(child.get()))
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

bool needRewrite(ASTSelectQuery & select)
{
    if (!select.tables)
        return false;

    auto tables = typeid_cast<const ASTTablesInSelectQuery *>(select.tables.get());
    if (!tables)
        return false;

    size_t num_tables = tables->children.size();
    if (num_tables <= 2)
        return false;

    for (size_t i = 1; i < tables->children.size(); ++i)
    {
        auto table = typeid_cast<const ASTTablesInSelectQueryElement *>(tables->children[i].get());
        if (!table || !table->table_join)
            throw Exception("Multiple JOIN expects joined tables", ErrorCodes::LOGICAL_ERROR);

        auto join = typeid_cast<const ASTTableJoin *>(table->table_join.get());
        if (join->kind == ASTTableJoin::Kind::Comma)
            throw Exception("Multiple COMMA JOIN is not supported", ErrorCodes::NOT_IMPLEMENTED);

        /// it's not trivial to support mix of JOIN ON & JOIN USING cause of short names
        if (!join || !join->on_expression)
            throw Exception("Multiple JOIN expects JOIN with ON section", ErrorCodes::NOT_IMPLEMENTED);
    }

    return true;
}

using RewriteMatcher = OneTypeMatcher<RewriteTablesVisitorData>;
using RewriteVisitor = InDepthNodeVisitor<RewriteMatcher, true>;
using ColumnAliasesVisitor = InDepthNodeVisitor<ColumnAliasesMatcher, true>;
using AppendSemanticMatcher = OneTypeMatcher<AppendSemanticVisitorData>;
using AppendSemanticVisitor = InDepthNodeVisitor<AppendSemanticMatcher, true>;

} /// namelesspace


void JoinToSubqueryTransformMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = typeid_cast<ASTSelectQuery *>(ast.get()))
        visit(*t, ast, data);
}

void JoinToSubqueryTransformMatcher::visit(ASTSelectQuery & select, ASTPtr &, Data & data)
{
    using RevertedAliases = AsteriskSemantic::RevertedAliases;

    if (!needRewrite(select))
        return;

    ColumnAliasesVisitor::Data aliases_data(getDatabaseAndTables(select, ""));
    if (select.select_expression_list)
    {
        aliases_data.public_names = true;
        ColumnAliasesVisitor(aliases_data).visit(select.select_expression_list);
        aliases_data.public_names = false;
    }
    if (select.where_expression)
        ColumnAliasesVisitor(aliases_data).visit(select.where_expression);
    if (select.prewhere_expression)
        ColumnAliasesVisitor(aliases_data).visit(select.prewhere_expression);
    if (select.having_expression)
        ColumnAliasesVisitor(aliases_data).visit(select.having_expression);

    /// JOIN sections
    for (auto & child : select.tables->children)
    {
        auto table = typeid_cast<ASTTablesInSelectQueryElement *>(child.get());
        if (table->table_join)
        {
            auto * join = typeid_cast<ASTTableJoin *>(table->table_join.get());
            ColumnAliasesVisitor(aliases_data).visit(join->on_expression);
        }
    }

    aliases_data.replaceIdentifiersWithAliases();

    auto rev_aliases = std::make_shared<RevertedAliases>();
    rev_aliases->swap(aliases_data.rev_aliases);

    auto & src_tables = select.tables->children;
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
    RewriteVisitor(visitor_data).visit(select.tables);

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
    auto left = typeid_cast<const ASTTablesInSelectQueryElement *>(ast_left.get());
    auto right = typeid_cast<const ASTTablesInSelectQueryElement *>(ast_right.get());
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
