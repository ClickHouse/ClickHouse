#include <Interpreters/IdentifierSemantic.h>

#include <Common/typeid_cast.h>

#include <Core/Settings.h>

#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool asterisk_include_alias_columns;
    extern const SettingsBool asterisk_include_materialized_columns;
}

namespace ErrorCodes
{
    extern const int AMBIGUOUS_COLUMN_NAME;
}

namespace
{

template <typename T>
std::optional<size_t> tryChooseTable(const ASTIdentifier & identifier, const std::vector<T> & tables,
                                     bool allow_ambiguous, bool column_match [[maybe_unused]] = false)
{
    using ColumnMatch = IdentifierSemantic::ColumnMatch;

    size_t best_table_pos = 0;
    auto best_match = ColumnMatch::NoMatch;
    size_t same_match = 0;

    for (size_t i = 0; i < tables.size(); ++i)
    {
        auto match = IdentifierSemantic::canReferColumnToTable(identifier, tables[i]);

        if constexpr (std::is_same_v<T, TableWithColumnNamesAndTypes>)
        {
            if (column_match && match == ColumnMatch::NoMatch && identifier.isShort() && tables[i].hasColumn(identifier.shortName()))
                match = ColumnMatch::ColumnName;
        }

        if (match != ColumnMatch::NoMatch)
        {
            if (match > best_match)
            {
                best_match = match;
                best_table_pos = i;
                same_match = 0;
            }
            else if (match == best_match)
                ++same_match;
        }
    }

    if ((best_match != ColumnMatch::NoMatch) && same_match)
    {
        if (!allow_ambiguous)
            throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME, "Ambiguous column '{}'", identifier.name());
        best_match = ColumnMatch::Ambiguous;
        return {};
    }

    if (best_match != ColumnMatch::NoMatch)
        return best_table_pos;
    return {};
}

}

std::optional<String> IdentifierSemantic::getColumnName(const ASTIdentifier & node)
{
    if (!node.semantic->special)
        return node.name();
    return {};
}

std::optional<String> IdentifierSemantic::getColumnName(const ASTPtr & ast)
{
    if (ast)
        if (const auto * id = ast->as<ASTIdentifier>())
            if (!id->semantic->special)
                return id->name();
    return {};
}

std::optional<ASTIdentifier> IdentifierSemantic::uncover(const ASTIdentifier & identifier)
{
    if (identifier.semantic->covered)
    {
        std::vector<String> name_parts = identifier.name_parts;
        return ASTIdentifier(std::move(name_parts));
    }
    return {};
}

void IdentifierSemantic::coverName(ASTIdentifier & identifier, const String & alias)
{
    identifier.setShortName(alias);
    identifier.semantic->covered = true;
}

bool IdentifierSemantic::canBeAlias(const ASTIdentifier & identifier)
{
    return identifier.semantic->can_be_alias;
}

void IdentifierSemantic::setMembership(ASTIdentifier & identifier, size_t table_pos)
{
    identifier.semantic->membership = table_pos;
    identifier.semantic->can_be_alias = false;
}

std::optional<size_t> IdentifierSemantic::getMembership(const ASTIdentifier & identifier)
{
    return identifier.semantic->membership;
}

std::optional<size_t> IdentifierSemantic::chooseTable(const ASTIdentifier & identifier, const std::vector<DatabaseAndTableWithAlias> & tables,
                                                      bool ambiguous)
{
    return tryChooseTable<DatabaseAndTableWithAlias>(identifier, tables, ambiguous);
}

std::optional<size_t> IdentifierSemantic::chooseTable(const ASTIdentifier & identifier, const TablesWithColumns & tables, bool ambiguous)
{
    return tryChooseTable<TableWithColumnNamesAndTypes>(identifier, tables, ambiguous);
}

std::optional<size_t> IdentifierSemantic::chooseTableColumnMatch(const ASTIdentifier & identifier, const TablesWithColumns & tables,
                                                                 bool ambiguous)
{
    return tryChooseTable<TableWithColumnNamesAndTypes>(identifier, tables, ambiguous, true);
}

std::optional<String> IdentifierSemantic::extractNestedName(const ASTIdentifier & identifier, const String & table_name)
{
    if (identifier.name_parts.size() == 3 && table_name == identifier.name_parts[0])
        return identifier.name_parts[1] + '.' + identifier.name_parts[2];
    if (identifier.name_parts.size() == 2)
        return identifier.name_parts[0] + '.' + identifier.name_parts[1];
    return {};
}

String IdentifierSemantic::extractNestedName(const ASTIdentifier & identifier, const DatabaseAndTableWithAlias & table)
{
    auto match = IdentifierSemantic::canReferColumnToTable(identifier, table);
    size_t to_strip = 0;
    switch (match)
    {
        case IdentifierSemantic::ColumnMatch::TableName:
        case IdentifierSemantic::ColumnMatch::AliasedTableName:
        case IdentifierSemantic::ColumnMatch::TableAlias:
            to_strip = 1;
            break;
        case IdentifierSemantic::ColumnMatch::DBAndTable:
            to_strip = 2;
            break;
        default:
            break;
    }
    String res;
    for (size_t i = to_strip, sz = identifier.name_parts.size(); i < sz; ++i)
    {
        if (!res.empty())
            res += ".";
        res += identifier.name_parts[i];
    }
    return res;
}

bool IdentifierSemantic::doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & database, const String & table)
{
    size_t num_components = identifier.name_parts.size();
    if (num_components >= 3)
        return identifier.name_parts[0] == database &&
               identifier.name_parts[1] == table;
    return false;
}

bool IdentifierSemantic::doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & table)
{
    size_t num_components = identifier.name_parts.size();
    if (num_components >= 2)
        return identifier.name_parts[0] == table;
    return false;
}

IdentifierSemantic::ColumnMatch IdentifierSemantic::canReferColumnToTable(const ASTIdentifier & identifier,
                                                                          const DatabaseAndTableWithAlias & db_and_table)
{
    /// database.table.column
    if (doesIdentifierBelongTo(identifier, db_and_table.database, db_and_table.table))
        return ColumnMatch::DBAndTable;

    /// alias.column
    if (doesIdentifierBelongTo(identifier, db_and_table.alias))
        return ColumnMatch::TableAlias;

    /// table.column
    if (doesIdentifierBelongTo(identifier, db_and_table.table))
    {
        if (!db_and_table.alias.empty())
            return ColumnMatch::AliasedTableName;
        return ColumnMatch::TableName;
    }

    return ColumnMatch::NoMatch;
}

IdentifierSemantic::ColumnMatch IdentifierSemantic::canReferColumnToTable(const ASTIdentifier & identifier,
                                                                          const TableWithColumnNamesAndTypes & table_with_columns)
{
    return canReferColumnToTable(identifier, table_with_columns.table);
}

/// Strip qualifications from left side of column name.
/// Example: 'database.table.name' -> 'name'.
void IdentifierSemantic::setColumnShortName(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    auto match = IdentifierSemantic::canReferColumnToTable(identifier, db_and_table);
    size_t to_strip = 0;
    switch (match)
    {
        case ColumnMatch::TableName:
        case ColumnMatch::AliasedTableName:
        case ColumnMatch::TableAlias:
            to_strip = 1;
            break;
        case ColumnMatch::DBAndTable:
            to_strip = 2;
            break;
        default:
            break;
    }

    if (!to_strip)
        return;

    identifier.name_parts = std::vector<String>(identifier.name_parts.begin() + to_strip, identifier.name_parts.end());
    identifier.resetFullName();
}

void IdentifierSemantic::setColumnLongName(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    String prefix = db_and_table.getQualifiedNamePrefix();
    if (!prefix.empty())
    {
        prefix.resize(prefix.size() - 1); /// crop dot
        identifier.name_parts = {prefix, identifier.shortName()};
        identifier.resetFullName();
        identifier.semantic->table = prefix;
        identifier.semantic->legacy_compound = true;
    }
}

std::optional<size_t> IdentifierSemantic::getIdentMembership(const ASTIdentifier & ident, const std::vector<TableWithColumnNamesAndTypes> & tables)
{
    std::optional<size_t> table_pos = IdentifierSemantic::getMembership(ident);
    if (table_pos)
        return table_pos;
    return IdentifierSemantic::chooseTableColumnMatch(ident, tables, true);
}

std::optional<size_t>
IdentifierSemantic::getIdentsMembership(ASTPtr ast, const std::vector<TableWithColumnNamesAndTypes> & tables, const Aliases & aliases)
{
    auto idents = IdentifiersCollector::collect(ast);

    std::optional<size_t> result;
    for (const auto * ident : idents)
    {
        /// short name clashes with alias, ambiguous
        if (ident->isShort() && aliases.contains(ident->shortName()))
            return {};
        const auto pos = getIdentMembership(*ident, tables);
        if (!pos)
            return {};
        /// identifiers from different tables
        if (result && *pos != *result)
            return {};
        result = pos;
    }
    return result;
}

IdentifiersCollector::ASTIdentifiers IdentifiersCollector::collect(const ASTPtr & node)
{
    IdentifiersCollector::Data ident_data;
    ConstInDepthNodeVisitor<IdentifiersCollector, true> ident_visitor(ident_data);
    ident_visitor.visit(node);
    return ident_data.idents;
}

bool IdentifiersCollector::needChildVisit(const ASTPtr &, const ASTPtr &)
{
    return true;
}

void IdentifiersCollector::visit(const ASTPtr & node, IdentifiersCollector::Data & data)
{
    if (const auto * ident = node->as<ASTIdentifier>())
        data.idents.push_back(ident);
}


IdentifierMembershipCollector::IdentifierMembershipCollector(const ASTSelectQuery & select, ContextPtr context)
{
    if (ASTPtr with = select.with())
        QueryAliasesNoSubqueriesVisitor(aliases).visit(with);
    QueryAliasesNoSubqueriesVisitor(aliases).visit(select.select());

    const auto & settings = context->getSettingsRef();
    tables = getDatabaseAndTablesWithColumns(
        getTableExpressions(select), context, settings[Setting::asterisk_include_alias_columns], settings[Setting::asterisk_include_materialized_columns]);
}

std::optional<size_t> IdentifierMembershipCollector::getIdentsMembership(ASTPtr ast) const
{
    return IdentifierSemantic::getIdentsMembership(ast, tables, aliases);
}

void splitConjunctionsAst(const ASTPtr & node, ASTs & result)
{
    if (!node)
        return;

    result.emplace_back(node);

    for (size_t idx = 0; idx < result.size();)
    {
        ASTPtr expression = result.at(idx);

        if (const auto * function = expression->as<ASTFunction>(); function && function->name == "and")
        {
            result.erase(result.begin() + idx);

            for (auto & child : function->arguments->children)
                result.emplace_back(child);

            continue;
        }
        ++idx;
    }
}

ASTs splitConjunctionsAst(const ASTPtr & node)
{
    ASTs result;
    splitConjunctionsAst(node, result);
    return result;
}

}
