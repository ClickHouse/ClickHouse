#include <Common/typeid_cast.h>

#include <Interpreters/IdentifierSemantic.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AMBIGUOUS_COLUMN_NAME;
}

namespace
{

const DatabaseAndTableWithAlias & extractTable(const DatabaseAndTableWithAlias & table)
{
    return table;
}

const DatabaseAndTableWithAlias & extractTable(const TableWithColumnNames & table)
{
    return table.first;
}

template <typename T>
IdentifierSemantic::ColumnMatch tryChooseTable(const ASTIdentifier & identifier, const std::vector<T> & tables,
                                               size_t & best_table_pos, bool allow_ambiguous)
{
    using ColumnMatch = IdentifierSemantic::ColumnMatch;

    best_table_pos = 0;
    auto best_match = ColumnMatch::NoMatch;
    size_t same_match = 0;

    for (size_t i = 0; i < tables.size(); ++i)
    {
        auto match = IdentifierSemantic::canReferColumnToTable(identifier, extractTable(tables[i]));
        if (value(match))
        {
            if (value(match) > value(best_match))
            {
                best_match = match;
                best_table_pos = i;
                same_match = 0;
            }
            else if (match == best_match)
                ++same_match;
        }
    }

    if (value(best_match) && same_match)
    {
        if (!allow_ambiguous)
            throw Exception("Ambiguous column '" + identifier.name + "'", ErrorCodes::AMBIGUOUS_COLUMN_NAME);
        return ColumnMatch::Ambiguous;
    }
    return best_match;
}

}

std::optional<String> IdentifierSemantic::getColumnName(const ASTIdentifier & node)
{
    if (!node.semantic->special)
        return node.name;
    return {};
}

std::optional<String> IdentifierSemantic::getColumnName(const ASTPtr & ast)
{
    if (ast)
        if (const auto * id = ast->as<ASTIdentifier>())
            if (!id->semantic->special)
                return id->name;
    return {};
}

std::optional<String> IdentifierSemantic::getTableName(const ASTIdentifier & node)
{
    if (node.semantic->special)
        return node.name;
    return {};
}

std::optional<String> IdentifierSemantic::getTableName(const ASTPtr & ast)
{
    if (ast)
        if (const auto * id = ast->as<ASTIdentifier>())
            if (id->semantic->special)
                return id->name;
    return {};
}

void IdentifierSemantic::setNeedLongName(ASTIdentifier & identifier, bool value)
{
    identifier.semantic->need_long_name = value;
}

bool IdentifierSemantic::canBeAlias(const ASTIdentifier & identifier)
{
    return identifier.semantic->can_be_alias;
}

void IdentifierSemantic::setMembership(ASTIdentifier & identifier, size_t table_no)
{
    identifier.semantic->membership = table_no;
}

size_t IdentifierSemantic::getMembership(const ASTIdentifier & identifier)
{
    return identifier.semantic->membership;
}

bool IdentifierSemantic::chooseTable(const ASTIdentifier & identifier, const std::vector<DatabaseAndTableWithAlias> & tables,
                                     size_t & best_table_pos, bool ambiguous)
{
    return value(tryChooseTable<DatabaseAndTableWithAlias>(identifier, tables, best_table_pos, ambiguous));
}

bool IdentifierSemantic::chooseTable(const ASTIdentifier & identifier, const std::vector<TableWithColumnNames> & tables,
                                     size_t & best_table_pos, bool ambiguous)
{
    return value(tryChooseTable<TableWithColumnNames>(identifier, tables, best_table_pos, ambiguous));
}

std::pair<String, String> IdentifierSemantic::extractDatabaseAndTable(const ASTIdentifier & identifier)
{
    if (identifier.name_parts.size() > 2)
        throw Exception("Logical error: more than two components in table expression", ErrorCodes::LOGICAL_ERROR);

    if (identifier.name_parts.size() == 2)
        return { identifier.name_parts[0], identifier.name_parts[1] };
    return { "", identifier.name };
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
        return ColumnMatch::DatabaseAndTable;

    /// alias.column
    if (doesIdentifierBelongTo(identifier, db_and_table.alias))
        return ColumnMatch::TableAlias;

    /// table.column
    if (doesIdentifierBelongTo(identifier, db_and_table.table))
        return ColumnMatch::TableName;

    return ColumnMatch::NoMatch;
}

/// Checks that ast is ASTIdentifier and remove num_qualifiers_to_strip components from left.
/// Example: 'database.table.name' -> (num_qualifiers_to_strip = 2) -> 'name'.
void IdentifierSemantic::setColumnShortName(ASTIdentifier & identifier, size_t to_strip)
{
    if (!to_strip)
        return;

    std::vector<String> stripped(identifier.name_parts.begin() + to_strip, identifier.name_parts.end());

    DB::String new_name;
    for (const auto & part : stripped)
    {
        if (!new_name.empty())
            new_name += '.';
        new_name += part;
    }
    identifier.name.swap(new_name);
}

void IdentifierSemantic::setColumnNormalName(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    auto match = IdentifierSemantic::canReferColumnToTable(identifier, db_and_table);
    size_t to_strip = 0;
    switch (match)
    {
        case ColumnMatch::TableName:
        case ColumnMatch::TableAlias:
            to_strip = 1;
            break;
        case ColumnMatch::DatabaseAndTable:
            to_strip = 2;
            break;
        default:
            break;
    }

    setColumnShortName(identifier, to_strip);
    if (value(match))
        identifier.semantic->can_be_alias = false;

    if (identifier.semantic->need_long_name)
        setColumnLongName(identifier, db_and_table);
}

void IdentifierSemantic::setColumnLongName(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    String prefix = db_and_table.getQualifiedNamePrefix();
    if (!prefix.empty())
    {
        String short_name = identifier.shortName();
        identifier.name = prefix + short_name;
        prefix.resize(prefix.size() - 1); /// crop dot
        identifier.name_parts = {prefix, short_name};
    }
}

String IdentifierSemantic::columnNormalName(const ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    ASTPtr copy = identifier.clone();
    setColumnNormalName(copy->as<ASTIdentifier &>(), db_and_table);
    return copy->getAliasOrColumnName();
}

String IdentifierSemantic::columnLongName(const ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    return db_and_table.getQualifiedNamePrefix() + identifier.shortName();
}

}
