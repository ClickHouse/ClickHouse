#include <Analyzer/Resolve/TableExpressionData.h>
#include <Interpreters/Context.h>

namespace DB
{

void AnalysisTableExpressionData::ensureColumnMembershipSetsArePopulated() const
{
    if (column_membership_sets_populated)
        return;
    column_names.reserve(column_names_and_types.size());
    column_identifier_first_parts.reserve(column_names_and_types.size());
    for (const auto & column_name_and_type : column_names_and_types)
    {
        column_names.insert(column_name_and_type.name);
        Identifier column_name_identifier(column_name_and_type.name);
        column_identifier_first_parts.insert(column_name_identifier.at(0));
    }
    column_membership_sets_populated = true;
}

const ColumnNameToColumnNodeMap & AnalysisTableExpressionData::getColumnNodeMap() const
{
    if (column_name_to_column_node.has_value())
        return *column_name_to_column_node;
    /// Emplace the (initially empty) map before invoking the populator. The populator
    /// first inserts every regular column (and ALIAS placeholders) into the map, then
    /// resolves ALIAS expressions; that resolution can recursively trigger identifier
    /// lookups that call this method again. Emplacing up front breaks the recursion:
    /// re-entrants find the map present and see the placeholders the populator has
    /// already inserted.
    auto & node_map = column_name_to_column_node.emplace();
    ensureColumnMembershipSetsArePopulated();
    if (populate_column_node_map)
        populate_column_node_map(node_map);
    return node_map;
}

void AnalysisTableExpressionData::setColumnNodeMapPopulator(std::function<void(ColumnNameToColumnNodeMap &)> populator)
{
    populate_column_node_map = std::move(populator);
}

ColumnNameToColumnNodeMap & AnalysisTableExpressionData::emplaceColumnNodeMap() const
{
    return column_name_to_column_node.emplace();
}

namespace
{

/// Matches the dotted `name` against the parts of `identifier` starting from `offset`. A part may cover several
/// components of the name when it was quoted (`"ns.t"`), and a component of the name is never matched partially.
/// Returns the index of the part after the last matched one, 0 if the name does not match.
size_t matchDottedName(const Identifier & identifier, size_t offset, std::string_view name)
{
    if (name.empty())
        return 0;

    size_t name_pos = 0;
    for (size_t i = offset; i < identifier.getPartsSize(); ++i)
    {
        std::string_view rest = std::string_view(name).substr(name_pos);
        const auto & part = identifier[i];
        if (!rest.starts_with(part))
            return 0;

        name_pos += part.size();
        if (name_pos == name.size())
            return i + 1;
        if (name[name_pos] != '.')
            return 0;
        ++name_pos;
    }

    return 0;
}

}

size_t AnalysisTableExpressionData::matchTableName(const Identifier & identifier, const ContextPtr & context) const
{
    if (table_name.empty())
        return 0;

    if (size_t matched = matchDottedName(identifier, 0, table_name))
        return matched;

    /// Relative to the current database: inside `USE db.ns`, the table `db`.`ns.t` is `t`, and the table `t` of the
    /// database `db.ns.x` is `x.t`. Only a name with more components than the current database can match this way.
    if (database_name.empty() || !(database_name.contains('.') || table_name.contains('.')))
        return 0;

    String current_database = context->getCurrentDatabase();
    if (current_database == database_name)
        return 0;

    String full_name = database_name + '.' + table_name;
    if (full_name.size() > current_database.size() + 1 && full_name.starts_with(current_database) && full_name[current_database.size()] == '.')
        return matchDottedName(identifier, 0, std::string_view(full_name).substr(current_database.size() + 1));

    return 0;
}

size_t AnalysisTableExpressionData::matchDatabaseAndTableName(const Identifier & identifier) const
{
    if (database_name.empty() || table_name.empty())
        return 0;

    size_t matched = matchDottedName(identifier, 0, database_name);
    if (!matched)
        return 0;
    return matchDottedName(identifier, matched, table_name);
}

}
