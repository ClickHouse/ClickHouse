#include <Analyzer/Resolve/TableExpressionData.h>

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
    /// Emplace the (initially empty) map before invoking the populator, so that a re-entrant
    /// call finds the map present.
    auto & node_map = column_name_to_column_node.emplace();
    ensureColumnMembershipSetsArePopulated();
    if (populate_column_node_map)
    {
        for (auto & alias_column_name : populate_column_node_map(node_map))
            unresolved_alias_columns.insert(std::move(alias_column_name));
    }
    return node_map;
}

ColumnNodePtr AnalysisTableExpressionData::tryGetColumnNode(std::string_view column_name) const
{
    const auto & node_map = getColumnNodeMap();
    auto it = node_map.find(column_name);
    if (it == node_map.end())
        return nullptr;

    auto unresolved_it = unresolved_alias_columns.find(column_name);
    if (unresolved_it != unresolved_alias_columns.end())
    {
        /// Erase before resolving: resolution of the ALIAS expression can look up this column again
        /// (it is an error of cyclic aliases), and the lookup has to get the column without recursion.
        unresolved_alias_columns.erase(unresolved_it);
        /// `node_map` is not changed by the resolution (only the nodes in it), so `it` remains valid.
        resolve_alias_column(it->second);
    }

    return it->second;
}

void AnalysisTableExpressionData::setColumnNodeMapPopulator(ColumnNodeMapPopulator populator, AliasColumnResolver alias_column_resolver)
{
    populate_column_node_map = std::move(populator);
    resolve_alias_column = std::move(alias_column_resolver);
}

ColumnNameToColumnNodeMap & AnalysisTableExpressionData::emplaceColumnNodeMap() const
{
    return column_name_to_column_node.emplace();
}

}
