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

}
