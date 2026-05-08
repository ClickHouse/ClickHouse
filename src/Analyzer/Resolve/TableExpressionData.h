#pragma once

#include <IO/Operators.h>
#include <Analyzer/ColumnNode.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

struct StringTransparentHash
{
    using is_transparent = void;
    using hash = std::hash<std::string_view>;

    [[maybe_unused]] size_t operator()(const char * data) const
    {
        return hash()(data);
    }

    size_t operator()(std::string_view data) const
    {
        return hash()(data);
    }

    size_t operator()(const std::string & data) const
    {
        return hash()(data);
    }
};

using ColumnNameToColumnNodeMap = std::unordered_map<std::string, ColumnNodePtr, StringTransparentHash, std::equal_to<>>;

struct AnalysisTableExpressionData
{
    std::string table_expression_name;
    std::string table_expression_description;
    std::string database_name;
    std::string table_name;
    bool should_qualify_columns = true;
    bool supports_subcolumns = false;
    NamesAndTypes column_names_and_types;
    /// Set of regular (non-subcolumn) column names. Eagerly populated; used for membership
    /// checks that don't need a `ColumnNode` (e.g. `hasFullIdentifierName`). Built once when
    /// `column_names_and_types` is finalised.
    std::unordered_set<std::string, StringTransparentHash, std::equal_to<>> column_names;
    /// `name -> ColumnNode`. Lazily populated by `ensureColumnNodeMapIsPopulated()`. Many
    /// queries (e.g. `SELECT count() FROM t`) never resolve any column identifier from a
    /// table and therefore never need this map; building 100+ `ColumnNode`s up front for
    /// such queries is the dominant cost of `initializeTableExpressionData` for wide tables.
    /// `nullopt` means "not populated yet" — `has_value()` doubles as the populated flag.
    mutable std::optional<ColumnNameToColumnNodeMap> column_name_to_column_node;
    /// Set by `initializeTableExpressionData`. Invoked at most once by
    /// `ensureColumnNodeMapIsPopulated()` to materialise `column_name_to_column_node` (and
    /// resolve ALIAS column expressions if any). Kept empty when there is no source table
    /// (e.g. subqueries) and the map is built eagerly.
    mutable std::function<void()> populate_column_node_map;
    std::unordered_set<std::string> subcolumn_names; /// Subset columns that are subcolumns of other columns
    std::unordered_set<std::string, StringTransparentHash, std::equal_to<>> column_identifier_first_parts;

    void ensureColumnNodeMapIsPopulated() const
    {
        if (column_name_to_column_node.has_value())
            return;
        /// Emplace the (initially empty) map before calling the populator. The populator
        /// first inserts every regular column (and ALIAS placeholders) into the map, then
        /// resolves ALIAS expressions; that resolution can recursively trigger identifier
        /// lookups that call this method again. Emplacing up front breaks the recursion:
        /// re-entrants find the map present and see the placeholders the populator has
        /// already inserted.
        column_name_to_column_node.emplace();
        if (populate_column_node_map)
            populate_column_node_map();
    }

    bool hasFullIdentifierName(IdentifierView identifier_view) const
    {
        /// Use the always-populated `column_names` set instead of the lazily-populated
        /// `column_name_to_column_node`.
        return column_names.contains(identifier_view.getFullName());
    }

    bool canBindIdentifier(IdentifierView identifier_view) const
    {
        return column_identifier_first_parts.contains(identifier_view.at(0)) || column_names.contains(identifier_view.at(0))
            || tryGetSubcolumnInfo(identifier_view.getFullName());
    }

    [[maybe_unused]] void dump(WriteBuffer & buffer) const
    {
        buffer << " Table expression name '" << table_expression_name << "'";

        if (!table_expression_description.empty())
            buffer << ", description '" << table_expression_description << "'\n";

        if (!database_name.empty())
            buffer << "   database name '" << database_name << "'\n";

        if (!table_name.empty())
            buffer << "   table name '" << table_name << "'\n";

        buffer << "   Should qualify columns " << should_qualify_columns << "\n";
        ensureColumnNodeMapIsPopulated();
        const auto & node_map = *column_name_to_column_node;
        buffer << "   Columns size " << node_map.size() << "\n";
        static constexpr size_t max_columns_to_dump = 10;
        size_t columns_dumped = 0;
        for (const auto & [column_name, column_node] : node_map)
        {
            if (columns_dumped >= max_columns_to_dump)
            {
                buffer << "    ... and " << (node_map.size() - max_columns_to_dump) << " more columns\n";
                break;
            }
            buffer << "    { " << column_name << " : " << column_node->dumpTree() << " }\n";
            ++columns_dumped;
        }
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }

    struct SubcolumnInfo
    {
        ColumnNodePtr column_node;
        std::string_view subcolumn_name;
        DataTypePtr subcolumn_type;
    };

    std::optional<SubcolumnInfo> tryGetSubcolumnInfo(std::string_view full_identifier_name) const
    {
        for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(full_identifier_name))
        {
            /// `column_names` is the always-populated set; use it as a fast existence check
            /// before forcing the lazy `column_name_to_column_node` map to be built.
            if (!column_names.contains(column_name))
                continue;
            ensureColumnNodeMapIsPopulated();
            auto it = column_name_to_column_node->find(column_name);
            if (it != column_name_to_column_node->end())
            {
                if (auto subcolumn_type = it->second->getResultType()->tryGetSubcolumnType(subcolumn_name))
                    return SubcolumnInfo{it->second, subcolumn_name, subcolumn_type};
            }
        }

        return std::nullopt;
    }
};

}
