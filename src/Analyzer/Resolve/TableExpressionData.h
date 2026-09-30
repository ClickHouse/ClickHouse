#pragma once

#include <IO/Operators.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/Identifier.h>
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
    /// Set of regular (non-subcolumn) column names. Lazily populated by
    /// `ensureColumnMembershipSetsArePopulated()`. Used for membership checks that don't need
    /// a `ColumnNode` (e.g. `hasFullIdentifierName`). For wide tables (~100 columns) building
    /// this set during `initializeTableExpressionData` is itself non-trivial; trivial queries
    /// like `SELECT count() FROM t` never consult it.
    mutable std::unordered_set<std::string, StringTransparentHash, std::equal_to<>> column_names;
    std::unordered_set<std::string> subcolumn_names; /// Subset columns that are subcolumns of other columns
    /// Set of `Identifier(name).at(0)` for every column. Used to test whether the first part
    /// of a compound identifier could refer to a column in this table. Populated together
    /// with `column_names` by `ensureColumnMembershipSetsArePopulated()`.
    mutable std::unordered_set<std::string, StringTransparentHash, std::equal_to<>> column_identifier_first_parts;
    mutable bool column_membership_sets_populated = false;

    void ensureColumnMembershipSetsArePopulated() const;

    /// Returns the `name -> ColumnNode` map, building it on first call. Many queries
    /// (e.g. `SELECT count() FROM t`) never resolve any column identifier from a table and
    /// therefore never need this map; building 100+ `ColumnNode`s up front for such queries
    /// is the dominant cost of `initializeTableExpressionData` for wide tables.
    const ColumnNameToColumnNodeMap & getColumnNodeMap() const;

    /// Install a populator that materialises the map (and resolves any ALIAS column
    /// expressions) on first `getColumnNodeMap()`. The populator receives the (initially
    /// empty) map by reference; emplacing it before invocation breaks recursion when ALIAS
    /// resolution triggers identifier lookups that call `getColumnNodeMap()` again.
    void setColumnNodeMapPopulator(std::function<void(ColumnNameToColumnNodeMap &)> populator);

    /// Eagerly emplace an empty map and return a mutable reference for callers that fill
    /// it inline (used for subquery / union projection lists, which are typically small).
    ColumnNameToColumnNodeMap & emplaceColumnNodeMap() const;

    bool hasFullIdentifierName(IdentifierView identifier_view) const
    {
        ensureColumnMembershipSetsArePopulated();
        return column_names.contains(identifier_view.getFullName());
    }

    bool canBindIdentifier(IdentifierView identifier_view) const
    {
        ensureColumnMembershipSetsArePopulated();
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
        const auto & node_map = getColumnNodeMap();
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
        ensureColumnMembershipSetsArePopulated();
        for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(full_identifier_name))
        {
            /// Use `column_names` as a fast existence check before forcing the
            /// `column_name_to_column_node` map to be built.
            if (!column_names.contains(column_name))
                continue;
            const auto & node_map = getColumnNodeMap();
            auto it = node_map.find(column_name);
            if (it != node_map.end())
            {
                if (auto subcolumn_type = it->second->getResultType()->tryGetSubcolumnType(subcolumn_name))
                    return SubcolumnInfo{it->second, subcolumn_name, subcolumn_type};
            }
        }

        return std::nullopt;
    }

private:
    mutable std::optional<ColumnNameToColumnNodeMap> column_name_to_column_node;
    std::function<void(ColumnNameToColumnNodeMap &)> populate_column_node_map;
};

}
