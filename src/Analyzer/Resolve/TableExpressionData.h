#pragma once

#include <IO/Operators.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/Identifier.h>
#include <DataTypes/NestedUtils.h>
#include <Common/Exception.h>
#include <Poco/String.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AMBIGUOUS_IDENTIFIER;
}

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
/// Maps lowercase column name to the list of original column names that match
using LowercaseToOriginalNamesMap = std::unordered_map<std::string, std::vector<std::string>>;

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

    /// Lowercase column name -> original-case names. Built once by `enableStandardMode()` from
    /// `column_name_to_column_node`; do not mutate after that. Multiple entries per key are allowed
    /// and reported as ambiguity at lookup time
    LowercaseToOriginalNamesMap lowercase_column_name_to_original_names;

    bool standard_mode = false;

    bool hasFullIdentifierName(IdentifierView identifier_view, bool use_case_insensitive = false) const
    {
        ensureColumnMembershipSetsArePopulated();
        const auto & full_name = identifier_view.getFullName();
        if (column_names.contains(full_name))
            return true;
        if (use_case_insensitive)
            return hasColumnCaseInsensitive(full_name);
        return false;
    }

    bool canBindIdentifier(IdentifierView identifier_view, bool use_case_insensitive = false) const
    {
        ensureColumnMembershipSetsArePopulated();
        const auto & first_part = identifier_view.at(0);
        if (column_identifier_first_parts.contains(first_part) || column_names.contains(first_part))
            return true;
        if (use_case_insensitive)
        {
            String lower_first = Poco::toLower(String(first_part));
            if (column_identifier_first_parts.contains(lower_first))
                return true;
        }
        return tryGetSubcolumnInfo(identifier_view.getFullName(), use_case_insensitive).has_value();
    }

    /// Case-insensitive lookup of a column. Returns end() of the on-demand map when not found.
    /// Throws AMBIGUOUS_IDENTIFIER when multiple columns differ only by case.
    ColumnNameToColumnNodeMap::const_iterator findColumnCaseInsensitive(
        std::string_view identifier_name,
        const String & scope_description) const
    {
        const auto & node_map = getColumnNodeMap();
        auto it = lowercase_column_name_to_original_names.find(Poco::toLower(String(identifier_name)));
        if (it == lowercase_column_name_to_original_names.end())
            return node_map.end();

        if (it->second.size() > 1)
            throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                "Identifier '{}' is ambiguous: matches multiple columns with different cases: {}. In scope {}",
                identifier_name, fmt::join(it->second, ", "), scope_description);
        return node_map.find(it->second.front());
    }

    bool hasColumnCaseInsensitive(std::string_view identifier_name) const
    {
        String lower_name = Poco::toLower(String(identifier_name));
        return lowercase_column_name_to_original_names.contains(lower_name);
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

    std::optional<SubcolumnInfo> tryGetSubcolumnInfo(
        std::string_view full_identifier_name,
        bool use_case_insensitive,
        const String & scope_description) const
    {
        ensureColumnMembershipSetsArePopulated();
        for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(full_identifier_name))
        {
            /// Use `column_names` as a fast existence check before forcing the column-node map to be built.
            /// In case-insensitive mode also consult the lowercase index so e.g. `Data.field` matches a column `data`.
            String resolved_column_name;
            if (column_names.contains(column_name))
            {
                resolved_column_name = String(column_name);
            }
            else if (use_case_insensitive)
            {
                auto lower_it = lowercase_column_name_to_original_names.find(Poco::toLower(String(column_name)));
                if (lower_it == lowercase_column_name_to_original_names.end() || lower_it->second.empty())
                    continue;
                if (lower_it->second.size() > 1)
                    throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                        "Identifier '{}' is ambiguous: column '{}' matches multiple columns with different cases: {}. In scope {}",
                        full_identifier_name, column_name, fmt::join(lower_it->second, ", "), scope_description);
                resolved_column_name = lower_it->second.front();
            }
            else
            {
                continue;
            }

            const auto & node_map = getColumnNodeMap();
            auto it = node_map.find(resolved_column_name);
            if (it != node_map.end())
            {
                if (auto subcolumn_type = it->second->getResultType()->tryGetSubcolumnType(subcolumn_name))
                    return SubcolumnInfo{it->second, subcolumn_name, subcolumn_type};
            }
        }

        return std::nullopt;
    }

    std::optional<SubcolumnInfo> tryGetSubcolumnInfo(std::string_view full_identifier_name, bool use_case_insensitive = false) const
    {
        return tryGetSubcolumnInfo(full_identifier_name, use_case_insensitive, "");
    }

    /// Build lowercase-to-original mappings for case-insensitive identifier resolution from the
    /// column-name source-of-truth `column_names_and_types`, so the lazy column-node map does not
    /// have to be materialised here.
    /// `case_sensitive_column_names` lists column names that must stay case-sensitive — they are
    /// skipped from the lowercase index. Used for projection-override aliases that were defined
    /// as double-quoted (e.g. `FROM (...) AS t("MyCol")`).
    void enableStandardMode(const std::unordered_set<std::string> & case_sensitive_column_names = {})
    {
        standard_mode = true;
        lowercase_column_name_to_original_names.clear();

        for (const auto & [column_name, _] : column_names_and_types)
        {
            if (case_sensitive_column_names.contains(column_name))
                continue;
            String lower_name = Poco::toLower(column_name);
            lowercase_column_name_to_original_names[lower_name].push_back(column_name);
        }

        /// Add lowercase entries to column_identifier_first_parts for binding check
        ensureColumnMembershipSetsArePopulated();
        std::vector<std::string> first_parts_to_add;
        for (const auto & first_part : column_identifier_first_parts)
        {
            if (case_sensitive_column_names.contains(first_part))
                continue;
            String lower_part = Poco::toLower(first_part);
            if (lower_part != first_part)
                first_parts_to_add.push_back(lower_part);
        }
        for (const auto & part : first_parts_to_add)
            column_identifier_first_parts.insert(part);
    }

private:
    mutable std::optional<ColumnNameToColumnNodeMap> column_name_to_column_node;
    std::function<void(ColumnNameToColumnNodeMap &)> populate_column_node_map;
};

}
