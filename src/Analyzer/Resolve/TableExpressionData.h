#pragma once

#include <IO/Operators.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/Identifier.h>
#include <DataTypes/NestedUtils.h>
#include <Common/Exception.h>
#include <Poco/String.h>

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

struct AnalysisTableExpressionData
{
    std::string table_expression_name;
    std::string table_expression_description;
    std::string database_name;
    std::string table_name;
    /// True iff `table_name` originated from a CTE defined with a double-quoted name
    /// (`WITH "MyCte" AS ...`). Qualifier matching in `standard` mode keeps such names exact so
    /// an unquoted `mycte.x` does not bind to a CTE defined as `"MyCte"`.
    bool table_name_is_double_quoted = false;
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

    bool standard_mode = false;

    bool hasFullIdentifierName(IdentifierView identifier_view) const
    {
        ensureColumnMembershipSetsArePopulated();
        return column_names.contains(identifier_view.getFullName());
    }

    bool canBindIdentifier(IdentifierView identifier_view, bool fold_subcolumn_suffix = false) const
    {
        ensureColumnMembershipSetsArePopulated();
        const auto & first_part = identifier_view.at(0);
        if (column_identifier_first_parts.contains(first_part) || column_names.contains(first_part))
            return true;
        return tryGetSubcolumnInfo(identifier_view.getFullName(), fold_subcolumn_suffix).has_value();
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
        /// Owning string so a `standard`-mode case-insensitive match can return its canonical
        /// suffix name without depending on a temporary returned by `IDataType::getSubcolumnNames`.
        String subcolumn_name;
        DataTypePtr subcolumn_type;
    };

    template <typename ScopeDescriptionProvider>
    std::optional<SubcolumnInfo> tryGetSubcolumnInfo(
        std::string_view full_identifier_name,
        ScopeDescriptionProvider && get_scope_description,
        bool fold_suffix) const
    {
        ensureColumnMembershipSetsArePopulated();
        for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(full_identifier_name))
        {
            /// Use `column_names` as a fast existence check before forcing the column-node map to be built.
            /// The base column is matched exactly; a case-folded base is respelled by the
            /// `tryResolveIdentifierByCaseFoldRespell` fallback and retried through this same path.
            if (!column_names.contains(column_name))
                continue;

            const auto & node_map = getColumnNodeMap();
            auto it = node_map.find(column_name);
            if (it == node_map.end())
                continue;

            /// Exact-case match first; backwards-compatible and works for storages that expose the
            /// subcolumn under its canonical name.
            if (auto subcolumn_type = it->second->getResultType()->tryGetSubcolumnType(subcolumn_name))
                return SubcolumnInfo{it->second, String(subcolumn_name), subcolumn_type};

            /// In standard mode also try a case-insensitive subcolumn match. Tuple/Variant/Map
            /// subcolumns are resolved via exact string lookup inside the type itself, so we have to
            /// canonicalize the suffix here. Multiple case-only-different subcolumn names are an
            /// ambiguity at this level. A double-quoted suffix like `data."name"` stays
            /// case-sensitive: `fold_suffix` is false for it.
            if (fold_suffix)
            {
                auto data_type = it->second->getResultType();
                String lower_suffix = Poco::toLower(String(subcolumn_name));
                String matched_subcolumn;
                for (const auto & candidate : data_type->getSubcolumnNames())
                {
                    if (Poco::toLower(candidate) != lower_suffix)
                        continue;
                    if (!matched_subcolumn.empty() && matched_subcolumn != candidate)
                        throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                            "Identifier '{}' is ambiguous: subcolumn '{}' matches multiple subcolumns with different cases: '{}', '{}'. In scope {}",
                            full_identifier_name, subcolumn_name, matched_subcolumn, candidate, get_scope_description());
                    matched_subcolumn = candidate;
                }
                if (!matched_subcolumn.empty())
                {
                    if (auto subcolumn_type = data_type->tryGetSubcolumnType(matched_subcolumn))
                        return SubcolumnInfo{it->second, std::move(matched_subcolumn), subcolumn_type};
                }
            }
        }

        return std::nullopt;
    }

    std::optional<SubcolumnInfo> tryGetSubcolumnInfo(std::string_view full_identifier_name, bool fold_suffix = false) const
    {
        return tryGetSubcolumnInfo(full_identifier_name, [] { return String{}; }, fold_suffix);
    }

    /// Projection-column names pinned case-sensitive (double-quoted aliases / overrides,
    /// e.g. `FROM (...) AS t("MyCol")`). Consulted by the case-fold respell fallback;
    /// populated by `enableStandardMode`.
    std::unordered_set<std::string> case_sensitive_column_names;

    void enableStandardMode(const std::unordered_set<std::string> & case_sensitive_column_names_ = {})
    {
        standard_mode = true;
        case_sensitive_column_names = case_sensitive_column_names_;
    }

private:
    mutable std::optional<ColumnNameToColumnNodeMap> column_name_to_column_node;
    std::function<void(ColumnNameToColumnNodeMap &)> populate_column_node_map;
};

}
