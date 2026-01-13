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
    ColumnNameToColumnNodeMap column_name_to_column_node;
    std::unordered_set<std::string> subcolumn_names; /// Subset columns that are subcolumns of other columns
    std::unordered_set<std::string, StringTransparentHash, std::equal_to<>> column_identifier_first_parts;

    LowercaseToOriginalNamesMap lowercase_column_name_to_original_names;

    bool use_standard_mode = false;

    bool hasFullIdentifierName(IdentifierView identifier_view, bool use_case_insensitive = false) const
    {
        const auto & full_name = identifier_view.getFullName();
        if (column_name_to_column_node.contains(full_name))
            return true;
        if (use_case_insensitive && use_standard_mode)
            return hasColumnCaseInsensitive(full_name);
        return false;
    }

    bool canBindIdentifier(IdentifierView identifier_view, bool use_case_insensitive = false) const
    {
        const auto & first_part = identifier_view.at(0);
        if (column_identifier_first_parts.contains(first_part) || column_name_to_column_node.contains(first_part))
            return true;
        if (use_case_insensitive && use_standard_mode)
        {
            String lower_first = Poco::toLower(String(first_part));
            if (column_identifier_first_parts.contains(lower_first))
                return true;
        }
        return tryGetSubcolumnInfo(identifier_view.getFullName(), use_case_insensitive).has_value();
    }

    ColumnNameToColumnNodeMap::const_iterator findColumnCaseInsensitive(
        std::string_view identifier_name,
        const String & scope_description) const
    {
        auto it = lowercase_column_name_to_original_names.find(Poco::toLower(String(identifier_name)));
        if (it == lowercase_column_name_to_original_names.end())
            return column_name_to_column_node.end();

        if (it->second.size() > 1)
            throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                "Identifier '{}' is ambiguous: matches multiple columns with different cases: {}. In scope {}",
                identifier_name, fmt::join(it->second, ", "), scope_description);
        return column_name_to_column_node.find(it->second.front());
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
        buffer << "   Columns size " << column_name_to_column_node.size() << "\n";
        for (const auto & [column_name, column_node] : column_name_to_column_node)
            buffer << "    { " << column_name << " : " << column_node->dumpTree() << " }\n";
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

    std::optional<SubcolumnInfo> tryGetSubcolumnInfo(std::string_view full_identifier_name, bool use_case_insensitive = false) const
    {
        for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(full_identifier_name))
        {
            auto it = column_name_to_column_node.find(column_name);
            if (it == column_name_to_column_node.end() && use_case_insensitive && use_standard_mode)
            {
                /// try case-insensitive lookup
                String lower_column_name = Poco::toLower(String(column_name));
                auto lower_it = lowercase_column_name_to_original_names.find(lower_column_name);
                if (lower_it != lowercase_column_name_to_original_names.end() && !lower_it->second.empty())
                    it = column_name_to_column_node.find(lower_it->second.front());
            }
            if (it != column_name_to_column_node.end())
            {
                if (auto subcolumn_type = it->second->getResultType()->tryGetSubcolumnType(subcolumn_name))
                    return SubcolumnInfo{it->second, subcolumn_name, subcolumn_type};
            }
        }

        return std::nullopt;
    }

    /// builds lowercase-to-original mappings for case-insensitive identifier resolution
    void enableStandardMode()
    {
        use_standard_mode = true;
        lowercase_column_name_to_original_names.clear();

        for (const auto & [column_name, _] : column_name_to_column_node)
        {
            String lower_name = Poco::toLower(column_name);
            lowercase_column_name_to_original_names[lower_name].push_back(column_name);
        }

        /// Add lowercase entries to column_identifier_first_parts for binding check
        std::vector<std::string> first_parts_to_add;
        for (const auto & first_part : column_identifier_first_parts)
        {
            String lower_part = Poco::toLower(first_part);
            if (lower_part != first_part)
                first_parts_to_add.push_back(lower_part);
        }
        for (const auto & part : first_parts_to_add)
            column_identifier_first_parts.insert(part);
    }

};

}
