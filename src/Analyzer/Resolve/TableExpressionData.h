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
    ColumnNameToColumnNodeMap column_name_to_column_node;
    std::unordered_set<std::string> subcolumn_names; /// Subset columns that are subcolumns of other columns
    std::unordered_set<std::string, StringTransparentHash, std::equal_to<>> column_identifier_first_parts;

    bool hasFullIdentifierName(IdentifierView identifier_view) const
    {
        return column_name_to_column_node.contains(identifier_view.getFullName());
    }

    bool canBindIdentifier(IdentifierView identifier_view) const
    {
        return column_identifier_first_parts.contains(identifier_view.at(0)) || column_name_to_column_node.contains(identifier_view.at(0))
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

    std::optional<SubcolumnInfo> tryGetSubcolumnInfo(std::string_view full_identifier_name) const
    {
        for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(full_identifier_name))
        {
            auto it = column_name_to_column_node.find(column_name);
            if (it != column_name_to_column_node.end())
            {
                if (auto subcolumn_type = it->second->getResultType()->tryGetSubcolumnType(subcolumn_name))
                    return SubcolumnInfo{it->second, subcolumn_name, subcolumn_type};
            }
        }

        return std::nullopt;
    }
};

}
