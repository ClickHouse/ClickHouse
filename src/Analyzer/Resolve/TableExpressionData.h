#pragma once

#include <IO/Operators.h>
#include <Analyzer/ColumnNode.h>

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
        return column_identifier_first_parts.contains(identifier_view.at(0));
    }

    [[maybe_unused]] void dump(WriteBuffer & buffer) const
    {
        buffer << "Table expression name " << table_expression_name;

        if (!table_expression_description.empty())
            buffer << " table expression description " << table_expression_description;

        if (!database_name.empty())
            buffer << " database name " << database_name;

        if (!table_name.empty())
            buffer << " table name " << table_name;

        buffer << " should qualify columns " << should_qualify_columns;
        buffer << " columns size " << column_name_to_column_node.size() << '\n';

        for (const auto & [column_name, column_node] : column_name_to_column_node)
            buffer << "Column name " << column_name << " column node " << column_node->dumpTree() << '\n';
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }
};

}
