#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using ColumnIdentifier = std::string;

class TableExpressionData
{
public:
    using ColumnNameToColumnIdentifier = std::unordered_map<std::string, ColumnIdentifier>;

    bool hasColumn(const std::string & column_name) const
    {
        return alias_columns_names.contains(column_name) || columns_names.contains(column_name);
    }

    void addColumn(const NameAndTypePair & column, const ColumnIdentifier & column_identifier)
    {
        if (hasColumn(column.name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column with name {} already exists");

        columns_names.insert(column.name);
        columns.push_back(column);
        column_name_to_column_identifier.emplace(column.name, column_identifier);
    }

    void addColumnIfNotExists(const NameAndTypePair & column, const ColumnIdentifier & column_identifier)
    {
        if (hasColumn(column.name))
            return;

        columns_names.insert(column.name);
        columns.push_back(column);
        column_name_to_column_identifier.emplace(column.name, column_identifier);
    }

    void addAliasColumnName(const std::string & column_name)
    {
        alias_columns_names.insert(column_name);
    }

    const NameSet & getAliasColumnsNames() const
    {
        return alias_columns_names;
    }

    const NameSet & getColumnsNames() const
    {
        return columns_names;
    }

    const NamesAndTypesList & getColumns() const
    {
        return columns;
    }

    const ColumnNameToColumnIdentifier & getColumnNameToIdentifier() const
    {
        return column_name_to_column_identifier;
    }

    const ColumnIdentifier & getColumnIdentifierOrThrow(const std::string & column_name) const
    {
        auto it = column_name_to_column_identifier.find(column_name);
        if (it == column_name_to_column_identifier.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column identifier for name {} does not exists",
                column_name);

        return it->second;
    }

    const ColumnIdentifier * getColumnIdentifierOrNull(const std::string & column_name) const
    {
        auto it = column_name_to_column_identifier.find(column_name);
        if (it == column_name_to_column_identifier.end())
            return nullptr;

        return &it->second;
    }

    /** Cache value of storage is remote method call.
      *
      * Valid only for table and table function node.
      */
    bool isRemote() const
    {
        return is_remote;
    }

    /// Set is remote value
    void setIsRemote(bool is_remote_value)
    {
        is_remote = is_remote_value;
    }

private:
    /// Valid for table, table function, query table expression nodes
    NamesAndTypesList columns;

    /// Valid for table, table function, query table expression nodes
    NameSet columns_names;

    /// Valid only for table table expression node
    NameSet alias_columns_names;

    /// Valid for table, table function, query table expression nodes
    ColumnNameToColumnIdentifier column_name_to_column_identifier;

    /// Cached value if table expression receives data from remote server
    bool is_remote = false;
};

}
