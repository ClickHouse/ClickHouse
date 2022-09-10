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

/** Table expression data is created for each table expression that take part in query.
  * Table expression data has information about columns that participate in query, their name to identifier mapping,
  * and additional table expression properties.
  */
class TableExpressionData
{
public:
    using ColumnNameToColumnIdentifier = std::unordered_map<std::string, ColumnIdentifier>;

    /// Return true if column with name exists, false otherwise
    bool hasColumn(const std::string & column_name) const
    {
        return alias_columns_names.contains(column_name) || columns_names.contains(column_name);
    }

    /** Add column in table expression data.
      * Column identifier must be created using global planner context.
      */
    void addColumn(const NameAndTypePair & column, const ColumnIdentifier & column_identifier)
    {
        if (hasColumn(column.name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column with name {} already exists");

        columns_names.insert(column.name);
        columns.push_back(column);
        column_name_to_column_identifier.emplace(column.name, column_identifier);
    }

    /** Add column if it does not exists in table expression data.
      * Column identifier must be created using global planner context.
      */
    void addColumnIfNotExists(const NameAndTypePair & column, const ColumnIdentifier & column_identifier)
    {
        if (hasColumn(column.name))
            return;

        columns_names.insert(column.name);
        columns.push_back(column);
        column_name_to_column_identifier.emplace(column.name, column_identifier);
    }

    /// Add alias column name
    void addAliasColumnName(const std::string & column_name)
    {
        alias_columns_names.insert(column_name);
    }

    /// Get alias column names
    const NameSet & getAliasColumnsNames() const
    {
        return alias_columns_names;
    }

    /// Get column names
    const NameSet & getColumnsNames() const
    {
        return columns_names;
    }

    /// Get columns
    const NamesAndTypesList & getColumns() const
    {
        return columns;
    }

    /// Get column name to identifier map
    const ColumnNameToColumnIdentifier & getColumnNameToIdentifier() const
    {
        return column_name_to_column_identifier;
    }

    /** Get column identifier for column name.
      * Exception is thrown if there are no identifier for column name.
      */
    const ColumnIdentifier & getColumnIdentifierOrThrow(const std::string & column_name) const
    {
        auto it = column_name_to_column_identifier.find(column_name);
        if (it == column_name_to_column_identifier.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column identifier for name {} does not exists",
                column_name);

        return it->second;
    }

    /** Get column identifier for column name.
      * Null is returned if there are no identifier for column name.
      */
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
