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
  *
  * Table expression can be table, table function, query, union, array join node.
  *
  * Examples:
  * SELECT * FROM (SELECT 1);
  * (SELECT 1) - table expression.
  *
  * SELECT * FROM test_table;
  * test_table - table expression.
  *
  * SELECT * FROM view(SELECT 1);
  * view(SELECT 1) - table expression.
  *
  * SELECT * FROM (SELECT 1) JOIN (SELECT 2);
  * (SELECT 1) - table expression.
  * (SELECT 2) - table expression.
  *
  * SELECT array, a FROM (SELECT [1] AS array) ARRAY JOIN array AS a;
  * ARRAY JOIN array AS a - table expression.
  */
class TableExpressionData
{
public:
    using ColumnNameToColumnIdentifier = std::unordered_map<std::string, ColumnIdentifier>;

    using ColumnIdentifierToColumnName = std::unordered_map<ColumnIdentifier, std::string>;

    /// Return true if column with name exists, false otherwise
    bool hasColumn(const std::string & column_name) const
    {
        return alias_columns_names.contains(column_name) || columns_names.contains(column_name);
    }

    /** Add column in table expression data.
      * Column identifier must be created using global planner context.
      *
      * Logical error exception is thrown if column already exists.
      */
    void addColumn(const NameAndTypePair & column, const ColumnIdentifier & column_identifier)
    {
        if (hasColumn(column.name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column with name {} already exists");

        columns_names.insert(column.name);
        columns.push_back(column);
        column_name_to_column_identifier.emplace(column.name, column_identifier);
        column_identifier_to_column_name.emplace(column_identifier, column.name);
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
        column_identifier_to_column_name.emplace(column_identifier, column.name);
    }

    /// Add alias column name
    void addAliasColumnName(const std::string & column_name)
    {
        alias_columns_names.insert(column_name);
    }

    /// Get alias columns names
    const NameSet & getAliasColumnsNames() const
    {
        return alias_columns_names;
    }

    /// Get columns names
    const NameSet & getColumnsNames() const
    {
        return columns_names;
    }

    /// Get columns
    const NamesAndTypesList & getColumns() const
    {
        return columns;
    }

    /// Get column name to column identifier map
    const ColumnNameToColumnIdentifier & getColumnNameToIdentifier() const
    {
        return column_name_to_column_identifier;
    }

    /// Get column identifier to column name map
    const ColumnNameToColumnIdentifier & getColumnIdentifierToColumnName() const
    {
        return column_identifier_to_column_name;
    }

    /** Get column identifier for column name.
      * Exception is thrown if there are no column identifier for column name.
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
      * Null is returned if there are no column identifier for column name.
      */
    const ColumnIdentifier * getColumnIdentifierOrNull(const std::string & column_name) const
    {
        auto it = column_name_to_column_identifier.find(column_name);
        if (it == column_name_to_column_identifier.end())
            return nullptr;

        return &it->second;
    }

    /** Get column name for column identifier.
      * Exception is thrown if there are no column name for column identifier.
      */
    const std::string & getColumnNameOrThrow(const ColumnIdentifier & column_identifier) const
    {
        auto it = column_identifier_to_column_name.find(column_identifier);
        if (it == column_identifier_to_column_name.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column name for identifier {} does not exists",
                column_identifier);

        return it->second;
    }

    /** Get column name for column identifier.
      * Null is returned if there are no column name for column identifier.
      */
    const std::string * getColumnNameOrNull(const ColumnIdentifier & column_identifier) const
    {
        auto it = column_identifier_to_column_name.find(column_identifier);
        if (it == column_identifier_to_column_name.end())
            return nullptr;

        return &it->second;
    }

    /** Returns true if storage is remote, false otherwise.
      *
      * Valid only for table and table function node.
      */
    bool isRemote() const
    {
        return is_remote;
    }

    /// Set is storage remote value
    void setIsRemote(bool is_remote_value)
    {
        is_remote = is_remote_value;
    }

private:
    /// Valid for table, table function, query, union, array join table expression nodes
    NamesAndTypesList columns;

    /// Valid for table, table function, query, union, array join table expression nodes
    NameSet columns_names;

    /// Valid only for table table expression node
    NameSet alias_columns_names;

    /// Valid for table, table function, query, union table, array join expression nodes
    ColumnNameToColumnIdentifier column_name_to_column_identifier;

    /// Valid for table, table function, query, union table, array join expression nodes
    ColumnIdentifierToColumnName column_identifier_to_column_name;

    /// Is storage remote
    bool is_remote = false;
};

}
