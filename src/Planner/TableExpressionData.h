#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

#include <Interpreters/ActionsDAG.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using ColumnIdentifier = std::string;
using ColumnIdentifiers = std::vector<ColumnIdentifier>;
using ColumnIdentifierSet = std::unordered_set<ColumnIdentifier>;

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
    using ColumnNameToColumn = std::unordered_map<std::string, NameAndTypePair>;

    using ColumnNameToColumnIdentifier = std::unordered_map<std::string, ColumnIdentifier>;

    using ColumnIdentifierToColumnName = std::unordered_map<ColumnIdentifier, std::string>;

    /// Return true if column with name exists, false otherwise
    bool hasColumn(const std::string & column_name) const
    {
        return alias_columns_names.contains(column_name) || column_name_to_column.contains(column_name);
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

        addColumnImpl(column, column_identifier);
    }

    /** Add column if it does not exists in table expression data.
      * Column identifier must be created using global planner context.
      */
    void addColumnIfNotExists(const NameAndTypePair & column, const ColumnIdentifier & column_identifier)
    {
        if (hasColumn(column.name))
            return;

        addColumnImpl(column, column_identifier);
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

    /// Get column name to column map
    const ColumnNameToColumn & getColumnNameToColumn() const
    {
        return column_name_to_column;
    }

    /// Get column names
    const Names & getColumnNames() const
    {
        return column_names;
    }

    NamesAndTypes getColumns() const
    {
        NamesAndTypes result;
        result.reserve(column_names.size());

        for (const auto & column_name : column_names)
            result.push_back(column_name_to_column.at(column_name));

        return result;
    }

    ColumnIdentifiers getColumnIdentifiers() const
    {
        ColumnIdentifiers result;
        result.reserve(column_identifier_to_column_name.size());

        for (const auto & [column_identifier, _] : column_identifier_to_column_name)
            result.push_back(column_identifier);

        return result;
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

    /** Get column for column name.
      * Exception is thrown if there are no column for column name.
      */
    const NameAndTypePair & getColumnOrThrow(const std::string & column_name) const
    {
        auto it = column_name_to_column.find(column_name);
        if (it == column_name_to_column.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column for column name {} does not exists. There are only column names: {}",
                column_name,
                fmt::join(column_names.begin(), column_names.end(), ", "));
        }

        return it->second;
    }

    /** Get column for column name.
      * Null is returned if there are no column for column name.
      */
    const NameAndTypePair * getColumnOrNull(const std::string & column_name) const
    {
        auto it = column_name_to_column.find(column_name);
        if (it == column_name_to_column.end())
            return nullptr;

        return &it->second;
    }

    /** Get column identifier for column name.
      * Exception is thrown if there are no column identifier for column name.
      */
    const ColumnIdentifier & getColumnIdentifierOrThrow(const std::string & column_name) const
    {
        auto it = column_name_to_column_identifier.find(column_name);
        if (it == column_name_to_column_identifier.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column identifier for column name {} does not exists. There are only column names: {}",
                column_name,
                fmt::join(column_names.begin(), column_names.end(), ", "));
        }

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
        {
            auto column_identifiers = getColumnIdentifiers();
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column name for column identifier {} does not exists. There are only column identifiers: {}",
                column_identifier,
                fmt::join(column_identifiers.begin(), column_identifiers.end(), ", "));
        }

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

    const ActionsDAGPtr & getPrewhereFilterActions() const
    {
        return prewhere_filter_actions;
    }

    void setPrewhereFilterActions(ActionsDAGPtr prewhere_filter_actions_value)
    {
        prewhere_filter_actions = std::move(prewhere_filter_actions_value);
    }

    const ActionsDAGPtr & getFilterActions() const
    {
        return filter_actions;
    }

    void setFilterActions(ActionsDAGPtr filter_actions_value)
    {
        filter_actions = std::move(filter_actions_value);
    }

private:
    void addColumnImpl(const NameAndTypePair & column, const ColumnIdentifier & column_identifier)
    {
        column_names.push_back(column.name);
        column_name_to_column.emplace(column.name, column);
        column_name_to_column_identifier.emplace(column.name, column_identifier);
        column_identifier_to_column_name.emplace(column_identifier, column.name);
    }

    /// Valid for table, table function, array join, query, union nodes
    Names column_names;

    /// Valid for table, table function, array join, query, union nodes
    ColumnNameToColumn column_name_to_column;

    /// Valid only for table node
    NameSet alias_columns_names;

    /// Valid for table, table function, array join, query, union nodes
    ColumnNameToColumnIdentifier column_name_to_column_identifier;

    /// Valid for table, table function, array join, query, union nodes
    ColumnIdentifierToColumnName column_identifier_to_column_name;

    /// Valid for table, table function
    ActionsDAGPtr filter_actions;

    /// Valid for table, table function
    ActionsDAGPtr prewhere_filter_actions;

    /// Is storage remote
    bool is_remote = false;
};

}
