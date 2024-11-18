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

struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

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
        return column_name_to_column.contains(column_name);
    }

    /** Add column in table expression data.
      * Column identifier must be created using global planner context.
      *
      * Logical error exception is thrown if column already exists.
      */
    void addColumn(const NameAndTypePair & column, const ColumnIdentifier & column_identifier, bool is_selected_column = true)
    {
        if (hasColumn(column.name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column with name {} already exists", column.name);

        column_names.push_back(column.name);
        addColumnImpl(column, column_identifier, is_selected_column);
    }

    /// Add alias column
    void addAliasColumn(const NameAndTypePair & column, const ColumnIdentifier & column_identifier, ActionsDAG actions_dag, bool is_selected_column = true)
    {
        alias_column_expressions.emplace(column.name, std::move(actions_dag));
        addColumnImpl(column, column_identifier, is_selected_column);
    }

    /// Mark existing column as selected
    void markSelectedColumn(const std::string & column_name)
    {
        auto [_, inserted] = selected_column_names_set.emplace(column_name);
        if (inserted)
            selected_column_names.push_back(column_name);
    }

    /// Get columns that are requested from table expression, including ALIAS columns
    const Names & getSelectedColumnsNames() const
    {
        return selected_column_names;
    }

    /// Get ALIAS columns names mapped to expressions
    std::unordered_map<std::string, ActionsDAG> & getAliasColumnExpressions()
    {
        return alias_column_expressions;
    }

    /// Get column name to column map
    const ColumnNameToColumn & getColumnNameToColumn() const
    {
        return column_name_to_column;
    }

    /// Get column names that are read from table expression
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
                "Column for column name {} does not exist. There are only column names: {}",
                column_name,
                fmt::join(column_names.begin(), column_names.end(), ", "));
        }

        return it->second;
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
                "Column identifier for column name {} does not exist. There are only column names: {}",
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

    bool isMergeTree() const
    {
        return is_merge_tree;
    }

    void setIsMergeTree(bool is_merge_tree_value)
    {
        is_merge_tree = is_merge_tree_value;
    }

    const std::optional<ActionsDAG> & getPrewhereFilterActions() const
    {
        return prewhere_filter_actions;
    }

    void setRowLevelFilterActions(ActionsDAG row_level_filter_actions_value)
    {
        row_level_filter_actions = std::move(row_level_filter_actions_value);
    }

    const std::optional<ActionsDAG> & getRowLevelFilterActions() const
    {
        return row_level_filter_actions;
    }

    void setPrewhereFilterActions(ActionsDAG prewhere_filter_actions_value)
    {
        prewhere_filter_actions = std::move(prewhere_filter_actions_value);
    }

    const std::optional<ActionsDAG> & getFilterActions() const
    {
        return filter_actions;
    }

    void setFilterActions(ActionsDAG filter_actions_value)
    {
        filter_actions = std::move(filter_actions_value);
    }

    const PrewhereInfoPtr & getPrewhereInfo() const
    {
        return prewhere_info;
    }

    void setPrewhereInfo(PrewhereInfoPtr prewhere_info_value)
    {
        prewhere_info = std::move(prewhere_info_value);
    }

private:
    void addColumnImpl(const NameAndTypePair & column, const ColumnIdentifier & column_identifier, bool add_to_selected_columns)
    {
        if (add_to_selected_columns)
            markSelectedColumn(column.name);

        column_name_to_column.emplace(column.name, column);
        column_name_to_column_identifier.emplace(column.name, column_identifier);
        column_identifier_to_column_name.emplace(column_identifier, column.name);
    }

    /// Set of columns that are physically read from table expression
    /// In case of ALIAS columns it contains source column names that are used to calculate alias
    /// This source column may be not used by user
    Names column_names;

    /// Set of columns that are SELECTed from table expression
    /// It may contain ALIAS columns.
    /// Mainly it's used to determine access to which columns to check
    /// For example user may have an access to column `a ALIAS x + y` but not to `x` and `y`
    /// In that case we can read `x` and `y` and calculate `a`, but not return `x` and `y` to user
    Names selected_column_names;
    /// To deduplicate columns in `selected_column_names`
    NameSet selected_column_names_set;

    /// Expression to calculate ALIAS columns
    std::unordered_map<std::string, ActionsDAG> alias_column_expressions;

    /// Valid for table, table function, array join, query, union nodes
    ColumnNameToColumn column_name_to_column;

    /// Valid for table, table function, array join, query, union nodes
    ColumnNameToColumnIdentifier column_name_to_column_identifier;

    /// Valid for table, table function, array join, query, union nodes
    ColumnIdentifierToColumnName column_identifier_to_column_name;

    /// Valid for table, table function
    std::optional<ActionsDAG> filter_actions;

    /// Valid for table, table function
    PrewhereInfoPtr prewhere_info;

    /// Valid for table, table function
    std::optional<ActionsDAG> prewhere_filter_actions;

    /// Valid for table, table function
    std::optional<ActionsDAG> row_level_filter_actions;

    /// Is storage remote
    bool is_remote = false;

    /// Is storage merge tree
    bool is_merge_tree = false;
};

}
