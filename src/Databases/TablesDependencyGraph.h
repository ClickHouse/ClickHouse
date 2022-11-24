#pragma once

#include <Interpreters/StorageID.h>

#include <unordered_map>
#include <unordered_set>


namespace DB
{
using TableNamesSet = std::unordered_set<QualifiedTableName>;

/// Represents dependencies of some tables on other tables or dictionaries.
///
/// NOTES: A "dependent" depends on its "dependency". For example, if table "A" depends on table "B", then
/// "B" is a dependency for "A", and "A" is a dependent for "B".
///
/// Dependencies can be added to the graph in any order. For example, if table "A" depends on "B", and "B" depends on "C", then
/// it's allowed to add first "A->B" and then "B->C", or first "B->C" and then "A->B", the resulting graph will be the same.
///
/// This class is used to represent various types of table-table dependencies:
/// 1. View dependencies: "source_table -> materialized_view".
/// Data inserted to a source table is also inserted to corresponding materialized views.
/// 2. Loading dependencies: specify in which order tables must be loaded during startup.
/// For example a dictionary should be loaded after it's source table and it's written in the graph as "dictionary -> source_table".
/// 3. Referential dependencies: "table -> all tables mentioned in its definition".
/// Referential dependencies are checked to decide if it's safe to drop a table (it can be unsafe if the table is used by another table).
///
/// WARNING: This class doesn't have an embedded mutex, so it must be synchronized outside.
class TablesDependencyGraph
{
public:
    explicit TablesDependencyGraph(const String & name_for_logging_);

    TablesDependencyGraph(const TablesDependencyGraph & src);
    TablesDependencyGraph(const TablesDependencyGraph && src);
    TablesDependencyGraph & operator=(const TablesDependencyGraph & src);
    TablesDependencyGraph & operator=(const TablesDependencyGraph && src);

    /// The dependency graph is empty if doesn't contain any tables.
    bool empty() const;

    /// Clears this dependency graph.
    void clear();

    /// Adds a single dependency "table_id" on "dependency".
    void addDependency(const StorageID & table_id, const StorageID & dependency);

    /// Adds a table with specified dependencies if there are no dependencies of the table in the graph yet;
    /// otherwise it replaces the dependencies of the table in the graph and shows a warning.
    void addDependencies(const StorageID & table_id, const std::vector<StorageID> & dependencies);
    void addDependencies(const StorageID & table_id, const TableNamesSet & dependencies);
    void addDependencies(const QualifiedTableName & table_name, const TableNamesSet & dependencies);

    /// Removes a single dependency of "table_id" on "dependency".
    /// If "remove_isolated_tables" is set the function will also remove tables with no dependencies and no dependents
    /// from the graph.
    bool removeDependency(const StorageID & table_id, const StorageID & dependency, bool remove_isolated_tables = false);

    /// Removes all dependencies of "table_id", returns those dependencies.
    std::vector<StorageID> removeDependencies(const StorageID & table_id, bool remove_isolated_tables = false);

    /// Removes a table from the graph and removes all references to in from the graph (both from its dependencies and dependents).
    bool removeTable(const StorageID & table_id);

    /// Removes tables from the graph by a specified filter.
    size_t removeTablesIf(const std::function<bool(const StorageID &)> & function);

    /// Removes tables with no dependencies and no dependents from the graph.
    size_t removeIsolatedTables();

    /// Returns the number of tables in the graph.
    size_t getNumberOfTables() const;

    /// Returns a list of all tables in the graph.
    std::vector<StorageID> getTables() const;

    /// Adds tables and dependencies with another graph.
    void mergeWith(const TablesDependencyGraph & other);

    /// Returns a list of dependencies of a specified table.
    std::vector<StorageID> getDependencies(const StorageID & table_id) const;
    size_t getNumberOfDependencies(const StorageID & table_id) const;
    bool hasDependencies(const StorageID & table_id) const { return getNumberOfDependencies(table_id) != 0; }

    /// Returns a list of dependents of a specified table.
    std::vector<StorageID> getDependents(const StorageID & table_id) const;
    size_t getNumberOfDependents(const StorageID & table_id) const;
    bool hasDependents(const StorageID & table_id) const { return getNumberOfDependents(table_id) != 0; }

    /// Returns the number of dependencies and the number of dependents of a specified table.
    void getNumberOfAdjacents(const StorageID & table_id, size_t & num_dependencies, size_t & num_dependents) const;

    /// Returns true if a specified table has no dependencies and no dependents.
    bool isIsolatedTable(const StorageID & table_id) const;

    /// Checks that there are no cyclic dependencies in the graph.
    /// Cyclic dependencies are dependencies like "A->A" or "A->B->C->D->A".
    void checkNoCyclicDependencies() const;
    bool hasCyclicDependencies() const;
    std::vector<StorageID> getTablesWithCyclicDependencies() const;
    String describeCyclicDependencies() const;

    /// Returns a list of tables sorted by their dependencies:
    /// tables without dependencies first, then
    /// tables which depend on the tables without dependencies, then
    /// tables which depend on the tables which depend on the tables without dependencies, and so on.
    std::vector<StorageID> getTablesSortedByDependency() const;

    /// The same as getTablesSortedByDependency() but make a list for parallel processing.
    std::vector<std::vector<StorageID>> getTablesSortedByDependencyForParallel() const;

    /// Outputs information about this graph as a bunch of logging messages.
    void log() const;

private:
    struct Node;
    using NodePtr = std::shared_ptr<Node>;

    struct Node
    {
        StorageID storage_id;

        /// If A depends on B then "A.dependencies" contains "B".
        std::unordered_set<NodePtr> dependencies;

        /// If A depends on B then "B.depends" contains "A".
        std::unordered_set<NodePtr> dependents;

        /// Tables without dependencies have level == 0, tables which depend on the tables without dependencies have level == 1, and so on.
        /// Calculated lazily.
        size_t level = 0;

        explicit Node(const StorageID & storage_id_) : storage_id(storage_id_) {}
    };

    using Nodes = std::vector<NodePtr>;

    struct LessByLevel
    {
        bool operator()(const NodePtr & left, const NodePtr & right) { return left->level < right->level; }
    };

    std::unordered_set<NodePtr> nodes;

    /// Nodes can be found either by UUID or by database name & table name. That's why we need two maps here.
    std::unordered_map<StorageID, NodePtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> nodes_by_database_and_table_names;
    std::unordered_map<UUID, NodePtr> nodes_by_uuid;

    /// Nodes sorted by their level. Calculated lazily.
    mutable Nodes nodes_sorted_by_level;

    /// This is set if both `level` inside each node and `nodes_sorted_by_level` are calculated.
    mutable bool levels_calculated = false;

    const String name_for_logging;
    mutable Poco::Logger * logger = nullptr;

    NodePtr findNode(const StorageID & storage_id) const;
    NodePtr addOrUpdateNode(const StorageID & storage_id);
    void removeNode(const NodePtr & node);

    std::vector<StorageID> getDependencies(const NodePtr & node) const;
    std::vector<StorageID> getDependents(const NodePtr & node) const;

    void calculateLevels() const;

    Poco::Logger * getLogger() const;
};

}
