#include <Databases/TablesDependencyGraph.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <boost/range/adaptor/reversed.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int INFINITE_LOOP;
    extern const int LOGICAL_ERROR;
}


namespace
{
    constexpr const size_t CYCLIC_LEVEL = std::numeric_limits<size_t>::max();
}


TablesDependencyGraph::TablesDependencyGraph(const String & name_for_logging_)
    : name_for_logging(name_for_logging_)
{
}


TablesDependencyGraph::TablesDependencyGraph(const TablesDependencyGraph & src)
    : TablesDependencyGraph(src.name_for_logging)
{
    *this = src;
}


TablesDependencyGraph::TablesDependencyGraph(TablesDependencyGraph && src) noexcept
    : TablesDependencyGraph(src.name_for_logging)
{
    *this = std::move(src);
}


TablesDependencyGraph & TablesDependencyGraph::operator=(const TablesDependencyGraph & src)
{
    if (this != &src)
    {
        nodes = src.nodes;
        nodes_by_database_and_table_names = src.nodes_by_database_and_table_names;
        nodes_by_uuid = src.nodes_by_uuid;
        levels_calculated = src.levels_calculated;
        nodes_sorted_by_level_lazy = src.nodes_sorted_by_level_lazy;
    }
    return *this;
}


TablesDependencyGraph & TablesDependencyGraph::operator=(TablesDependencyGraph && src) noexcept
{
    if (this != &src)
    {
        nodes = std::exchange(src.nodes, decltype(nodes){});
        nodes_by_database_and_table_names = std::exchange(src.nodes_by_database_and_table_names, decltype(nodes_by_database_and_table_names){});
        nodes_by_uuid = std::exchange(src.nodes_by_uuid, decltype(nodes_by_uuid){});
        levels_calculated = std::exchange(src.levels_calculated, false);
        nodes_sorted_by_level_lazy = std::exchange(src.nodes_sorted_by_level_lazy, decltype(nodes_sorted_by_level_lazy){});
    }
    return *this;
}


void TablesDependencyGraph::clear()
{
    nodes.clear();
    nodes_by_database_and_table_names.clear();
    nodes_by_uuid.clear();
    setNeedRecalculateLevels();
}


bool TablesDependencyGraph::empty() const
{
    return nodes.empty();
}


size_t TablesDependencyGraph::getNumberOfTables() const
{
    return nodes.size();
}


void TablesDependencyGraph::addDependency(const StorageID & table_id, const StorageID & dependency)
{
    auto * table_node = addOrUpdateNode(table_id);
    auto * dependency_node = addOrUpdateNode(dependency);

    bool inserted = table_node->dependencies.insert(dependency_node).second;
    if (!inserted)
        return; /// Not inserted because we already had this dependency.

    /// `dependency_node` must be updated too.
    [[maybe_unused]] bool inserted_to_set = dependency_node->dependents.insert(table_node).second;
    chassert(inserted_to_set);

    setNeedRecalculateLevels();
}


void TablesDependencyGraph::addDependencies(const StorageID & table_id, const std::vector<StorageID> & dependencies)
{
    auto * table_node = addOrUpdateNode(table_id);

    std::unordered_set<Node *> new_dependency_nodes;
    for (const auto & dependency : dependencies)
        new_dependency_nodes.emplace(addOrUpdateNode(dependency));

    if (table_node->dependencies == new_dependency_nodes)
        return;

    auto old_dependencies = getDependencies(*table_node);
    auto old_dependency_nodes = std::move(table_node->dependencies);

    if (!old_dependencies.empty())
    {
        LOG_WARNING(
            getLogger(),
            "Replacing outdated dependencies ({}) of {} with: {}",
            fmt::join(old_dependencies, ", "),
            table_id,
            fmt::join(dependencies, ", "));
    }

    for (auto * dependency_node : old_dependency_nodes)
    {
        if (!new_dependency_nodes.contains(dependency_node))
        {
            [[maybe_unused]] bool removed_from_set = dependency_node->dependents.erase(table_node);
            chassert(removed_from_set);
        }
    }

    for (auto * dependency_node : new_dependency_nodes)
    {
        if (!old_dependency_nodes.contains(dependency_node))
        {
            [[maybe_unused]] bool inserted_to_set = dependency_node->dependents.insert(table_node).second;
            chassert(inserted_to_set);
        }
    }

    table_node->dependencies = std::move(new_dependency_nodes);
    setNeedRecalculateLevels();
}


void TablesDependencyGraph::addDependencies(const StorageID & table_id, const TableNamesSet & dependencies)
{
    std::vector<StorageID> converted_dependencies;
    for (const auto & dependency : dependencies)
        converted_dependencies.emplace_back(StorageID{dependency});
    addDependencies(table_id, converted_dependencies);
}


void TablesDependencyGraph::addDependencies(const QualifiedTableName & table_name, const TableNamesSet & dependencies)
{
    addDependencies(StorageID{table_name}, dependencies);
}


bool TablesDependencyGraph::removeDependency(const StorageID & table_id, const StorageID & dependency, bool remove_isolated_tables)
{
    auto * table_node = findNode(table_id);
    if (!table_node)
        return false;

    auto * dependency_node = findNode(dependency);
    if (!dependency_node)
        return false;

    auto dependency_it = table_node->dependencies.find(dependency_node);
    if (dependency_it == table_node->dependencies.end())
        return false; /// No such dependency, nothing to remove.

    table_node->dependencies.erase(dependency_it);
    bool table_node_removed = false;

    /// `dependency_node` must be updated too.
    [[maybe_unused]] bool removed_from_set = dependency_node->dependents.erase(table_node);
    chassert(removed_from_set);

    if (remove_isolated_tables && dependency_node->dependencies.empty() && dependency_node->dependents.empty())
    {
        /// The dependency table has no dependencies and no dependents now, so we will remove it from the graph.
        removeNode(dependency_node);
        if (table_node == dependency_node)
            table_node_removed = true;
    }

    if (remove_isolated_tables && !table_node_removed && table_node->dependencies.empty() && table_node->dependents.empty())
    {
        /// The table `table_id` has no dependencies and no dependents now, so we will remove it from the graph.
        removeNode(table_node);
    }

    setNeedRecalculateLevels();
    return true;
}


std::vector<StorageID> TablesDependencyGraph::removeDependencies(const StorageID & table_id, bool remove_isolated_tables)
{
    auto * table_node = findNode(table_id);
    if (!table_node)
        return {};

    auto dependency_nodes = std::move(table_node->dependencies);
    table_node->dependencies.clear();
    bool table_node_removed = false;

    std::vector<StorageID> dependencies;
    dependencies.reserve(dependency_nodes.size());

    for (auto * dependency_node : dependency_nodes)
    {
        /// We're gathering the list of dependencies the table `table_id` had in the graph to return from the function.
        dependencies.emplace_back(dependency_node->storage_id);

        /// Update `dependency_node`.
        [[maybe_unused]] bool removed_from_set = dependency_node->dependents.erase(table_node);
        chassert(removed_from_set);

        if (remove_isolated_tables && dependency_node->dependencies.empty() && dependency_node->dependents.empty())
        {
            /// The dependency table has no dependencies and no dependents now, so we will remove it from the graph.
            removeNode(dependency_node);
            if (table_node == dependency_node)
                table_node_removed = true;
        }
    }

    chassert(table_node->dependencies.empty());
    if (remove_isolated_tables && !table_node_removed && table_node->dependents.empty())
    {
        /// The table `table_id` has no dependencies and no dependents now, so we will remove it from the graph.
        removeNode(table_node);
    }

    setNeedRecalculateLevels();
    return dependencies;
}


bool TablesDependencyGraph::removeTable(const StorageID & table_id)
{
    auto * table_node = findNode(table_id);
    if (!table_node)
        return false;

    removeNode(table_node);

    setNeedRecalculateLevels();
    return true;
}


TablesDependencyGraph::Node * TablesDependencyGraph::findNode(const StorageID & table_id) const
{
    table_id.assertNotEmpty();
    if (table_id.hasUUID())
    {
        auto it = nodes_by_uuid.find(table_id.uuid);
        if (it != nodes_by_uuid.end())
            return it->second; /// Found by UUID.
    }
    if (!table_id.table_name.empty())
    {
        auto it = nodes_by_database_and_table_names.find(table_id);
        if (it != nodes_by_database_and_table_names.end())
        {
            auto * node = it->second;
            if (table_id.hasUUID() && node->storage_id.hasUUID() && (table_id.uuid != node->storage_id.uuid))
            {
                /// We found a table with specified database and table names in the graph, but surprisingly it has a different UUID.
                /// Maybe an "EXCHANGE TABLES" command has been executed somehow without changing the graph?
                LOG_WARNING(getLogger(), "Found table {} in the graph with unexpected UUID {}", table_id, node->storage_id.uuid);
                return nullptr; /// Act like it's not found.
            }
            return node; /// Found by table name.
        }
    }
    return nullptr; /// Not found.
}


TablesDependencyGraph::Node * TablesDependencyGraph::addOrUpdateNode(const StorageID & table_id)
{
    auto * node = findNode(table_id);
    if (node)
    {
        /// Node has been found, maybe we can update the information in the graph with new table_name or new UUID.
        if (table_id.hasUUID() && !node->storage_id.hasUUID())
        {
            node->storage_id.uuid = table_id.uuid;
            [[maybe_unused]] bool inserted_to_map = nodes_by_uuid.emplace(node->storage_id.uuid, node).second;
            chassert(inserted_to_map);
        }

        if (!table_id.table_name.empty() && ((table_id.table_name != node->storage_id.table_name) || (table_id.database_name != node->storage_id.database_name)))
        {
            auto it = nodes_by_database_and_table_names.find(table_id);
            if (it != nodes_by_database_and_table_names.end())
            {
                LOG_WARNING(getLogger(), "Name conflict in the graph having tables {} and {} while adding table {}. Will remove {} from the graph",
                            node->storage_id, it->second->storage_id, table_id, it->second->storage_id);
                removeNode(it->second);
            }
            nodes_by_database_and_table_names.erase(node->storage_id);
            node->storage_id.database_name = table_id.database_name;
            node->storage_id.table_name = table_id.table_name;
            [[maybe_unused]] bool inserted_to_map = nodes_by_database_and_table_names.emplace(node->storage_id, node).second;
            chassert(inserted_to_map);
        }
    }
    else
    {
        /// Node has not been found by UUID or table name.
        if (!table_id.table_name.empty())
        {
            auto it = nodes_by_database_and_table_names.find(table_id);
            if (it != nodes_by_database_and_table_names.end())
            {
                LOG_WARNING(getLogger(), "Name conflict in the graph having table {} while adding table {}. Will remove {} from the graph",
                            it->second->storage_id, table_id, it->second->storage_id);
                removeNode(it->second);
            }
        }
        auto node_ptr = std::make_shared<Node>(table_id);
        nodes.insert(node_ptr);
        node = node_ptr.get();
        if (table_id.hasUUID())
        {
            [[maybe_unused]] bool inserted_to_map = nodes_by_uuid.emplace(table_id.uuid, node).second;
            chassert(inserted_to_map);
        }
        if (!table_id.table_name.empty())
        {
            [[maybe_unused]] bool inserted_to_map = nodes_by_database_and_table_names.emplace(table_id, node).second;
            chassert(inserted_to_map);
        }
    }
    return node;
}


void TablesDependencyGraph::removeNode(Node * node)
{
    chassert(node);
    auto dependency_nodes = std::move(node->dependencies);
    auto dependent_nodes = std::move(node->dependents);

    if (node->storage_id.hasUUID())
    {
        [[maybe_unused]] bool removed_from_map = nodes_by_uuid.erase(node->storage_id.uuid);
        chassert(removed_from_map);
    }

    if (!node->storage_id.table_name.empty())
    {
        [[maybe_unused]]bool removed_from_map = nodes_by_database_and_table_names.erase(node->storage_id);
        chassert(removed_from_map);
    }

    for (auto * dependency_node : dependency_nodes)
    {
        [[maybe_unused]] bool removed_from_set = dependency_node->dependents.erase(node);
        chassert(removed_from_set);
    }

    for (auto * dependent_node : dependent_nodes)
    {
        [[maybe_unused]] bool removed_from_set = dependent_node->dependencies.erase(node);
        chassert(removed_from_set);
    }

    auto it = nodes.find(node);
    chassert(it != nodes.end());
    nodes.erase(it);

    nodes_sorted_by_level_lazy.clear();
}


size_t TablesDependencyGraph::removeTablesIf(const std::function<bool(const StorageID &)> & function)
{
    size_t num_removed = 0;

    auto it = nodes.begin();
    while (it != nodes.end())
    {
        auto * current = (it++)->get();
        if (function(current->storage_id))
        {
            StorageID storage_id = current->storage_id;
            removeNode(current);
            ++num_removed;
        }
    }

    if (num_removed)
        setNeedRecalculateLevels();

    return num_removed;
}


size_t TablesDependencyGraph::removeIsolatedTables()
{
    size_t num_removed = 0;
    auto it = nodes.begin();
    while (it != nodes.end())
    {
        auto * current = (it++)->get();
        if (current->dependencies.empty() && current->dependents.empty())
        {
            removeNode(current);
            ++num_removed;
        }
    }

    if (num_removed)
        setNeedRecalculateLevels();

    return num_removed;
}


std::vector<StorageID> TablesDependencyGraph::getTables() const
{
    std::vector<StorageID> res;
    res.reserve(nodes.size());
    for (const auto & node : nodes)
        res.emplace_back(node->storage_id);
    return res;
}


void TablesDependencyGraph::mergeWith(const TablesDependencyGraph & other)
{
    for (const auto & other_node : other.nodes)
        addDependencies(other_node->storage_id, TablesDependencyGraph::getDependencies(*other_node));
}


std::vector<StorageID> TablesDependencyGraph::getDependencies(const StorageID & table_id) const
{
    const auto * node = findNode(table_id);
    if (!node)
        return {};
    return getDependencies(*node);
}


std::vector<StorageID> TablesDependencyGraph::getDependencies(const Node & node)
{
    std::vector<StorageID> res;
    res.reserve(node.dependencies.size());
    for (const auto * dependency_node : node.dependencies)
        res.emplace_back(dependency_node->storage_id);
    return res;
}

size_t TablesDependencyGraph::getNumberOfDependencies(const StorageID & table_id) const
{
    const auto * node = findNode(table_id);
    if (!node)
        return 0;
    return node->dependencies.size();
}


std::vector<StorageID> TablesDependencyGraph::getDependents(const StorageID & table_id) const
{
    const auto * node = findNode(table_id);
    if (!node)
        return {};
    return getDependents(*node);
}


std::vector<StorageID> TablesDependencyGraph::getDependents(const Node & node)
{
    std::vector<StorageID> res;
    res.reserve(node.dependents.size());
    for (const auto * dependent_node : node.dependents)
        res.emplace_back(dependent_node->storage_id);
    return res;
}


size_t TablesDependencyGraph::getNumberOfDependents(const StorageID & table_id) const
{
    const auto * node = findNode(table_id);
    if (!node)
        return 0;
    return node->dependents.size();
}


void TablesDependencyGraph::getNumberOfAdjacents(const StorageID & table_id, size_t & num_dependencies, size_t & num_dependents) const
{
    num_dependencies = 0;
    num_dependents = 0;

    const auto * node = findNode(table_id);
    if (!node)
        return;

    num_dependencies = node->dependencies.size();
    num_dependents = node->dependents.size();
}


bool TablesDependencyGraph::isIsolatedTable(const StorageID & table_id) const
{
    const auto * node = findNode(table_id);
    if (!node)
        return false;

    return node->dependencies.empty() && node->dependents.empty();
}


void TablesDependencyGraph::checkNoCyclicDependencies() const
{
    if (hasCyclicDependencies())
    {
        throw Exception(
            ErrorCodes::INFINITE_LOOP,
            "{}: Tables {} have cyclic dependencies: {}",
            name_for_logging,
            fmt::join(getTablesWithCyclicDependencies(), ", "),
            describeCyclicDependencies());
    }
}


bool TablesDependencyGraph::hasCyclicDependencies() const
{
    const auto & nodes_sorted_by_level = getNodesSortedByLevel();
    return !nodes_sorted_by_level.empty() && (nodes_sorted_by_level.back()->level == CYCLIC_LEVEL);
}


std::vector<StorageID> TablesDependencyGraph::getTablesWithCyclicDependencies() const
{
    std::vector<StorageID> res;
    for (const auto * node : getNodesSortedByLevel() | boost::adaptors::reversed)
    {
        if (node->level != CYCLIC_LEVEL)
            break;
        res.emplace_back(node->storage_id);
    }
    return res;
}


String TablesDependencyGraph::describeCyclicDependencies() const
{
    String res;
    for (const auto * node : getNodesSortedByLevel() | boost::adaptors::reversed)
    {
        if (node->level != CYCLIC_LEVEL)
            break;
        if (!res.empty())
            res += "; ";
        res += node->storage_id.getNameForLogs();
        res += " -> [";
        bool need_comma = false;
        for (const auto * dependency_node : node->dependencies)
        {
            if (dependency_node->level != CYCLIC_LEVEL)
                continue;
            if (need_comma)
                res += ", ";
            need_comma = true;
            res += dependency_node->storage_id.getNameForLogs();
        }
        res += "]";
    }
    return res;
}


void TablesDependencyGraph::setNeedRecalculateLevels() const
{
    levels_calculated = false;
    nodes_sorted_by_level_lazy.clear();
}


void TablesDependencyGraph::calculateLevels() const
{
    if (levels_calculated)
        return;
    levels_calculated = true;

    /// First find tables with no dependencies, add them to `nodes_sorted_by_level_lazy`.
    /// Then remove those tables from the dependency graph (we imitate that removing by decrementing `num_dependencies_to_count`),
    /// and find which tables have no dependencies now.
    /// Repeat until we have tables with no dependencies.
    /// In the end we expect all nodes from `nodes` to be added to `nodes_sorted_by_level_lazy`.
    /// If some nodes are still not added to `nodes_sorted_by_level_lazy` in the end then there is a cyclic dependency.
    /// Complexity: O(V + E)

    nodes_sorted_by_level_lazy.clear();
    nodes_sorted_by_level_lazy.reserve(nodes.size());
    size_t current_level = 0;

    /// Find tables with no dependencies.
    for (const auto & node_ptr : nodes)
    {
        const Node * node = node_ptr.get();
        node->num_dependencies_to_count = node->dependencies.size();
        if (!node->num_dependencies_to_count)
        {
            node->level = current_level;
            nodes_sorted_by_level_lazy.emplace_back(node);
        }
    }

    size_t num_nodes_without_dependencies = nodes_sorted_by_level_lazy.size();
    ++current_level;

    while (num_nodes_without_dependencies)
    {
        size_t begin = nodes_sorted_by_level_lazy.size() - num_nodes_without_dependencies;
        size_t end = nodes_sorted_by_level_lazy.size();

        /// Decrement number of dependencies for each dependent table.
        for (size_t i = begin; i != end; ++i)
        {
            const Node * current_node = nodes_sorted_by_level_lazy[i];
            for (const Node * dependent_node : current_node->dependents)
            {
                if (!dependent_node->num_dependencies_to_count)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "{}: Trying to decrement 0 dependencies counter for {}. It's a bug",
                                    name_for_logging, dependent_node->storage_id);

                if (!--dependent_node->num_dependencies_to_count)
                {
                    dependent_node->level = current_level;
                    nodes_sorted_by_level_lazy.emplace_back(dependent_node);
                }
            }
        }

        if (nodes_sorted_by_level_lazy.size() > nodes.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "{}: Some tables were found more than once while passing through the dependency graph. "
                            "It's a bug", name_for_logging);

        num_nodes_without_dependencies = nodes_sorted_by_level_lazy.size() - end;
        ++current_level;
    }

    if (nodes_sorted_by_level_lazy.size() < nodes.size())
    {
        for (const auto & node_ptr : nodes)
        {
            const Node * node = node_ptr.get();
            if (node->num_dependencies_to_count)
            {
                node->level = CYCLIC_LEVEL;
                nodes_sorted_by_level_lazy.emplace_back(node);
            }
        }
    }
}


const TablesDependencyGraph::NodesSortedByLevel & TablesDependencyGraph::getNodesSortedByLevel() const
{
    calculateLevels();
    return nodes_sorted_by_level_lazy;
}


std::vector<StorageID> TablesDependencyGraph::getTablesSortedByDependency() const
{
    std::vector<StorageID> res;
    res.reserve(nodes.size());
    for (const auto * node : getNodesSortedByLevel())
    {
        res.emplace_back(node->storage_id);
    }
    return res;
}


std::vector<std::vector<StorageID>> TablesDependencyGraph::getTablesSplitByDependencyLevel() const
{
    std::vector<std::vector<StorageID>> tables_split_by_level;
    auto sorted_nodes = getNodesSortedByLevel();
    if (sorted_nodes.empty())
        return tables_split_by_level;

    tables_split_by_level.resize(sorted_nodes.back()->level + 1);
    for (const auto * node : sorted_nodes)
    {
        tables_split_by_level[node->level].emplace_back(node->storage_id);
    }
    return tables_split_by_level;
}


void TablesDependencyGraph::log() const
{
    if (nodes.empty())
    {
        LOG_TRACE(getLogger(), "No tables");
        return;
    }

    for (const auto * node : getNodesSortedByLevel())
    {
        String dependencies_desc = node->dependencies.empty()
            ? "no dependencies"
            : fmt::format("{} dependencies: {}", node->dependencies.size(), fmt::join(getDependencies(*node), ", "));

        String level_desc = (node->level == CYCLIC_LEVEL) ? "cyclic" : fmt::format("level {}", node->level);

        LOG_TRACE(getLogger(), "Table {} has {} ({})", node->storage_id, dependencies_desc, level_desc);
    }
}


LoggerPtr TablesDependencyGraph::getLogger() const
{
    if (!logger)
        logger = ::getLogger(name_for_logging);
    return logger;
}

}
