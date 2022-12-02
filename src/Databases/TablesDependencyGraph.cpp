#include <Databases/TablesDependencyGraph.h>
#include <Common/logger_useful.h>
#include <boost/range/adaptor/reversed.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int INFINITE_LOOP;
}


namespace
{
    constexpr const size_t CYCLIC_LEVEL = static_cast<size_t>(-2);
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
    nodes = src.nodes;
    nodes_by_database_and_table_names = src.nodes_by_database_and_table_names;
    nodes_by_uuid = src.nodes_by_uuid;
    levels_calculated = src.levels_calculated;
    nodes_sorted_by_level_lazy = src.nodes_sorted_by_level_lazy;
    return *this;
}


TablesDependencyGraph & TablesDependencyGraph::operator=(TablesDependencyGraph && src) noexcept
{
    nodes = std::exchange(src.nodes, decltype(nodes){});
    nodes_by_database_and_table_names = std::exchange(src.nodes_by_database_and_table_names, decltype(nodes_by_database_and_table_names){});
    nodes_by_uuid = std::exchange(src.nodes_by_uuid, decltype(nodes_by_uuid){});
    levels_calculated = std::exchange(src.levels_calculated, false);
    nodes_sorted_by_level_lazy = std::exchange(src.nodes_sorted_by_level_lazy, decltype(nodes_sorted_by_level_lazy){});
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

    if (table_node->dependencies.contains(dependency_node))
        return; /// Already have this dependency.

    table_node->dependencies.insert(dependency_node);
    dependency_node->dependents.insert(table_node);

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
            dependency_node->dependents.erase(table_node);
    }

    for (auto * dependency_node : new_dependency_nodes)
    {
        if (!old_dependency_nodes.contains(dependency_node))
            dependency_node->dependents.insert(table_node);
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
        return false;

    table_node->dependencies.erase(dependency_it);
    dependency_node->dependents.erase(table_node);
    bool table_node_removed = false;

    if (remove_isolated_tables && dependency_node->dependencies.empty() && dependency_node->dependents.empty())
    {
        removeNode(dependency_node);
        if (table_node == dependency_node)
            table_node_removed = true;
    }

    if (remove_isolated_tables && !table_node_removed && table_node->dependencies.empty() && table_node->dependents.empty())
        removeNode(table_node);

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
        dependencies.emplace_back(dependency_node->storage_id);
        dependency_node->dependents.erase(table_node);

        if (remove_isolated_tables && dependency_node->dependencies.empty() && dependency_node->dependents.empty())
        {
            removeNode(dependency_node);
            if (table_node == dependency_node)
                table_node_removed = true;
        }
    }

    if (remove_isolated_tables && !table_node_removed && table_node->dependencies.empty() && table_node->dependents.empty())
        removeNode(table_node);

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
                return nullptr; /// UUID is different, it's not the node we're looking for.
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
            nodes_by_uuid.emplace(node->storage_id.uuid, node);
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
            nodes_by_database_and_table_names.emplace(node->storage_id, node);
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
            nodes_by_uuid.emplace(table_id.uuid, node);
        if (!table_id.table_name.empty())
            nodes_by_database_and_table_names.emplace(table_id, node);
    }
    return node;
}


void TablesDependencyGraph::removeNode(Node * node)
{
    auto dependency_nodes = std::move(node->dependencies);
    auto dependent_nodes = std::move(node->dependents);

    if (node->storage_id.hasUUID())
        nodes_by_uuid.erase(node->storage_id.uuid);

    if (!node->storage_id.table_name.empty())
        nodes_by_database_and_table_names.erase(node->storage_id);

    for (auto * dependency_node : dependency_nodes)
        dependency_node->dependents.erase(node);

    for (auto * dependent_node : dependent_nodes)
        dependent_node->dependencies.erase(node);

    nodes.erase(node->shared_from_this());
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
        auto current = (it++)->get();
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
        addDependencies(other_node->storage_id, other.getDependencies(*other_node));
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


void TablesDependencyGraph::setNeedRecalculateLevels()
{
    levels_calculated = false;
    nodes_sorted_by_level_lazy.clear();
}


void TablesDependencyGraph::calculateLevels() const
{
    if (levels_calculated)
        return;
    levels_calculated = true;

    nodes_sorted_by_level_lazy.clear();
    nodes_sorted_by_level_lazy.reserve(nodes.size());

    std::unordered_set<const Node *> nodes_to_process;
    for (const auto & node_ptr : nodes)
        nodes_to_process.emplace(node_ptr.get());

    size_t current_level = 0;

    while (!nodes_to_process.empty())
    {
        size_t old_num_sorted = nodes_sorted_by_level_lazy.size();

        for (auto it = nodes_to_process.begin(); it != nodes_to_process.end();)
        {
            const auto * current_node = *(it++);
            bool has_dependencies = false;
            for (const auto * dependency : current_node->dependencies)
            {
                if (nodes_to_process.contains(dependency))
                    has_dependencies = true;
            }

            if (!has_dependencies)
            {
                current_node->level = current_level;
                nodes_sorted_by_level_lazy.emplace_back(current_node);
            }
        }

        if (nodes_sorted_by_level_lazy.size() == old_num_sorted)
            break;

        for (size_t i = old_num_sorted; i != nodes_sorted_by_level_lazy.size(); ++i)
            nodes_to_process.erase(nodes_sorted_by_level_lazy[i]);

        ++current_level;
    }

    for (const auto * node_with_cyclic_dependencies : nodes_to_process)
    {
        node_with_cyclic_dependencies->level = CYCLIC_LEVEL;
        nodes_sorted_by_level_lazy.emplace_back(node_with_cyclic_dependencies);
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


std::vector<std::vector<StorageID>> TablesDependencyGraph::getTablesSortedByDependencyForParallel() const
{
    std::vector<std::vector<StorageID>> res;
    std::optional<size_t> last_level;
    for (const auto * node : getNodesSortedByLevel())
    {
        if (node->level != last_level)
            res.emplace_back();
        auto & table_ids = res.back();
        table_ids.emplace_back(node->storage_id);
        last_level = node->level;
    }
    return res;
}


void TablesDependencyGraph::log() const
{
    if (empty())
    {
        LOG_TEST(getLogger(), "No tables");
        return;
    }

    for (const auto * node : getNodesSortedByLevel())
    {
        String dependencies_desc = node->dependencies.empty()
            ? "no dependencies"
            : fmt::format("{} dependencies: {}", node->dependencies.size(), fmt::join(getDependencies(*node), ", "));

        String level_desc = (node->level == CYCLIC_LEVEL) ? "cyclic" : fmt::format("level {}", node->level);

        LOG_TEST(getLogger(), "Table {} has {} ({})", node->storage_id, dependencies_desc, level_desc);
    }
}


Poco::Logger * TablesDependencyGraph::getLogger() const
{
    if (!logger)
        logger = &Poco::Logger::get(name_for_logging);
    return logger;
}

}
