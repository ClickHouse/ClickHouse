#pragma once

#include <IO/Progress.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>
#include <Common/CurrentMetrics.h>
#include <list>

namespace DB
{

class RefreshTask;
using RefreshTaskPtr = std::shared_ptr<RefreshTask>;
using RefreshTaskList = std::list<RefreshTaskPtr>;

/// Set of refreshable views
class RefreshSet
{
public:
    /// RAII thing that unregisters a task and its dependencies in destructor. Not thread safe.
    class Handle
    {
        friend class RefreshSet;
    public:
        Handle() = default;

        Handle(Handle &&) noexcept;
        Handle & operator=(Handle &&) noexcept;

        ~Handle();

        void rename(StorageID new_id, std::optional<StorageID> new_inner_table_id);
        void changeDependencies(std::vector<StorageID> deps);

        void reset();

        explicit operator bool() const { return parent_set != nullptr; }

        const StorageID & getID() const { return id; }
        const std::vector<StorageID> & getDependencies() const { return dependencies; }

    private:
        RefreshSet * parent_set = nullptr;
        StorageID id = StorageID::createEmpty();
        std::optional<StorageID> inner_table_id;
        std::vector<StorageID> dependencies;
        RefreshTaskList::iterator iter; // in parent_set->tasks[id]
        RefreshTaskList::iterator inner_table_iter; // in parent_set->inner_tables[inner_table_id]
        std::optional<CurrentMetrics::Increment> metric_increment;

        Handle(RefreshSet * parent_set_, StorageID id_, std::optional<StorageID> inner_table_id, RefreshTaskList::iterator iter_, RefreshTaskList::iterator inner_table_iter_, std::vector<StorageID> dependencies_);
    };

    RefreshSet();

    void emplace(StorageID id, std::optional<StorageID> inner_table_id, const std::vector<StorageID> & dependencies, RefreshTaskPtr task);

    /// Finds active refreshable view(s) by database and table name.
    /// Normally there's at most one, but we allow name collisions here, just in case.
    RefreshTaskList findTasks(const StorageID & id) const;
    std::vector<RefreshTaskPtr> getTasks() const;

    RefreshTaskPtr tryGetTaskForInnerTable(const StorageID & inner_table_id) const;

    /// Calls notify() on all tasks that depend on `id`.
    void notifyDependents(const StorageID & id) const;

    void setRefreshesStopped(bool stopped);
    bool refreshesStopped() const;

private:
    using TaskMap = std::unordered_map<StorageID, RefreshTaskList, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using DependentsMap = std::unordered_map<StorageID, std::unordered_set<RefreshTaskPtr>, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using InnerTableMap = std::unordered_map<StorageID, RefreshTaskList, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

    /// Protects the two maps below, not locked for any nontrivial operations (e.g. operations that
    /// block or lock other mutexes).
    mutable std::mutex mutex;

    TaskMap tasks;
    DependentsMap dependents;
    InnerTableMap inner_tables;

    std::atomic<bool> refreshes_stopped {false};

    RefreshTaskList::iterator addTaskLocked(StorageID id, RefreshTaskPtr task);
    void removeTaskLocked(StorageID id, RefreshTaskList::iterator iter);
    RefreshTaskList::iterator addInnerTableLocked(StorageID inner_table_id, RefreshTaskPtr task);
    void removeInnerTableLocked(StorageID inner_table_id, RefreshTaskList::iterator inner_table_iter);
    void addDependenciesLocked(RefreshTaskPtr task, const std::vector<StorageID> & dependencies);
    void removeDependenciesLocked(RefreshTaskPtr task, const std::vector<StorageID> & dependencies);
};

}
