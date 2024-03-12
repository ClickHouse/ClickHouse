#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>

#include <Common/CurrentMetrics.h>

namespace DB
{

class RefreshTask;
using RefreshTaskPtr = std::shared_ptr<RefreshTask>;
using DatabaseAndTableNameSet = std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

/// Set of refreshable views
class RefreshSet
{
public:
    /// RAII thing that unregisters a task and its dependencies in destructor.
    /// Storage IDs must be unique. Not thread safe.
    class Handle
    {
        friend class RefreshSet;
    public:
        Handle() = default;

        Handle(Handle &&) noexcept;
        Handle & operator=(Handle &&) noexcept;

        ~Handle();

        void rename(StorageID new_id);
        void changeDependencies(std::vector<StorageID> deps);

        void reset();

        explicit operator bool() const { return parent_set != nullptr; }

        const StorageID & getID() const { return id; }
        const std::vector<StorageID> & getDependencies() const { return dependencies; }

    private:
        RefreshSet * parent_set = nullptr;
        StorageID id = StorageID::createEmpty();
        std::vector<StorageID> dependencies;
        std::optional<CurrentMetrics::Increment> metric_increment;

        Handle(RefreshSet * parent_set_, StorageID id_, std::vector<StorageID> dependencies_);
    };

    RefreshSet();

    Handle emplace(StorageID id, const std::vector<StorageID> & dependencies, RefreshTaskPtr task);

    RefreshTaskPtr getTask(const StorageID & id) const;
    std::vector<RefreshTaskPtr> getTasks() const;

    /// Calls notifyDependencyProgress() on all tasks that depend on `id`.
    void notifyDependents(const StorageID & id) const;

private:
    using TaskMap = std::unordered_map<StorageID, RefreshTaskPtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using DependentsMap = std::unordered_map<StorageID, DatabaseAndTableNameSet, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

    /// Protects the two maps below, not locked for any nontrivial operations (e.g. operations that
    /// block or lock other mutexes).
    mutable std::mutex mutex;

    TaskMap tasks;
    DependentsMap dependents;

    void addDependenciesLocked(const StorageID & id, const std::vector<StorageID> & dependencies);
    void removeDependenciesLocked(const StorageID & id, const std::vector<StorageID> & dependencies);
};

}
