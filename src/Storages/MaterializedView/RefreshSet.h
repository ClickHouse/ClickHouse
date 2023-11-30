#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>
#include <Storages/MaterializedView/RefreshTask_fwd.h>

#include <Common/CurrentMetrics.h>

namespace DB
{

using DatabaseAndTableNameSet = std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

enum class RefreshState : RefreshTaskStateUnderlying
{
    Disabled = 0,
    Scheduled,
    WaitingForDependencies,
    Running,
    Paused
};

enum class LastRefreshResult : RefreshTaskStateUnderlying
{
    Unknown = 0,
    Canceled,
    Exception,
    Finished
};

struct RefreshInfo
{
    StorageID view_id = StorageID::createEmpty();
    RefreshState state = RefreshState::Scheduled;
    LastRefreshResult last_refresh_result = LastRefreshResult::Unknown;
    std::optional<UInt32> last_refresh_time;
    UInt32 next_refresh_time = 0;
    String exception_message; // if last_refresh_result is Exception
    std::vector<StorageID> remaining_dependencies;
    ProgressValues progress;
};

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

    using InfoContainer = std::vector<RefreshInfo>;

    RefreshSet();

    Handle emplace(StorageID id, std::vector<StorageID> dependencies, RefreshTaskHolder task);

    RefreshTaskHolder getTask(const StorageID & id) const;

    InfoContainer getInfo() const;

    /// Get tasks that depend on the given one.
    std::vector<RefreshTaskHolder> getDependents(const StorageID & id) const;

private:
    using TaskMap = std::unordered_map<StorageID, RefreshTaskHolder, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
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
