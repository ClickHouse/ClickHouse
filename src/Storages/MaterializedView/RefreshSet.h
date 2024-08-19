#pragma once

#include <IO/Progress.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>
#include <Storages/MaterializedView/RefreshTask_fwd.h>
#include <Common/CurrentMetrics.h>
#include <list>

namespace DB
{

enum class RefreshState
{
    Disabled = 0,
    Scheduled,
    WaitingForDependencies,
    Running,
};

enum class LastRefreshResult
{
    Unknown = 0,
    Cancelled,
    Error,
    Finished
};

struct RefreshInfo
{
    StorageID view_id = StorageID::createEmpty();
    RefreshState state = RefreshState::Scheduled;
    LastRefreshResult last_refresh_result = LastRefreshResult::Unknown;
    std::optional<UInt32> last_attempt_time;
    std::optional<UInt32> last_success_time;
    UInt64 last_attempt_duration_ms = 0;
    UInt32 next_refresh_time = 0;
    UInt64 refresh_count = 0;
    UInt64 retry = 0;
    String exception_message; // if last_refresh_result is Error
    std::vector<StorageID> remaining_dependencies;
    ProgressValues progress;
};

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
        RefreshTaskList::iterator iter; // in parent_set->tasks[id]
        std::optional<CurrentMetrics::Increment> metric_increment;

        Handle(RefreshSet * parent_set_, StorageID id_, RefreshTaskList::iterator iter_, std::vector<StorageID> dependencies_);
    };

    using InfoContainer = std::vector<RefreshInfo>;

    RefreshSet();

    void emplace(StorageID id, const std::vector<StorageID> & dependencies, RefreshTaskHolder task);

    /// Finds active refreshable view(s) by database and table name.
    /// Normally there's at most one, but we allow name collisions here, just in case.
    RefreshTaskList findTasks(const StorageID & id) const;

    InfoContainer getInfo() const;

    /// Get tasks that depend on the given one.
    std::vector<RefreshTaskHolder> getDependents(const StorageID & id) const;

private:
    using TaskMap = std::unordered_map<StorageID, RefreshTaskList, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using DependentsMap = std::unordered_map<StorageID, std::unordered_set<RefreshTaskHolder>, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

    /// Protects the two maps below, not locked for any nontrivial operations (e.g. operations that
    /// block or lock other mutexes).
    mutable std::mutex mutex;

    TaskMap tasks;
    DependentsMap dependents;

    RefreshTaskList::iterator addTaskLocked(StorageID id, RefreshTaskHolder task);
    void removeTaskLocked(StorageID id, RefreshTaskList::iterator iter);
    void addDependenciesLocked(RefreshTaskHolder task, const std::vector<StorageID> & dependencies);
    void removeDependenciesLocked(RefreshTaskHolder task, const std::vector<StorageID> & dependencies);
};

}
