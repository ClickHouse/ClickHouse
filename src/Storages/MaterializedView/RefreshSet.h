#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>
#include <Storages/MaterializedView/RefreshTask_fwd.h>

#include <Common/CurrentMetrics.h>

namespace DB
{

using DatabaseAndTableNameSet = std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

struct RefreshInfo
{
    String database;
    String view_name;
    String refresh_status;
    String last_refresh_result;
    UInt32 last_refresh_time;
    UInt32 next_refresh_time;
    Float64 progress;
    Float64 elapsed_ns;
    UInt64 read_rows;
    UInt64 read_bytes;
    UInt64 total_rows_to_read;
    UInt64 total_bytes_to_read;
    UInt64 written_rows;
    UInt64 written_bytes;
    UInt64 result_rows;
    UInt64 result_bytes;
};

class RefreshSetElement
{
    friend class RefreshTask;
public:
    RefreshSetElement(StorageID id, std::vector<StorageID> deps, RefreshTaskHolder task);

    RefreshSetElement(const RefreshSetElement &) = delete;
    RefreshSetElement & operator=(const RefreshSetElement &) = delete;

    RefreshInfo getInfo() const;

    RefreshTaskHolder getTask() const;
    const StorageID & getID() const;
    const std::vector<StorageID> & getDependencies() const;

private:
    RefreshTaskObserver corresponding_task;
    StorageID view_id;
    std::vector<StorageID> dependencies;

    std::atomic<UInt64> read_rows{0};
    std::atomic<UInt64> read_bytes{0};
    std::atomic<UInt64> total_rows_to_read{0};
    std::atomic<UInt64> total_bytes_to_read{0};
    std::atomic<UInt64> written_rows{0};
    std::atomic<UInt64> written_bytes{0};
    std::atomic<UInt64> result_rows{0};
    std::atomic<UInt64> result_bytes{0};
    std::atomic<UInt64> elapsed_ns{0};
    std::atomic<Int64> last_s{0};
    std::atomic<Int64> next_s{0};
    std::atomic<RefreshTaskStateUnderlying> state{0};
    std::atomic<RefreshTaskStateUnderlying> last_result{0};
};

/// Set of refreshable views
class RefreshSet
{
private:
    using ElementMap = std::unordered_map<StorageID, RefreshSetElement, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using ElementMapIter = typename ElementMap::iterator;

public:
    class Entry
    {
        friend class RefreshSet;
    public:
        Entry() = default;

        Entry(Entry &&) noexcept;
        Entry & operator=(Entry &&) noexcept;

        ~Entry();

        explicit operator bool() const { return parent_set != nullptr; }
        RefreshSetElement * operator->() { chassert(parent_set); return &iter->second; }

        void reset();

    private:
        RefreshSet * parent_set = nullptr;
        ElementMapIter iter;
        std::optional<CurrentMetrics::Increment> metric_increment;

        Entry(RefreshSet & set, ElementMapIter it);
    };

    using InfoContainer = std::vector<RefreshInfo>;

    RefreshSet();

    Entry emplace(StorageID id, std::vector<StorageID> dependencies, RefreshTaskHolder task);

    RefreshTaskHolder getTask(const StorageID & id) const;

    InfoContainer getInfo() const;

    /// Get tasks that depend on the given one.
    std::vector<RefreshTaskHolder> getDependents(const StorageID & id) const;

private:
    using DependentsMap = std::unordered_map<StorageID, DatabaseAndTableNameSet, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

    /// Protects the two maps below, not locked for any nontrivial operations (e.g. operations that
    /// block or lock other mutexes).
    mutable std::mutex mutex;

    ElementMap elements;
    DependentsMap dependents;

    void erase(ElementMapIter it);
};

using RefreshSetEntry = RefreshSet::Entry;

}
