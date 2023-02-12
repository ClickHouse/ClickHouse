#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>
#include <Storages/MaterializedView/RefreshTask_fwd.h>

#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric Refresh;
}

namespace DB 
{

struct RefreshInfo 
{
    String database;
    String view_name;
    String refresh_status;
    String last_refresh_status;
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
    RefreshSetElement(RefreshTaskHolder task, StorageID id);

    RefreshSetElement(const RefreshSetElement &) = delete;
    RefreshSetElement & operator=(const RefreshSetElement &) = delete;

    RefreshInfo getInfo() const;

    RefreshTaskHolder getTask() const;

    const StorageID & getID() const;

private:
    RefreshTaskObserver corresponding_task;
    StorageID view_id;

    mutable std::atomic<UInt64> read_rows{0};
    mutable std::atomic<UInt64> read_bytes{0};
    mutable std::atomic<UInt64> total_rows_to_read{0};
    mutable std::atomic<UInt64> total_bytes_to_read{0};
    mutable std::atomic<UInt64> written_rows{0};
    mutable std::atomic<UInt64> written_bytes{0};
    mutable std::atomic<UInt64> result_rows{0};
    mutable std::atomic<UInt64> result_bytes{0};
    mutable std::atomic<UInt64> elapsed_ns{0};
    mutable std::atomic<Int64> last_s{0};
    mutable std::atomic<Int64> next_s{0};
    mutable std::atomic<RefreshTaskStateUnderlying> state{0};
    mutable std::atomic<RefreshTaskStateUnderlying> last_state{0};
};

struct RefreshSetLess
{
    using is_transparent = std::true_type;

    bool operator()(const RefreshSetElement & l, const RefreshSetElement & r) const;
    bool operator()(const StorageID & l, const RefreshSetElement & r) const;
    bool operator()(const RefreshSetElement & l, const StorageID & r) const;
    bool operator()(const StorageID & l, const StorageID & r) const;
};

/// Set of refreshable views
class RefreshSet
{
private:
    using Container = std::set<RefreshSetElement, RefreshSetLess>;
    using ContainerIter = typename Container::iterator;

public:
    class Entry
    {
        friend class RefreshSet;
    public:
        Entry();

        Entry(Entry &&) noexcept;
        Entry & operator=(Entry &&) noexcept;

        ~Entry();

        const RefreshSetElement * operator->() const { return std::to_address(iter); }

    private:
        RefreshSet * parent_set;
        ContainerIter iter;
        std::optional<CurrentMetrics::Increment> metric_increment;

        Entry(
            RefreshSet & set,
            ContainerIter it,
            const CurrentMetrics::Metric & metric);

        void cleanup(RefreshSet * set);
    };

    using InfoContainer = std::vector<RefreshInfo>;

    RefreshSet();

    template <typename... Args>
    std::optional<Entry> emplace(Args &&... args)
    {
        std::lock_guard guard(elements_mutex);
        if (auto [it, is_inserted] = elements.emplace(std::forward<Args>(args)...); is_inserted)
            return Entry(*this, std::move(it), set_metric);
        return {};
    }

    RefreshTaskHolder getTask(const StorageID & id) const;

    InfoContainer getInfo() const;

private:
    mutable std::mutex elements_mutex;
    Container elements;
    CurrentMetrics::Metric set_metric;

    void erase(ContainerIter it);
};

using RefreshSetEntry = RefreshSet::Entry;

}
