#pragma once
#include <Common/CurrentMetrics.h>
#include <boost/noncopyable.hpp>
#include <Storages/MergeTree/BackgroundProcessList.h>
#include <Common/Stopwatch.h>
#include <Poco/URI.h>


namespace CurrentMetrics
{
    extern const Metric ReplicatedFetch;
}

namespace DB
{

struct ReplicatedFetchInfo
{
    std::string database;
    std::string table;
    std::string partition_id;

    std::string result_part_name;
    std::string result_part_path;

    std::string source_replica_path;
    std::string source_replica_hostname;
    UInt16 source_replica_port{};
    std::string interserver_scheme;
    std::string uri;

    UInt8 to_detached{};

    Float64 elapsed{};
    Float64 progress{};

    UInt64 total_size_bytes_compressed{};
    UInt64 bytes_read_compressed{};

    UInt64 thread_id{};
};


struct ReplicatedFetchListElement : private boost::noncopyable
{
    const std::string database;
    const std::string table;
    const std::string partition_id;

    const std::string result_part_name;
    const std::string result_part_path;

    const std::string source_replica_path;
    const std::string source_replica_hostname;
    const UInt16 source_replica_port;
    const std::string interserver_scheme;
    const std::string uri;

    const UInt8 to_detached;

    Stopwatch watch;
    std::atomic<Float64> progress{};
    /// How many bytes already read
    std::atomic<UInt64> bytes_read_compressed{};
    /// Total bytes to read
    /// NOTE: can be zero if we fetching data from old server.
    /// In this case progress is not tracked.
    const UInt64 total_size_bytes_compressed{};

    const UInt64 thread_id;

    /// Set on server shutdown. Checked in `ReplicatedFetchReadCallback` on every buffer
    /// refill of the part download, which aborts with the `ABORTED` exception.
    std::atomic<bool> is_cancelled{};

    ReplicatedFetchListElement(
        const std::string & database_, const std::string & table_,
        const std::string & partition_id_, const std::string & result_part_name_,
        const std::string & result_part_path_, const std::string & source_replica_path_,
        const Poco::URI & uri, UInt8 to_detached_, UInt64 total_size_bytes_compressed_);

    ReplicatedFetchInfo getInfo() const;
};


using ReplicatedFetchListEntry = BackgroundProcessListEntry<ReplicatedFetchListElement, ReplicatedFetchInfo>;

/// List of currently processing replicated fetches
class ReplicatedFetchList final : public BackgroundProcessList<ReplicatedFetchListElement, ReplicatedFetchInfo>
{
private:
    using Parent = BackgroundProcessList<ReplicatedFetchListElement, ReplicatedFetchInfo>;
    /// Set by cancelAll (on server shutdown): entries inserted after it are cancelled at birth.
    std::atomic<bool> all_cancelled = false;

public:
    ReplicatedFetchList ()
        : Parent(CurrentMetrics::ReplicatedFetch)
    {}

    /// Whether `cancelAll` has been called (the server is shutting down).
    /// Checked in `fetchSelectedPart` while reading the part header, before the fetch
    /// is registered in the list and gets its own per-entry `is_cancelled` flag.
    bool isAllCancelled() const { return all_cancelled; }

    /// Cancel all current fetches, and also all inserted later.
    /// Used on server shutdown, when their results would be discarded anyway.
    void cancelAll()
    {
        /// See the comment in `MergeList::cancelAll` about the ordering with `insert`.
        all_cancelled = true;
        std::lock_guard lock{mutex};
        for (auto & fetch_element : entries)
            fetch_element.is_cancelled = true;
    }

    template <typename... Args>
    EntryPtr insert(Args &&... args)
    {
        auto entry = Parent::insert(std::forward<Args>(args)...);
        if (all_cancelled)
            (*entry)->is_cancelled = true;
        return entry;
    }
};

}
