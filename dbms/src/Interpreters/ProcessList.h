#pragma once

#include <map>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <shared_mutex>

#include <Poco/Condition.h>
#include <Core/Defines.h>
#include <IO/Progress.h>
#include <Common/Stopwatch.h>
#include <Common/MemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/Throttler.h>
#include <Interpreters/QueryPriorities.h>
#include <Interpreters/ClientInfo.h>
#include <Common/ThreadStatus.h>
#include <DataStreams/BlockIO.h>
#include "ThreadPerformanceProfile.h"


namespace CurrentMetrics
{
    extern const Metric Query;
}

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;
using Tables = std::map<String, StoragePtr>;
struct Settings;
class IAST;

struct ProcessListForUser;
struct QueryStatus;
struct ThreadStatus;


/** List of currently executing queries.
  * Also implements limit on their number.
  */

/** Information of process list element.
  * To output in SHOW PROCESSLIST query. Does not contain any complex objects, that do something on copy or destructor.
  */
struct QueryStatusInfo
{
    String query;
    double elapsed_seconds;
    size_t read_rows;
    size_t read_bytes;
    size_t total_rows;
    size_t written_rows;
    size_t written_bytes;
    Int64 memory_usage;
    ClientInfo client_info;

    /// Optional fields, filled by request
    std::vector<UInt32> thread_numbers;
    std::unique_ptr<ProfileEvents::Counters> profile_counters;
};


/// Query and information about its execution.
struct QueryStatus
{
    String query;
    ClientInfo client_info;

    Stopwatch watch;

    /// Progress of input stream
    Progress progress_in;
    /// Progress of output stream
    Progress progress_out;

    QueryPriorities::Handle priority_handle;

    ProfileEvents::Counters performance_counters;
    MemoryTracker memory_tracker;

    mutable std::shared_mutex threads_mutex;
    /// Key is Poco's thread_id
    using QueryThreadStatuses = std::map<int, ThreadStatusPtr>;
    QueryThreadStatuses thread_statuses;

    CurrentMetrics::Increment num_queries_increment{CurrentMetrics::Query};

    bool is_cancelled = false;

    /// Temporary tables could be registered here. Modify under mutex.
    Tables temporary_tables;

    void setUserProcessList(ProcessListForUser * user_process_list_);
    /// Be careful using it. For example, queries field of ProcessListForUser could be modified concurrently.
    const ProcessListForUser * getUserProcessList() const { return user_process_list; }

protected:

    mutable std::mutex query_streams_mutex;

    /// Streams with query results, point to BlockIO from executeQuery()
    /// This declaration is compatible with notes about BlockIO::process_list_entry:
    ///  there are no cyclic dependencies: BlockIO::in,out point to objects inside ProcessListElement (not whole object)
    BlockInputStreamPtr query_stream_in;
    BlockOutputStreamPtr query_stream_out;

    bool query_streams_initialized{false};
    bool query_streams_released{false};

    ProcessListForUser * user_process_list = nullptr;

public:

    QueryStatus(
        const String & query_,
        const ClientInfo & client_info_,
        size_t max_memory_usage,
        double memory_tracker_fault_probability,
        QueryPriorities::Handle && priority_handle_);

    ~QueryStatus()
    {
        // TODO: master thread should be reset
    }

    bool updateProgressIn(const Progress & value)
    {
        progress_in.incrementPiecewiseAtomically(value);

        if (priority_handle)
            priority_handle->waitIfNeed(std::chrono::seconds(1));        /// NOTE Could make timeout customizable.

        return !is_cancelled;
    }

    bool updateProgressOut(const Progress & value)
    {
        progress_out.incrementPiecewiseAtomically(value);
        return !is_cancelled;
    }


    QueryStatusInfo getInfo() const
    {
        QueryStatusInfo res;

        res.query             = query;
        res.client_info       = client_info;
        res.elapsed_seconds   = watch.elapsedSeconds();
        res.read_rows         = progress_in.rows;
        res.read_bytes        = progress_in.bytes;
        res.total_rows        = progress_in.total_rows;
        res.written_rows      = progress_out.rows;
        res.written_bytes     = progress_out.bytes;
        res.memory_usage      = memory_tracker.get();

        return res;
    }

    /// Copies pointers to in/out streams
    void setQueryStreams(const BlockIO & io);

    /// Frees in/out streams
    void releaseQueryStreams();

    /// It means that ProcessListEntry still exists, but stream was already destroyed
    bool streamsAreReleased();

    /// Get query in/out pointers from BlockIO
    bool tryGetQueryStreams(BlockInputStreamPtr & in, BlockOutputStreamPtr & out) const;
};


/// Data about queries for one user.
struct ProcessListForUser
{
    ProcessListForUser();

    /// Query_id -> ProcessListElement *
    using QueryToElement = std::unordered_map<String, QueryStatus *>;
    QueryToElement queries;

    ProfileEvents::Counters user_performance_counters;
    /// Limit and counter for memory of all simultaneously running queries of single user.
    MemoryTracker user_memory_tracker;

    /// Count network usage for all simultaneously running queries of single user.
    ThrottlerPtr user_throttler;
};


class ProcessList;


/// Keeps iterator to process list and removes element in destructor.
class ProcessListEntry
{
private:
    using Container = std::list<QueryStatus>;

    ProcessList & parent;
    Container::iterator it;
public:
    ProcessListEntry(ProcessList & parent_, Container::iterator it_)
        : parent(parent_), it(it_) {}

    ~ProcessListEntry();

    QueryStatus * operator->() { return &*it; }
    const QueryStatus * operator->() const { return &*it; }

    QueryStatus & get() { return *it; }
    const QueryStatus & get() const { return *it; }
};


class ProcessList
{
    friend class ProcessListEntry;
public:
    using Element = QueryStatus;
    using Entry = ProcessListEntry;

    /// list, for iterators not to invalidate. NOTE: could replace with cyclic buffer, but not worth.
    using Container = std::list<Element>;
    using Info = std::vector<QueryStatusInfo>;
    /// User -> queries
    using UserToQueries = std::unordered_map<String, ProcessListForUser>;

private:
    mutable std::mutex mutex;
    mutable Poco::Condition have_space;        /// Number of currently running queries has become less than maximum.

    /// List of queries
    Container processes;
    size_t max_size;        /// 0 means no limit. Otherwise, when limit exceeded, an exception is thrown.

    /// Stores per-user info: queries, statistics and limits
    UserToQueries user_to_queries;

    /// Stores info about queries grouped by their priority
    QueryPriorities priorities;

    /// Limit and counter for memory of all simultaneously running queries.
    MemoryTracker total_memory_tracker;

    /// Call under lock. Finds process with specified current_user and current_query_id.
    QueryStatus * tryGetProcessListElement(const String & current_query_id, const String & current_user);

public:
    ProcessList(size_t max_size_ = 0) : max_size(max_size_) {}

    using EntryPtr = std::shared_ptr<ProcessListEntry>;

    /** Register running query. Returns refcounted object, that will remove element from list in destructor.
      * If too many running queries - wait for not more than specified (see settings) amount of time.
      * If timeout is passed - throw an exception.
      * Don't count KILL QUERY queries.
      */
    EntryPtr insert(const String & query_, const IAST * ast, const ClientInfo & client_info, const Settings & settings);

    /// Number of currently executing queries.
    size_t size() const { return processes.size(); }

    /// Get current state of process list.
    Info getInfo(bool get_thread_list = false, bool get_profile_events = false) const
    {
        Info per_query_infos;

        std::lock_guard<std::mutex> lock(mutex);

        per_query_infos.reserve(processes.size());
        for (const auto & process : processes)
        {
            per_query_infos.emplace_back(process.getInfo());
            QueryStatusInfo & current_info = per_query_infos.back();

            if (get_thread_list)
            {
                std::lock_guard lock(process.threads_mutex);
                current_info.thread_numbers.reserve(process.thread_statuses.size());

                for (auto & thread_status_elem : process.thread_statuses)
                    current_info.thread_numbers.emplace_back(thread_status_elem.second->poco_thread_number);
            }

            if (get_profile_events)
            {
                current_info.profile_counters = std::make_unique<ProfileEvents::Counters>(ProfileEvents::Level::Process);
                process.performance_counters.getPartiallyAtomicSnapshot(*current_info.profile_counters);
            }
        }

        return per_query_infos;
    }

    void setMaxSize(size_t max_size_)
    {
        std::lock_guard<std::mutex> lock(mutex);
        max_size = max_size_;
    }

    /// Register temporary table. Then it is accessible by query_id and name.
    void addTemporaryTable(QueryStatus & elem, const String & table_name, const StoragePtr & storage);

    enum class CancellationCode
    {
        NotFound = 0,                     /// already cancelled
        QueryIsNotInitializedYet = 1,
        CancelCannotBeSent = 2,
        CancelSent = 3,
        Unknown
    };

    /// Try call cancel() for input and output streams of query with specified id and user
    CancellationCode sendCancelToQuery(const String & current_query_id, const String & current_user, bool kill = false);
};

}
