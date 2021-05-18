#pragma once

#include <Core/Defines.h>
#include <DataStreams/BlockIO.h>
#include <IO/Progress.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/QueryPriorities.h>
#include <Storages/IStorage_fwd.h>
#include <Poco/Condition.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Throttler.h>

#include <condition_variable>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>


namespace CurrentMetrics
{
    extern const Metric Query;
}

namespace DB
{

struct Settings;
class IAST;

struct ProcessListForUser;
class QueryStatus;
class ThreadStatus;
class ProcessListEntry;


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
    Int64 peak_memory_usage;
    ClientInfo client_info;
    bool is_cancelled;

    /// Optional fields, filled by query
    std::vector<UInt64> thread_ids;
    std::shared_ptr<ProfileEvents::Counters> profile_counters;
    std::shared_ptr<Settings> query_settings;
    std::string current_database;
};

/// Query and information about its execution.
class QueryStatus : public WithContext
{
protected:
    friend class ProcessList;
    friend class ThreadStatus;
    friend class CurrentThread;
    friend class ProcessListEntry;

    String query;
    ClientInfo client_info;

    /// Info about all threads involved in query execution
    ThreadGroupStatusPtr thread_group;

    Stopwatch watch;

    /// Progress of input stream
    Progress progress_in;
    /// Progress of output stream
    Progress progress_out;

    QueryPriorities::Handle priority_handle;

    CurrentMetrics::Increment num_queries_increment{CurrentMetrics::Query};

    std::atomic<bool> is_killed { false };

    void setUserProcessList(ProcessListForUser * user_process_list_);
    /// Be careful using it. For example, queries field of ProcessListForUser could be modified concurrently.
    const ProcessListForUser * getUserProcessList() const { return user_process_list; }

    mutable std::mutex query_streams_mutex;

    /// Streams with query results, point to BlockIO from executeQuery()
    /// This declaration is compatible with notes about BlockIO::process_list_entry:
    ///  there are no cyclic dependencies: BlockIO::in,out point to objects inside ProcessListElement (not whole object)
    BlockInputStreamPtr query_stream_in;
    BlockOutputStreamPtr query_stream_out;

    enum QueryStreamsStatus
    {
        NotInitialized,
        Initialized,
        Released
    };

    QueryStreamsStatus query_streams_status{NotInitialized};

    ProcessListForUser * user_process_list = nullptr;

public:

    QueryStatus(
        ContextPtr context_,
        const String & query_,
        const ClientInfo & client_info_,
        QueryPriorities::Handle && priority_handle_);

    ~QueryStatus();

    const ClientInfo & getClientInfo() const
    {
        return client_info;
    }

    ProgressValues getProgressIn() const
    {
        return progress_in.getValues();
    }

    ProgressValues getProgressOut() const
    {
        return progress_out.getValues();
    }

    ThrottlerPtr getUserNetworkThrottler();

    bool updateProgressIn(const Progress & value)
    {
        CurrentThread::updateProgressIn(value);
        progress_in.incrementPiecewiseAtomically(value);

        if (priority_handle)
            priority_handle->waitIfNeed(std::chrono::seconds(1));        /// NOTE Could make timeout customizable.

        return !is_killed.load(std::memory_order_relaxed);
    }

    bool updateProgressOut(const Progress & value)
    {
        CurrentThread::updateProgressOut(value);
        progress_out.incrementPiecewiseAtomically(value);

        return !is_killed.load(std::memory_order_relaxed);
    }

    QueryStatusInfo getInfo(bool get_thread_list = false, bool get_profile_events = false, bool get_settings = false) const;

    /// Copies pointers to in/out streams
    void setQueryStreams(const BlockIO & io);

    /// Frees in/out streams
    void releaseQueryStreams();

    /// It means that ProcessListEntry still exists, but stream was already destroyed
    bool streamsAreReleased();

    /// Get query in/out pointers from BlockIO
    bool tryGetQueryStreams(BlockInputStreamPtr & in, BlockOutputStreamPtr & out) const;

    CancellationCode cancelQuery(bool kill);

    bool isKilled() const { return is_killed; }
};


/// Information of process list for user.
struct ProcessListForUserInfo
{
    Int64 memory_usage;
    Int64 peak_memory_usage;

    // Optional field, filled by request.
    std::shared_ptr<ProfileEvents::Counters> profile_counters;
};


/// Data about queries for one user.
struct ProcessListForUser
{
    ProcessListForUser();

    /// query_id -> ProcessListElement(s). There can be multiple queries with the same query_id as long as all queries except one are cancelled.
    using QueryToElement = std::unordered_map<String, QueryStatus *>;
    QueryToElement queries;

    ProfileEvents::Counters user_performance_counters{VariableContext::User, &ProfileEvents::global_counters};
    /// Limit and counter for memory of all simultaneously running queries of single user.
    MemoryTracker user_memory_tracker{VariableContext::User};

    /// Count network usage for all simultaneously running queries of single user.
    ThrottlerPtr user_throttler;

    ProcessListForUserInfo getInfo(bool get_profile_events = false) const;

    /// Clears MemoryTracker for the user.
    /// Sometimes it is important to reset the MemoryTracker, because it may accumulate skew
    ///  due to the fact that there are cases when memory can be allocated while processing the query, but released later.
    /// Clears network bandwidth Throttler, so it will not count periods of inactivity.
    void resetTrackers()
    {
        user_memory_tracker.reset();
        if (user_throttler)
            user_throttler.reset();
    }
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
public:
    using Element = QueryStatus;
    using Entry = ProcessListEntry;

    /// list, for iterators not to invalidate. NOTE: could replace with cyclic buffer, but not worth.
    using Container = std::list<Element>;
    using Info = std::vector<QueryStatusInfo>;
    using UserInfo = std::unordered_map<String, ProcessListForUserInfo>;

    /// User -> queries
    using UserToQueries = std::unordered_map<String, ProcessListForUser>;

protected:
    friend class ProcessListEntry;

    mutable std::mutex mutex;
    mutable std::condition_variable have_space;        /// Number of currently running queries has become less than maximum.

    /// List of queries
    Container processes;
    size_t max_size = 0;        /// 0 means no limit. Otherwise, when limit exceeded, an exception is thrown.

    /// Stores per-user info: queries, statistics and limits
    UserToQueries user_to_queries;

    /// Stores info about queries grouped by their priority
    QueryPriorities priorities;

    /// Limit network bandwidth for all users
    ThrottlerPtr total_network_throttler;

    /// Call under lock. Finds process with specified current_user and current_query_id.
    QueryStatus * tryGetProcessListElement(const String & current_query_id, const String & current_user);

public:
    using EntryPtr = std::shared_ptr<ProcessListEntry>;

    /** Register running query. Returns refcounted object, that will remove element from list in destructor.
      * If too many running queries - wait for not more than specified (see settings) amount of time.
      * If timeout is passed - throw an exception.
      * Don't count KILL QUERY queries.
      */
    EntryPtr insert(const String & query_, const IAST * ast, ContextPtr query_context);

    /// Number of currently executing queries.
    size_t size() const { return processes.size(); }

    /// Get current state of process list.
    Info getInfo(bool get_thread_list = false, bool get_profile_events = false, bool get_settings = false) const;

    /// Get current state of process list per user.
    UserInfo getUserInfo(bool get_profile_events = false) const;

    void setMaxSize(size_t max_size_)
    {
        std::lock_guard lock(mutex);
        max_size = max_size_;
    }

    /// Try call cancel() for input and output streams of query with specified id and user
    CancellationCode sendCancelToQuery(const String & current_query_id, const String & current_user, bool kill = false);

    void killAllQueries();
};

}
