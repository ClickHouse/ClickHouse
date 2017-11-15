#pragma once

#include <map>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <Poco/Condition.h>
#include <Common/Stopwatch.h>
#include <Core/Defines.h>
#include <Core/Progress.h>
#include <Common/MemoryTracker.h>
#include <Interpreters/QueryPriorities.h>
#include <Interpreters/ClientInfo.h>
#include <Common/CurrentMetrics.h>
#include <DataStreams/BlockIO.h>
#include <Common/Throttler.h>


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


/** List of currently executing queries.
  * Also implements limit on their number.
  */

/** Information of process list element.
  * To output in SHOW PROCESSLIST query. Does not contain any complex objects, that do something on copy or destructor.
  */
struct ProcessInfo
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
};


/// Query and information about its execution.
struct ProcessListElement
{
    String query;
    ClientInfo client_info;

    Stopwatch watch;

    /// Progress of input stream
    Progress progress_in;
    /// Progress of output stream
    Progress progress_out;

    MemoryTracker memory_tracker;

    QueryPriorities::Handle priority_handle;

    CurrentMetrics::Increment num_queries {CurrentMetrics::Query};

    bool is_cancelled = false;

    /// Temporary tables could be registered here. Modify under mutex.
    Tables temporary_tables;

    /// Be careful using it. For example, queries field could be modified concurrently.
    const ProcessListForUser * user_process_list = nullptr;

protected:

    mutable std::mutex query_streams_mutex;

    /// Streams with query results, point to BlockIO from executeQuery()
    /// This declaration is compatible with notes about BlockIO::process_list_entry:
    ///  there are no cyclic dependencies: BlockIO::in,out point to objects inside ProcessListElement (not whole object)
    BlockInputStreamPtr query_stream_in;
    BlockOutputStreamPtr query_stream_out;

    bool query_streams_initialized{false};
    bool query_streams_released{false};

public:

    ProcessListElement(
        const String & query_,
        const ClientInfo & client_info_,
        size_t max_memory_usage,
        double memory_tracker_fault_probability,
        QueryPriorities::Handle && priority_handle_)
        : query(query_), client_info(client_info_), memory_tracker(max_memory_usage),
        priority_handle(std::move(priority_handle_))
    {
        memory_tracker.setDescription("(for query)");
        current_memory_tracker = &memory_tracker;

        if (memory_tracker_fault_probability)
            memory_tracker.setFaultProbability(memory_tracker_fault_probability);
    }

    ~ProcessListElement()
    {
        current_memory_tracker = nullptr;
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


    ProcessInfo getInfo() const
    {
        ProcessInfo res;

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
    /// Query_id -> ProcessListElement *
    using QueryToElement = std::unordered_map<String, ProcessListElement *>;
    QueryToElement queries;

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
    using Container = std::list<ProcessListElement>;

    ProcessList & parent;
    Container::iterator it;
public:
    ProcessListEntry(ProcessList & parent_, Container::iterator it_)
        : parent(parent_), it(it_) {}

    ~ProcessListEntry();

    ProcessListElement * operator->() { return &*it; }
    const ProcessListElement * operator->() const { return &*it; }

    ProcessListElement & get() { return *it; }
    const ProcessListElement & get() const { return *it; }
};


class ProcessList
{
    friend class ProcessListEntry;
public:
    using Element = ProcessListElement;
    using Entry = ProcessListEntry;

    /// list, for iterators not to invalidate. NOTE: could replace with cyclic buffer, but not worth.
    using Container = std::list<Element>;
    using Info = std::vector<ProcessInfo>;
    /// User -> queries
    using UserToQueries = std::unordered_map<String, ProcessListForUser>;

private:
    mutable std::mutex mutex;
    mutable Poco::Condition have_space;        /// Number of currently running queries has become less than maximum.

    /// List of queries
    Container cont;
    size_t cur_size;        /// In C++03 or C++11 and old ABI, std::list::size is not O(1).
    size_t max_size;        /// 0 means no limit. Otherwise, when limit exceeded, an exception is thrown.

    /// Stores per-user info: queries, statistics and limits
    UserToQueries user_to_queries;

    /// Stores info about queries grouped by their priority
    QueryPriorities priorities;

    /// Limit and counter for memory of all simultaneously running queries.
    MemoryTracker total_memory_tracker;

    /// Call under lock. Finds process with specified current_user and current_query_id.
    ProcessListElement * tryGetProcessListElement(const String & current_query_id, const String & current_user);

public:
    ProcessList(size_t max_size_ = 0) : cur_size(0), max_size(max_size_) {}

    using EntryPtr = std::shared_ptr<ProcessListEntry>;

    /** Register running query. Returns refcounted object, that will remove element from list in destructor.
      * If too much running queries - wait for not more than specified (see settings) amount of time.
      * If timeout is passed - throw an exception.
      * Don't count KILL QUERY queries.
      */
    EntryPtr insert(const String & query_, const IAST * ast, const ClientInfo & client_info, const Settings & settings);

    /// Number of currently executing queries.
    size_t size() const { return cur_size; }

    /// Get current state of process list.
    Info getInfo() const
    {
        std::lock_guard<std::mutex> lock(mutex);

        Info res;
        res.reserve(cur_size);
        for (const auto & elem : cont)
            res.emplace_back(elem.getInfo());

        return res;
    }

    void setMaxSize(size_t max_size_)
    {
        std::lock_guard<std::mutex> lock(mutex);
        max_size = max_size_;
    }

    /// Register temporary table. Then it is accessible by query_id and name.
    void addTemporaryTable(ProcessListElement & elem, const String & table_name, const StoragePtr & storage);

    enum class CancellationCode
    {
        NotFound = 0,                     /// already cancelled
        QueryIsNotInitializedYet = 1,
        CancelCannotBeSent = 2,
        CancelSent = 3,
        Unknown
    };

    /// Try call cancel() for input and output streams of query with specified id and user
    CancellationCode sendCancelToQuery(const String & current_query_id, const String & current_user);
};

}
