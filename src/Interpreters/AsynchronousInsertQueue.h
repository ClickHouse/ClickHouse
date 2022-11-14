#pragma once

#include <Parsers/IAST_fwd.h>
#include <Common/ThreadPool.h>
#include <Core/Settings.h>
#include <Poco/Logger.h>
#include <future>

namespace DB
{

/// A queue, that stores data for insert queries and periodically flushes it to tables.
/// The data is grouped by table, format and settings of insert query.
class AsynchronousInsertQueue : public WithContext
{
public:
    using Milliseconds = std::chrono::milliseconds;

    AsynchronousInsertQueue(ContextPtr context_, size_t pool_size);
    ~AsynchronousInsertQueue();

    std::future<void> push(ASTPtr query, ContextPtr query_context);
    void waitForProcessingQuery(const String & query_id, const Milliseconds & timeout);

private:

    struct InsertQuery
    {
        ASTPtr query;
        Settings settings;

        InsertQuery(const ASTPtr & query_, const Settings & settings_);
        InsertQuery(const InsertQuery & other);
        InsertQuery & operator=(const InsertQuery & other);

        bool operator==(const InsertQuery & other) const;
        struct Hash { UInt64 operator()(const InsertQuery & insert_query) const; };
    };

    struct InsertData
    {
        struct Entry
        {
        public:
            const String bytes;
            const String query_id;
            const std::chrono::time_point<std::chrono::system_clock> create_time;

            Entry(String && bytes_, String && query_id_);

            void finish(std::exception_ptr exception_ = nullptr);
            std::future<void> getFuture() { return promise.get_future(); }
            bool isFinished() const { return finished; }

        private:
            std::promise<void> promise;
            std::atomic_bool finished = false;
        };

        explicit InsertData(std::chrono::steady_clock::time_point now) : first_update(now) {}

        using EntryPtr = std::shared_ptr<Entry>;

        std::list<EntryPtr> entries;
        size_t size = 0;

        /// Timestamp of the first insert into queue, or after the last queue dump.
        /// Used to detect for how long the queue is active, so we can dump it by timer.
        std::chrono::time_point<std::chrono::steady_clock> first_update;
    };

    using InsertDataPtr = std::unique_ptr<InsertData>;

    struct Container
    {
        InsertQuery key;
        InsertDataPtr data;
    };

    /// Ordered container
    using Queue = std::map<std::chrono::steady_clock::time_point, Container>;
    using QueueIterator = Queue::iterator;
    using QueueIteratorByKey = std::unordered_map<InsertQuery, QueueIterator, InsertQuery::Hash>;

    mutable std::mutex queue_mutex;
    mutable std::condition_variable are_tasks_available;

    Queue queue;
    QueueIteratorByKey queue_iterators;

    /// Logic and events behind queue are as follows:
    ///  - busy_timeout:   if queue is active for too long and there are a lot of rapid inserts, then we dump the data, so it doesn't
    ///                    grow for a long period of time and users will be able to select new data in deterministic manner.
    ///
    /// During processing incoming INSERT queries we can also check whether the maximum size of data in buffer is reached
    /// (async_insert_max_data_size setting). If so, then again we dump the data.

    std::atomic<bool> shutdown{false};

    ThreadPool pool;  /// dump the data only inside this pool.
    ThreadFromGlobalPool dump_by_first_update_thread;  /// uses busy_timeout and busyCheck()

    Poco::Logger * log = &Poco::Logger::get("AsynchronousInsertQueue");

    void busyCheck();
    void scheduleDataProcessingJob(const InsertQuery & key, InsertDataPtr data, ContextPtr global_context);

    static void processData(InsertQuery key, InsertDataPtr data, ContextPtr global_context);

    template <typename E>
    static void finishWithException(const ASTPtr & query, const std::list<InsertData::EntryPtr> & entries, const E & exception);

    /// @param timeout - time to wait
    /// @return true if shutdown requested
    bool waitForShutdown(const Milliseconds & timeout);

public:
    auto getQueueLocked() const
    {
        std::unique_lock lock(queue_mutex);
        return std::make_pair(std::ref(queue), std::move(lock));
    }
};

}
