#pragma once

#include <Core/Settings.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ThreadPool.h>

#include <future>

namespace DB
{

/// A queue, that stores data for insert queries and periodically flushes it to tables.
/// The data is grouped by table, format and settings of insert query.
class AsynchronousInsertQueue : public WithContext
{
public:
    using Milliseconds = std::chrono::milliseconds;

    AsynchronousInsertQueue(ContextPtr context_, size_t pool_size_);
    ~AsynchronousInsertQueue();

    struct PushResult
    {
        enum Status
        {
            OK,
            TOO_MUCH_DATA,
        };

        Status status;

        /// Future that allows to wait until the query is flushed.
        std::future<void> future;

        /// Read buffer that contains extracted
        /// from query data in case of too much data.
        std::unique_ptr<ReadBuffer> insert_data_buffer;
    };

    PushResult push(ASTPtr query, ContextPtr query_context);
    size_t getPoolSize() const { return pool_size; }

private:

    struct InsertQuery
    {
    public:
        ASTPtr query;
        String query_str;
        Settings settings;
        UInt128 hash;

        InsertQuery(const ASTPtr & query_, const Settings & settings_);
        InsertQuery(const InsertQuery & other);
        InsertQuery & operator=(const InsertQuery & other);
        bool operator==(const InsertQuery & other) const;

    private:
        UInt128 calculateHash() const;
    };

    struct InsertData
    {
        struct Entry
        {
        public:
            String bytes;
            const String query_id;
            const String async_dedup_token;
            MemoryTracker * const user_memory_tracker;
            const std::chrono::time_point<std::chrono::system_clock> create_time;

            Entry(String && bytes_, String && query_id_, const String & async_dedup_token, MemoryTracker * user_memory_tracker_);

            void finish(std::exception_ptr exception_ = nullptr);
            std::future<void> getFuture() { return promise.get_future(); }
            bool isFinished() const { return finished; }

        private:
            std::promise<void> promise;
            std::atomic_bool finished = false;
        };

        ~InsertData()
        {
            auto it = entries.begin();
            // Entries must be destroyed in context of user who runs async insert.
            // Each entry in the list may correspond to a different user,
            // so we need to switch current thread's MemoryTracker parent on each iteration.
            while (it != entries.end())
            {
                MemoryTrackerSwitcher switcher((*it)->user_memory_tracker);
                it = entries.erase(it);
            }
        }

        using EntryPtr = std::shared_ptr<Entry>;

        std::list<EntryPtr> entries;

        size_t size_in_bytes = 0;
        size_t query_number = 0;
    };

    using InsertDataPtr = std::unique_ptr<InsertData>;

    struct Container
    {
        InsertQuery key;
        InsertDataPtr data;
    };

    /// Ordered container
    /// Key is a timestamp of the first insert into batch.
    /// Used to detect for how long the batch is active, so we can dump it by timer.
    using Queue = std::map<std::chrono::steady_clock::time_point, Container>;
    using QueueIterator = Queue::iterator;
    using QueueIteratorByKey = std::unordered_map<UInt128, QueueIterator>;

    struct QueueShard
    {
        mutable std::mutex mutex;
        mutable std::condition_variable are_tasks_available;

        Queue queue;
        QueueIteratorByKey iterators;
    };

    const size_t pool_size;
    std::vector<QueueShard> queue_shards;

    /// Logic and events behind queue are as follows:
    ///  - async_insert_busy_timeout_ms:
    ///   if queue is active for too long and there are a lot of rapid inserts, then we dump the data, so it doesn't
    ///   grow for a long period of time and users will be able to select new data in deterministic manner.
    ///
    /// During processing incoming INSERT queries we can also check whether the maximum size of data in buffer is reached
    /// (async_insert_max_data_size setting). If so, then again we dump the data.

    std::atomic<bool> shutdown{false};

    /// Dump the data only inside this pool.
    ThreadPool pool;

    /// Uses async_insert_busy_timeout_ms and processBatchDeadlines()
    std::vector<ThreadFromGlobalPool> dump_by_first_update_threads;

    Poco::Logger * log = &Poco::Logger::get("AsynchronousInsertQueue");

    void processBatchDeadlines(size_t shard_num);
    void scheduleDataProcessingJob(const InsertQuery & key, InsertDataPtr data, ContextPtr global_context);

    static void processData(InsertQuery key, InsertDataPtr data, ContextPtr global_context);

    template <typename E>
    static void finishWithException(const ASTPtr & query, const std::list<InsertData::EntryPtr> & entries, const E & exception);

public:
    auto getQueueLocked(size_t shard_num) const
    {
        auto & shard = queue_shards[shard_num];
        std::unique_lock lock(shard.mutex);
        return std::make_pair(std::ref(shard.queue), std::move(lock));
    }
};

}
