#pragma once

#include <Parsers/IAST_fwd.h>
#include <Common/ThreadPool.h>
#include <Core/Settings.h>
#include <Poco/Logger.h>

#include <unordered_map>


namespace DB
{

/// A queue, that stores data for insert queries and periodically flushes it to tables.
/// The data is grouped by table, format and settings of insert query.
class AsynchronousInsertQueue : public WithContext
{
public:
    using Milliseconds = std::chrono::milliseconds;

    /// Using structure to allow and benefit from designated initialization and not mess with a positional arguments in ctor.
    struct Timeout
    {
        Milliseconds busy;
        Milliseconds stale;
    };

    AsynchronousInsertQueue(ContextPtr context_, size_t pool_size, size_t max_data_size, const Timeout & timeouts);
    ~AsynchronousInsertQueue();

    void push(ASTPtr query, ContextPtr query_context);
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

            Entry(String && bytes_, String && query_id_);

            void finish(std::exception_ptr exception_ = nullptr);
            bool wait(const Milliseconds & timeout) const;
            bool isFinished() const;
            std::exception_ptr getException() const;

        private:
            mutable std::mutex mutex;
            mutable std::condition_variable cv;

            bool finished = false;
            std::exception_ptr exception;
        };

        using EntryPtr = std::shared_ptr<Entry>;

        std::list<EntryPtr> entries;
        size_t size = 0;

        /// Timestamp of the first insert into queue, or after the last queue dump.
        /// Used to detect for how long the queue is active, so we can dump it by timer.
        std::chrono::time_point<std::chrono::steady_clock> first_update = std::chrono::steady_clock::now();

        /// Timestamp of the last insert into queue.
        /// Used to detect for how long the queue is stale, so we can dump it by another timer.
        std::chrono::time_point<std::chrono::steady_clock> last_update;
    };

    using InsertDataPtr = std::unique_ptr<InsertData>;

    /// A separate container, that holds a data and a mutex for it.
    /// When it's needed to process current chunk of data, it can be moved for processing
    /// and new data can be recreated without holding a lock during processing.
    struct Container
    {
        std::mutex mutex;
        InsertDataPtr data;
    };

    using Queue = std::unordered_map<InsertQuery, std::shared_ptr<Container>, InsertQuery::Hash>;
    using QueueIterator = Queue::iterator;

    mutable std::shared_mutex rwlock;
    Queue queue;

    using QueryIdToEntry = std::unordered_map<String, InsertData::EntryPtr>;
    mutable std::mutex currently_processing_mutex;
    QueryIdToEntry currently_processing_queries;

    /// Logic and events behind queue are as follows:
    ///  - busy_timeout:   if queue is active for too long and there are a lot of rapid inserts, then we dump the data, so it doesn't
    ///                    grow for a long period of time and users will be able to select new data in deterministic manner.
    ///  - stale_timeout:  if queue is stale for too long, then we dump the data too, so that users will be able to select the last
    ///                    piece of inserted data.
    ///  - max_data_size:  if the maximum size of data is reached, then again we dump the data.

    const size_t max_data_size;  /// in bytes
    const Milliseconds busy_timeout;
    const Milliseconds stale_timeout;

    std::atomic<bool> shutdown{false};
    ThreadPool pool;  /// dump the data only inside this pool.
    ThreadFromGlobalPool dump_by_first_update_thread;  /// uses busy_timeout and busyCheck()
    ThreadFromGlobalPool dump_by_last_update_thread;   /// uses stale_timeout and staleCheck()
    ThreadFromGlobalPool cleanup_thread;               /// uses busy_timeout and cleanup()

    Poco::Logger * log = &Poco::Logger::get("AsynchronousInsertQueue");

    void busyCheck();
    void staleCheck();
    void cleanup();

    /// Should be called with shared or exclusively locked 'rwlock'.
    void pushImpl(InsertData::EntryPtr entry, QueueIterator it);

    void scheduleDataProcessingJob(const InsertQuery & key, InsertDataPtr data, ContextPtr global_context);
    static void processData(InsertQuery key, InsertDataPtr data, ContextPtr global_context);

    template <typename E>
    static void finishWithException(const ASTPtr & query, const std::list<InsertData::EntryPtr> & entries, const E & exception);

public:
    auto getQueueLocked() const
    {
        std::shared_lock lock(rwlock);
        return std::make_pair(std::ref(queue), std::move(lock));
    }
};

}
