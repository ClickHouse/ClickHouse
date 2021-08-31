#pragma once

#include <Parsers/IAST_fwd.h>
#include <Common/RWLock.h>
#include <Common/ThreadPool.h>
#include <Core/Settings.h>
#include <Poco/Logger.h>

#include <unordered_map>


namespace DB
{

class ASTInsertQuery;
struct BlockIO;

class AsynchronousInsertQueue : public WithContext
{
public:
    using Milliseconds = std::chrono::milliseconds;
    using Seconds = std::chrono::seconds;

    /// Using structure to allow and benefit from designated initialization and not mess with a positional arguments in ctor.
    struct Timeout
    {
        Seconds busy;
        Seconds stale;
    };

    AsynchronousInsertQueue(ContextPtr context_, size_t pool_size, size_t max_data_size, const Timeout & timeouts);
    ~AsynchronousInsertQueue();

    void push(const ASTPtr & query, const Settings & settings, const String & query_id);
    void waitForProcessingQuery(const String & query_id, const Milliseconds & timeout);

private:

    struct InsertQuery
    {
        ASTPtr query;
        Settings settings;

        InsertQuery(const ASTPtr & query_, const Settings & settings_);
        InsertQuery(const InsertQuery & other);
        InsertQuery & operator==(const InsertQuery & other);
        bool operator==(const InsertQuery & other) const;
        struct Hash { UInt64 operator()(const InsertQuery & insert_query) const; };
    };

    struct InsertData
    {
        struct Entry
        {
        public:
            String bytes;
            String query_id;

            bool finished = false;
            std::exception_ptr exception;

            void finish(std::exception_ptr exception_ = nullptr);
            bool wait(const Milliseconds & timeout);

        private:
            std::mutex mutex;
            std::condition_variable cv;
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

    struct Container
    {
        std::mutex mutex;
        InsertDataPtr data;
    };

    using Queue = std::unordered_map<InsertQuery, std::shared_ptr<Container>, InsertQuery::Hash>;
    using QueueIterator = Queue::iterator;

    std::shared_mutex rwlock;
    Queue queue;

    std::mutex currently_processing_mutex;
    std::unordered_map<String, InsertData::EntryPtr> currently_processing_queries;

    /// Logic and events behind queue are as follows:
    ///  - reset_timeout:  if queue is empty for some time, then we delete the queue and free all associated resources, e.g. tables.
    ///  - busy_timeout:   if queue is active for too long and there are a lot of rapid inserts, then we dump the data, so it doesn't
    ///                    grow for a long period of time and users will be able to select new data in deterministic manner.
    ///  - stale_timeout:  if queue is stale for too long, then we dump the data too, so that users will be able to select the last
    ///                    piece of inserted data.
    ///  - access_timeout: also we have to check if user still has access to the tables periodically, and if the access is lost, then
    ///                    we dump pending data and delete queue immediately.
    ///  - max_data_size:  if the maximum size of data is reached, then again we dump the data.

    const size_t max_data_size;  /// in bytes
    const Seconds busy_timeout;
    const Seconds stale_timeout;

    std::atomic<bool> shutdown{false};
    ThreadPool pool;  /// dump the data only inside this pool.
    ThreadFromGlobalPool dump_by_first_update_thread;  /// uses busy_timeout and busyCheck()
    ThreadFromGlobalPool dump_by_last_update_thread;   /// uses stale_timeout and staleCheck()
    ThreadFromGlobalPool cleanup_thread;

    Poco::Logger * log = &Poco::Logger::get("AsynchronousInsertQueue");

    void busyCheck();
    void staleCheck();
    void cleanup();

    void scheduleProcessJob(const InsertQuery & key, InsertDataPtr data, ContextPtr global_context);
    static void processData(InsertQuery key, InsertDataPtr data, ContextPtr global_context);
};

}
