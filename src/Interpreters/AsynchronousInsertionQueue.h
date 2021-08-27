#pragma once

#include <Parsers/IAST_fwd.h>
#include <Common/RWLock.h>
#include <Common/ThreadPool.h>
#include <Core/Settings.h>

#include <unordered_map>


namespace DB
{

class ASTInsertQuery;
struct BlockIO;

class AsynchronousInsertQueue : public WithContext
{
    public:
        /// Using structure to allow and benefit from designated initialization and not mess with a positional arguments in ctor.
        struct Timeout
        {
            std::chrono::seconds busy, stale;
        };

        AsynchronousInsertQueue(ContextPtr context_, size_t pool_size, size_t max_data_size, const Timeout & timeouts);
        ~AsynchronousInsertQueue();

        void push(const ASTPtr & query, const Settings & settings, const String & query_id);

    private:

        struct InsertQuery
        {
            ASTPtr query;
            Settings settings;
        };
        struct InsertData;

        struct InsertQueryHash
        {
            std::size_t operator () (const InsertQuery &) const;
        };

        struct InsertQueryEquality
        {
            bool operator () (const InsertQuery &, const InsertQuery &) const;
        };

        /// Logic and events behind queue are as follows:
        ///  - reset_timeout:  if queue is empty for some time, then we delete the queue and free all associated resources, e.g. tables.
        ///  - busy_timeout:   if queue is active for too long and there are a lot of rapid inserts, then we dump the data, so it doesn't
        ///                    grow for a long period of time and users will be able to select new data in deterministic manner.
        ///  - stale_timeout:  if queue is stale for too long, then we dump the data too, so that users will be able to select the last
        ///                    piece of inserted data.
        ///  - access_timeout: also we have to check if user still has access to the tables periodically, and if the access is lost, then
        ///                    we dump pending data and delete queue immediately.
        ///  - max_data_size:  if the maximum size of data is reached, then again we dump the data.

        using Queue = std::unordered_map<InsertQuery, std::shared_ptr<InsertData>, InsertQueryHash, InsertQueryEquality>;
        using QueueIterator = Queue::iterator;

        const size_t max_data_size;  /// in bytes
        const std::chrono::seconds busy_timeout, stale_timeout;

        RWLock lock;
        std::unique_ptr<Queue> queue;

        std::atomic<bool> shutdown{false};
        ThreadPool pool;  /// dump the data only inside this pool.
        ThreadFromGlobalPool dump_by_first_update_thread;  /// uses busy_timeout and busyCheck()
        ThreadFromGlobalPool dump_by_last_update_thread;   /// uses stale_timeout and staleCheck()
        /// TODO: ThreadFromGlobalPool check_access_thread;

        void busyCheck();
        void staleCheck();

        void pushImpl(const ASTPtr & query, const String & query_id, QueueIterator it);
        static void processData(std::shared_ptr<InsertData> data, ContextPtr global_context);
};

}
