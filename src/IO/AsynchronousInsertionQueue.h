#pragma once

#include <Parsers/IAST_fwd.h>
#include <Common/RWLock.h>
#include <Common/ThreadPool.h>

#include <unordered_map>


namespace DB
{

class ASTInsertQuery;
struct BlockIO;
struct Settings;

class AsynchronousInsertQueue
{
    public:
        AsynchronousInsertQueue(size_t pool_size, size_t max_data_size);

        bool push(ASTInsertQuery * query, const Settings & settings);
        void push(ASTInsertQuery * query, BlockIO && io, const Settings & settings);

    private:
        struct InsertQuery;
        struct InsertData;

        struct InsertQueryHash
        {
            std::size_t operator () (const InsertQuery &) const;
        };

        struct InsertQueryEquality
        {
            bool operator () (const InsertQuery &, const InsertQuery &) const;
        };

        using Queue = std::unordered_map<InsertQuery, std::shared_ptr<InsertData>, InsertQueryHash, InsertQueryEquality>;
        using QueueIterator = Queue::iterator;

        const size_t max_data_size;

        RWLock lock;
        Queue queue;

        ThreadPool pool;
        /// TODO: ThreadFromGlobalPool remove_empty_thread, check_access_thread;

        void pushImpl(ASTInsertQuery * query, QueueIterator & it);  /// use only under lock
        void processData(std::shared_ptr<InsertData> data);
};

}
