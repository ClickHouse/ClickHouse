#include <IO/AsynchronousInsertionQueue.h>

#include <Core/Settings.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/copyData.h>
#include <Parsers/ASTInsertQuery.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include "IO/WriteBufferFromOStream.h"
#include "Parsers/formatAST.h"


namespace DB
{

struct AsynchronousInsertQueue::InsertData
{
    std::mutex mutex;
    std::list<std::string> data;
    size_t size = 0;
    BlockIO io;

    /// Timestamp of the first insert into queue, or after the last queue dump.
    /// Used to detect for how long the queue is active, so we can dump it by timer.
    std::chrono::time_point<std::chrono::steady_clock> first_update = std::chrono::steady_clock::now();

    /// Timestamp of the last insert into queue.
    /// Used to detect for how long the queue is stale, so we can dump it by another timer.
    std::chrono::time_point<std::chrono::steady_clock> last_update;

    /// Indicates that the BlockIO should be updated, because we can't read/write prefix and suffix more than once.
    bool reset = false;
};

std::size_t AsynchronousInsertQueue::InsertQueryHash::operator() (const InsertQuery & query) const
{
    const auto * insert_query = query.query->as<ASTInsertQuery>();
    std::size_t hash = 0;

    hash ^= std::hash<String>()(insert_query->table_id.getFullTableName());
    hash ^= std::hash<String>()(insert_query->format);
    // TODO: insert_query->columns
    // TODO: insert_query->table_function
    // TODO: insert_query->settings_ast

    // TODO: some of query.settings

    return hash;
}

bool AsynchronousInsertQueue::InsertQueryEquality::operator() (const InsertQuery & query1, const InsertQuery & query2) const
{
    const auto * insert_query1 = query1.query->as<ASTInsertQuery>();
    const auto * insert_query2 = query2.query->as<ASTInsertQuery>();

    if (insert_query1->table_id != insert_query2->table_id)
        return false;
    if (insert_query1->format != insert_query2->format)
        return false;
    // TODO: same fields as in InsertQueryHash.

    return true;
}

AsynchronousInsertQueue::AsynchronousInsertQueue(size_t pool_size, size_t max_data_size_, const Timeout & timeouts)
    : max_data_size(max_data_size_)
    , busy_timeout(timeouts.busy)
    , stale_timeout(timeouts.stale)
    , lock(RWLockImpl::create())
    , queue(new Queue)
    , pool(pool_size)
    , dump_by_first_update_thread(&AsynchronousInsertQueue::busyCheck, this)
{
    using namespace std::chrono;

    if (stale_timeout > 0s)
        dump_by_last_update_thread = ThreadFromGlobalPool(&AsynchronousInsertQueue::staleCheck, this);
}

AsynchronousInsertQueue::~AsynchronousInsertQueue()
{
    /// TODO: add a setting for graceful shutdown.

    shutdown = true;

    assert(dump_by_first_update_thread.joinable());
    dump_by_first_update_thread.join();

    if (dump_by_last_update_thread.joinable())
        dump_by_last_update_thread.join();

    pool.wait();
}

bool AsynchronousInsertQueue::push(ASTInsertQuery * query, const Settings & settings)
{
    auto read_lock = lock->getLock(RWLockImpl::Read, String());

    auto it = queue->find(InsertQuery{query->shared_from_this(), settings});

    if (it != queue->end())
    {
        std::unique_lock<std::mutex> data_lock(it->second->mutex);

        if (it->second->reset)
            return false;

        pushImpl(query, it);
        return true;
    }

    return false;
}

void AsynchronousInsertQueue::push(ASTInsertQuery * query, BlockIO && io, const Settings & settings)
{
    auto write_lock = lock->getLock(RWLockImpl::Write, String());

    InsertQuery key{query->shared_from_this(), settings};
    auto it = queue->find(key);
    if (it == queue->end())
    {
        it = queue->insert({key, std::make_shared<InsertData>()}).first;
        it->second->io = std::move(io);
    }
    else if (it->second->reset)
    {
        it->second = std::make_shared<InsertData>();
        it->second->io = std::move(io);
    }

    std::unique_lock<std::mutex> data_lock(it->second->mutex);
    pushImpl(query, it);
}

void AsynchronousInsertQueue::busyCheck()
{
    auto timeout = busy_timeout;

    while (!shutdown)
    {
        std::this_thread::sleep_for(timeout);

        auto read_lock = lock->getLock(RWLockImpl::Read, String());

        /// TODO: use priority queue instead of raw unsorted queue.
        timeout = busy_timeout;
        for (auto & [_, data] : *queue)
        {
            std::unique_lock<std::mutex> data_lock(data->mutex);

            auto lag = std::chrono::steady_clock::now() - data->first_update;

            if (lag >= busy_timeout)
                pool.scheduleOrThrowOnError([data = data] { processData(data); });
            else
                timeout = std::min(timeout, std::chrono::ceil<std::chrono::seconds>(busy_timeout - lag));
        }
    }
}

void AsynchronousInsertQueue::staleCheck()
{
    while(!shutdown)
    {
        std::this_thread::sleep_for(stale_timeout);

        auto read_lock = lock->getLock(RWLockImpl::Read, String());

        for (auto & [_, data] : *queue)
        {
            std::unique_lock<std::mutex> data_lock(data->mutex);

            auto lag = std::chrono::steady_clock::now() - data->last_update;

            if (lag >= stale_timeout)
                pool.scheduleOrThrowOnError([data = data] { processData(data); });
        }
    }
}

void AsynchronousInsertQueue::pushImpl(ASTInsertQuery * query, QueueIterator & it)
{
    ConcatReadBuffer concat_buf;

    auto ast_buf = std::make_unique<ReadBufferFromMemory>(query->data, query->data ? query->end - query->data : 0);

    if (query->data)
        concat_buf.appendBuffer(std::move(ast_buf));

    if (query->tail)
        concat_buf.appendBuffer(wrapReadBufferReference(*query->tail));

    /// NOTE: must not read from |query->tail| before read all between |query->data| and |query->end|.

    /// It's important to read the whole data per query as a single chunk, so we can safely drop it in case of parsing failure.
    auto & new_data = it->second->data.emplace_back();
    new_data.reserve(concat_buf.totalSize());
    WriteBufferFromString write_buf(new_data);

    copyData(concat_buf, write_buf);
    it->second->size += concat_buf.count();
    it->second->last_update = std::chrono::steady_clock::now();

    String query_string;
    {
        WriteBufferFromString buf(query_string);
        formatAST(*query, buf);
    }
    LOG_INFO(&Poco::Logger::get("AsynchronousInsertQueue"), "Queue size {} for query '{}'", it->second->size, query_string);

    if (it->second->size > max_data_size)
        /// Since we're under lock here, it's safe to pass-by-copy the shared_ptr
        /// without a race with the cleanup thread, which may reset last shared_ptr instance.
        pool.scheduleOrThrowOnError([data = it->second] { processData(data); });
}

// static
void AsynchronousInsertQueue::processData(std::shared_ptr<InsertData> data)
{
    std::unique_lock<std::mutex> data_lock(data->mutex);

    if (data->reset)
        return;

    auto in = std::dynamic_pointer_cast<InputStreamFromASTInsertQuery>(data->io.in);
    assert(in);

    auto log_progress = [](const Block & block)
    {
        LOG_INFO(&Poco::Logger::get("AsynchronousInsertQueue"), "Flushed {} rows", block.rows());
    };

    for (const auto & datum : data->data)
        in->appendBuffer(std::make_unique<ReadBufferFromString>(datum));
    copyData(*in, *data->io.out, [] {return false;}, log_progress);

    data->reset = true;
}

}
