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
    std::chrono::time_point<std::chrono::steady_clock> first_update;

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

AsynchronousInsertQueue::AsynchronousInsertQueue(size_t pool_size, size_t max_data_size_)
    : max_data_size(max_data_size_), lock(RWLockImpl::create()), queue(new Queue), pool(pool_size)
{
}

bool AsynchronousInsertQueue::push(ASTInsertQuery * query, const Settings & settings)
{
    auto read_lock = lock->getLock(RWLockImpl::Read, String());

    auto it = queue->find(InsertQuery{query->shared_from_this(), settings});
    if (it != queue->end() && !it->second->reset)
    {
        pushImpl(query, it);
        return true;
    }

    return false;
}

void AsynchronousInsertQueue::push(ASTInsertQuery * query, BlockIO && io, const Settings & settings)
{
    auto write_lock = lock->getLock(RWLockImpl::Write, String());

    auto it = queue->find(InsertQuery{query->shared_from_this(), settings});
    if (it == queue->end())
    {
        InsertQuery key{query->shared_from_this(), settings};
        it = queue->insert({key, std::make_shared<InsertData>()}).first;
        it->second->io = std::move(io);
        it->second->first_update = std::chrono::steady_clock::now();
    }
    else
    {
        std::unique_lock<std::mutex> data_lock(it->second->mutex);

        it->second->reset = false;
        it->second->io = std::move(io);

        /// All other fields should have been already reset.
    }

    pushImpl(query, it);
}

void AsynchronousInsertQueue::pushImpl(ASTInsertQuery * query, QueueIterator & it)
{
    ConcatReadBuffer::Buffers buffers;

    auto ast_buf = std::make_unique<ReadBufferFromMemory>(query->data, query->data ? query->end - query->data : 0);

    if (query->data)
        buffers.push_back(std::move(ast_buf));

    if (query->tail)
        buffers.push_back(wrapReadBufferReference(*query->tail));

    /// NOTE: must not read from |query->tail| before read all between |query->data| and |query->end|.

    ConcatReadBuffer concat_buf(std::move(buffers));

    std::unique_lock<std::mutex> data_lock(it->second->mutex);

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

    auto in = std::dynamic_pointer_cast<InputStreamFromASTInsertQuery>(data->io.in);
    assert(in);

    for (const auto & datum : data->data)
        in->appendBuffer(std::make_unique<ReadBufferFromString>(datum));
    copyData(*in, *data->io.out);

    data->data.clear();
    data->size = 0;
    data->first_update = std::chrono::steady_clock::now();
    data->last_update = data->first_update;
    data->reset = true;
}

}
