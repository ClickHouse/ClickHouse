#include <IO/AsynchronousInsertionQueue.h>

#include <Core/Settings.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/copyData.h>
#include <Parsers/ASTInsertQuery.h>
#include <Common/getNumberOfPhysicalCPUCores.h>


namespace DB
{

struct AsynchronousInsertQueue::InsertQuery
{
    ASTPtr query;
    Settings settings;
};

struct AsynchronousInsertQueue::InsertData
{
    std::mutex mutex;
    std::list<std::string> data;
    size_t size = 0;
    std::chrono::time_point<std::chrono::steady_clock> first_update, last_update;
    BlockIO io;
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

AsynchronousInsertQueue::AsynchronousInsertQueue(size_t pool_size, size_t max_data_size_) : max_data_size(max_data_size_), pool(pool_size)
{
}

bool AsynchronousInsertQueue::push(ASTInsertQuery * query, const Settings & settings)
{
    auto read_lock = lock->getLock(RWLockImpl::Read, String());

    auto it = queue.find(InsertQuery{query->shared_from_this(), settings});
    if (it != queue.end())
    {
        pushImpl(query, it);
        return true;
    }

    return false;
}

void AsynchronousInsertQueue::push(ASTInsertQuery * query, BlockIO && io, const Settings & settings)
{
    auto write_lock = lock->getLock(RWLockImpl::Write, String());

    auto it = queue.find(InsertQuery{query->shared_from_this(), settings});
    if (it == queue.end())
    {
        InsertQuery key{query->shared_from_this(), settings};
        it = queue.insert({key, std::make_shared<InsertData>()}).first;
        it->second->io = std::move(io);
        it->second->first_update = std::chrono::steady_clock::now();
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

    if (it->second->size > max_data_size)
        /// Since we're under lock here it's safe to pass-by-copy the shared_ptr
        /// without a race with the cleanup thread, which may reset last shared_ptr instance.
        pool.scheduleOrThrowOnError([this, data = it->second] { processData(data); });
}

void AsynchronousInsertQueue::processData(std::shared_ptr<InsertData> data)
{
    data->first_update = std::chrono::steady_clock::now();
}

}
