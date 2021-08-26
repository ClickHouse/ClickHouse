#include <Interpreters/AsynchronousInsertionQueue.h>

#include <Core/Settings.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/copyData.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/queryToString.h>


namespace DB
{

struct AsynchronousInsertQueue::InsertData
{
    InsertData(ASTPtr query_, const Settings & settings_, const Block & header_)
        : query(std::move(query_)), settings(settings_), header(header_)
    {
    }

    ASTPtr query;
    Settings settings;
    Block header;
    String query_id;

    struct Data
    {
        String bytes;
        String query_id;
    };

    std::mutex mutex;
    std::list<Data> data;
    size_t size = 0;

    /// Timestamp of the first insert into queue, or after the last queue dump.
    /// Used to detect for how long the queue is active, so we can dump it by timer.
    std::chrono::time_point<std::chrono::steady_clock> first_update = std::chrono::steady_clock::now();

    /// Timestamp of the last insert into queue.
    /// Used to detect for how long the queue is stale, so we can dump it by another timer.
    std::chrono::time_point<std::chrono::steady_clock> last_update;

    /// Indicates that the BlockIO should be updated, because we can't read/write prefix and suffix more than once.
    bool is_reset = false;

    void reset()
    {
        data.clear();
        is_reset = true;
    }
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

AsynchronousInsertQueue::AsynchronousInsertQueue(ContextPtr context_, size_t pool_size, size_t max_data_size_, const Timeout & timeouts)
    : WithContext(context_)
    , max_data_size(max_data_size_)
    , busy_timeout(timeouts.busy)
    , stale_timeout(timeouts.stale)
    , lock(RWLockImpl::create())
    , queue(new Queue)
    , pool(pool_size)
    , dump_by_first_update_thread(&AsynchronousInsertQueue::busyCheck, this)
{
    using namespace std::chrono;

    assert(pool_size);

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

bool AsynchronousInsertQueue::push(
    const ASTPtr & query, const Settings & settings, const String & query_id)
{
    auto write_lock = lock->getLock(RWLockImpl::Write, String());
    InsertQuery key{query, settings};

    auto it = queue->find(key);
    if (it != queue->end())
    {
        std::unique_lock<std::mutex> data_lock(it->second->mutex);
        if (it->second->is_reset)
            return false;

        pushImpl(query, query_id, it);
        return true;
    }

    return false;
}

void AsynchronousInsertQueue::push(
    const ASTPtr & query, const Settings & settings, const String & query_id, const Block & header)
{
    auto write_lock = lock->getLock(RWLockImpl::Write, String());
    InsertQuery key{query, settings};

    auto it = queue->find(key);
    if (it == queue->end())
        it = queue->emplace(key, std::make_shared<InsertData>(query, settings, header)).first;
    else if (it->second->is_reset)
        it->second = std::make_shared<InsertData>(query, settings, header);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Entry for query '{}' already exists and not reset", queryToString(query));

    std::unique_lock<std::mutex> data_lock(it->second->mutex);
    pushImpl(query, query_id, it);
}

void AsynchronousInsertQueue::pushImpl(const ASTPtr & query, const String & query_id, QueueIterator it)
{
    auto read_buf = getReadBufferFromASTInsertQuery(query);

    /// It's important to read the whole data per query as a single chunk, so we can safely drop it in case of parsing failure.
    auto & new_data = it->second->data.emplace_back();

    new_data.query_id = query_id;
    new_data.bytes.reserve(read_buf->totalSize());
    WriteBufferFromString write_buf(new_data.bytes);

    copyData(*read_buf, write_buf);
    it->second->size += read_buf->count();
    it->second->last_update = std::chrono::steady_clock::now();

    LOG_INFO(&Poco::Logger::get("AsynchronousInsertQueue"),
        "Queue size {} for query '{}'", it->second->size, queryToString(*query));

    if (it->second->size > max_data_size)
        /// Since we're under lock here, it's safe to pass-by-copy the shared_ptr
        /// without a race with the cleanup thread, which may reset last shared_ptr instance.
        pool.scheduleOrThrowOnError([data = it->second, global_context = getContext()] { processData(data, global_context); });
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
                pool.scheduleOrThrowOnError([data = data, global_context = getContext()] { processData(data, global_context); });
            else
                timeout = std::min(timeout, std::chrono::ceil<std::chrono::seconds>(busy_timeout - lag));
        }
    }
}

void AsynchronousInsertQueue::staleCheck()
{
    while (!shutdown)
    {
        std::this_thread::sleep_for(stale_timeout);

        auto read_lock = lock->getLock(RWLockImpl::Read, String());

        for (auto & [_, data] : *queue)
        {
            std::unique_lock<std::mutex> data_lock(data->mutex);

            auto lag = std::chrono::steady_clock::now() - data->last_update;

            if (lag >= stale_timeout)
                pool.scheduleOrThrowOnError([data = data, global_context = getContext()] { processData(data, global_context); });
        }
    }
}

// static
void AsynchronousInsertQueue::processData(std::shared_ptr<InsertData> data, ContextPtr global_context)
try
{
    std::unique_lock<std::mutex> data_lock(data->mutex);

    if (data->is_reset)
        return;

    // ReadBuffers read_buffers;
    // for (const auto & datum : data->data)
    //     read_buffers.emplace_back(std::make_unique<ReadBufferFromString>(datum));

    // auto insert_context = Context::createCopy(global_context);
    // insert_context->makeQueryContext();
    // insert_context->setSettings(data->settings);

    // InterpreterInsertQuery interpreter(data->query, std::move(read_buffers), insert_context);
    // auto io = interpreter.execute();
    // assert(io.pipeline.initialized());

    // auto log_progress = [&](const Progress & progress)
    // {
    //     LOG_INFO(&Poco::Logger::get("AsynchronousInsertQueue"),
    //         "Flushed {} rows, {} bytes", progress.written_rows, progress.written_bytes);
    // };

    // io.pipeline.setProgressCallback(log_progress);
    // auto executor = io.pipeline.execute();
    // executor->execute(io.pipeline.getNumThreads());

    auto insert_context = Context::createCopy(global_context);
    /// 'resetParser' doesn't work for parallel parsing.
    data->settings.set("input_format_parallel_parsing", false);
    insert_context->makeQueryContext();
    insert_context->setSettings(data->settings);

    auto [format, pipe] = getSourceFromASTInsertQuery(data->query, false, data->header, insert_context, nullptr);

    MutableColumns input_columns = data->header.cloneEmptyColumns();
    size_t total_rows = 0;
    for (const auto & datum : data->data)
    {
        ReadBufferFromString buf(datum.bytes);
        format->setReadBuffer(buf);
        size_t current_rows = 0;

        try
        {
            Chunk chunk;
            QueryPipeline pipeline;
            pipeline.init(std::move(pipe));
            PullingPipelineExecutor executor(pipeline);
            while (executor.pull(chunk))
            {
                assert(chunk.getNumColumns() == input_chunk.getNumColumns());
                const auto & columns = chunk.getColumns();
                for (size_t i = 0; i < chunk.getNumColumns(); ++i)
                    input_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());

                current_rows += chunk.getNumRows();
            }
        }
        catch (const Exception & e)
        {
            LOG_INFO(&Poco::Logger::get("AsynchronousInsertQueue"),
                "Failed query '{}' with query id {}. {}",
                queryToString(data->query), datum.query_id, e.displayText());

            for (const auto & column : input_columns)
                if (column->size() > total_rows)
                    column->popBack(column->size() - total_rows);

            continue;
        }

        LOG_INFO(&Poco::Logger::get("AsynchronousInsertQueue"),
                "Succeded query '{}' with query id {}",
                queryToString(data->query), datum.query_id);

        format->resetParser();
        total_rows += current_rows;
    }

    std::cerr << "should be inserted query: " << queryToString(data->query) << "\n";
    auto block = data->header.cloneWithColumns(std::move(input_columns));
    std::cerr << "insert block: " << block.dumpStructure() << "\n";

    data->reset();
}
catch (...)
{
    tryLogCurrentException("AsynchronousInsertQueue", __PRETTY_FUNCTION__);
}

}
