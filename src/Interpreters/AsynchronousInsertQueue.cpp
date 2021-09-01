#include <Interpreters/AsynchronousInsertQueue.h>

#include <Core/Settings.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/copyData.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/IStorage.h>
#include <Common/SipHash.h>
#include <Common/FieldVisitorHash.h>
#include <Access/AccessFlags.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

UInt64 AsynchronousInsertQueue::InsertQuery::Hash::operator()(const InsertQuery & insert_query) const
{
    SipHash hash;
    insert_query.query->updateTreeHash(hash);

    for (const auto & setting : insert_query.settings.allChanged())
    {
        hash.update(setting.getName());
        applyVisitor(FieldVisitorHash(hash), setting.getValue());
    }

    return hash.get64();
}

bool AsynchronousInsertQueue::InsertQuery::operator==(const InsertQuery & other) const
{
    return queryToString(query) == queryToString(other.query) && settings == other.settings;
}


void AsynchronousInsertQueue::InsertData::Entry::finish(std::exception_ptr exception_)
{
    std::lock_guard lock(mutex);
    finished = true;
    exception = exception_;
    cv.notify_all();
}

bool AsynchronousInsertQueue::InsertData::Entry::wait(const Milliseconds & timeout) const
{
    std::unique_lock lock(mutex);
    return cv.wait_for(lock, timeout, [&] { return finished; });
}

bool AsynchronousInsertQueue::InsertData::Entry::isFinished() const
{
    std::lock_guard lock(mutex);
    return finished;
}

std::exception_ptr AsynchronousInsertQueue::InsertData::Entry::getException() const
{
    std::lock_guard lock(mutex);
    return exception;
}


AsynchronousInsertQueue::AsynchronousInsertQueue(ContextPtr context_, size_t pool_size, size_t max_data_size_, const Timeout & timeouts)
    : WithContext(context_)
    , max_data_size(max_data_size_)
    , busy_timeout(timeouts.busy)
    , stale_timeout(timeouts.stale)
    , pool(pool_size)
    , dump_by_first_update_thread(&AsynchronousInsertQueue::busyCheck, this)
    , cleanup_thread(&AsynchronousInsertQueue::cleanup, this)
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

    assert(cleanup_thread.joinable());
    cleanup_thread.join();

    if (dump_by_last_update_thread.joinable())
        dump_by_last_update_thread.join();

    pool.wait();

    std::lock_guard lock(currently_processing_mutex);
    for (const auto & [_, entry] : currently_processing_queries)
    {
        if (!entry->isFinished())
            entry->finish(std::make_exception_ptr(Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Wait for async insert timeout exceeded)")));
    }
}

void AsynchronousInsertQueue::scheduleProcessDataJob(const InsertQuery & key, InsertDataPtr data, ContextPtr global_context)
{
    /// Wrap 'unique_ptr' with 'shared_ptr' to make this
    /// lambda copyable and allow to save it to the thread pool.
    pool.scheduleOrThrowOnError([=, data = std::make_shared<InsertDataPtr>(std::move(data))]
    {
        processData(std::move(key), std::move(*data), std::move(global_context));
    });
}

void AsynchronousInsertQueue::push(ASTPtr query, ContextPtr query_context)
{
    query = query->clone();
    const auto & settings = query_context->getSettingsRef();
    auto & insert_query = query->as<ASTInsertQuery &>();

    InterpreterInsertQuery interpreter(query, query_context, settings.insert_allow_materialized_columns);
    auto table = interpreter.getTable(insert_query);
    auto sample_block = interpreter.getSampleBlock(insert_query, table, table->getInMemoryMetadataPtr());

    query_context->checkAccess(AccessFlags(AccessType::INSERT), insert_query.table_id, sample_block.getNames());

    auto read_buf = getReadBufferFromASTInsertQuery(query);

    /// It's important to read the whole data per query as a single chunk, so we can safely drop it in case of parsing failure.
    auto entry = std::make_shared<InsertData::Entry>();
    entry->query_id = query_context->getCurrentQueryId();
    entry->bytes.reserve(read_buf->totalSize());

    WriteBufferFromString write_buf(entry->bytes);
    copyData(*read_buf, write_buf);

    InsertQuery key{query, settings};
    Queue::iterator it;
    bool found = false;

    {
        std::shared_lock read_lock(rwlock);
        it = queue.find(key);
        if (it != queue.end())
            found = true;
    }

    if (!found)
    {
        std::unique_lock write_lock(rwlock);
        it = queue.emplace(key, std::make_shared<Container>()).first;
    }

    auto & [data_mutex, data] = *it->second;
    std::lock_guard data_lock(data_mutex);

    if (!data)
        data = std::make_unique<InsertData>();

    data->size += read_buf->count();
    data->last_update = std::chrono::steady_clock::now();
    data->entries.emplace_back(entry);

    {
        std::lock_guard currently_processing_lock(currently_processing_mutex);
        currently_processing_queries.emplace(entry->query_id, entry);
    }

    LOG_INFO(log, "Have {} pending inserts with total {} bytes of data for query '{}'",
        data->entries.size(), data->size, queryToString(*query));

    if (data->size > max_data_size)
        scheduleProcessDataJob(key, std::move(data), getContext());
}

void AsynchronousInsertQueue::waitForProcessingQuery(const String & query_id, const Milliseconds & timeout)
{
    InsertData::EntryPtr entry;

    {
        std::lock_guard lock(currently_processing_mutex);
        auto it = currently_processing_queries.find(query_id);
        if (it == currently_processing_queries.end())
            return;

        entry = it->second;
    }

    bool finished = entry->wait(timeout);

    if (!finished)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Wait for async insert timeout ({} ms) exceeded)", timeout.count());

    if (auto exception = entry->getException())
        std::rethrow_exception(exception);
}

void AsynchronousInsertQueue::busyCheck()
{
    auto timeout = busy_timeout;

    while (!shutdown)
    {
        std::this_thread::sleep_for(timeout);

        /// TODO: use priority queue instead of raw unsorted queue.
        timeout = busy_timeout;
        std::shared_lock read_lock(rwlock);

        for (auto & [key, elem] : queue)
        {
            std::lock_guard data_lock(elem->mutex);
            if (!elem->data)
                continue;

            auto lag = std::chrono::steady_clock::now() - elem->data->first_update;
            if (lag >= busy_timeout)
                scheduleProcessDataJob(key, std::move(elem->data), getContext());
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
        std::shared_lock read_lock(rwlock);

        for (auto & [key, elem] : queue)
        {
            std::lock_guard data_lock(elem->mutex);
            if (!elem->data)
                continue;

            auto lag = std::chrono::steady_clock::now() - elem->data->last_update;
            if (lag >= stale_timeout)
                scheduleProcessDataJob(key, std::move(elem->data), getContext());
        }
    }
}

void AsynchronousInsertQueue::cleanup()
{
    auto timeout = busy_timeout * 5;

    while (!shutdown)
    {
        std::this_thread::sleep_for(timeout);
        std::vector<InsertQuery> keys_to_remove;

        {
            std::shared_lock read_lock(rwlock);

            for (auto & [key, elem] : queue)
            {
                std::lock_guard data_lock(elem->mutex);
                if (!elem->data)
                    keys_to_remove.push_back(key);
            }
        }

        if (!keys_to_remove.empty())
        {
            std::unique_lock write_lock(rwlock);
            size_t total_removed = 0;

            for (const auto & key : keys_to_remove)
            {
                auto it = queue.find(key);
                if (it != queue.end() && !it->second->data)
                {
                    queue.erase(it);
                    ++total_removed;
                }
            }

            if (total_removed)
                LOG_TRACE(log, "Removed stale entries for {} queries from asynchronous insertion queue", keys_to_remove.size());
        }

        {
            std::vector<String> ids_to_remove;
            std::lock_guard lock(currently_processing_mutex);

            for (const auto & [query_id, entry] : currently_processing_queries)
                if (entry->isFinished())
                    ids_to_remove.push_back(query_id);

            if (!ids_to_remove.empty())
            {
                for (const auto & id : ids_to_remove)
                    currently_processing_queries.erase(id);

                LOG_TRACE(log, "Removed {} finished entries", ids_to_remove.size());
            }
        }
    }
}

// static
void AsynchronousInsertQueue::processData(InsertQuery key, InsertDataPtr data, ContextPtr global_context)
try
{
    if (!data)
        return;

    const auto * log = &Poco::Logger::get("AsynchronousInsertQueue");

    auto insert_context = Context::createCopy(global_context);
    /// 'resetParser' doesn't work for parallel parsing.
    key.settings.set("input_format_parallel_parsing", false);
    insert_context->makeQueryContext();
    insert_context->setSettings(key.settings);

    InterpreterInsertQuery interpreter(key.query, insert_context, key.settings.insert_allow_materialized_columns);
    auto sinks = interpreter.getSinks();
    assert(sinks.size() == 1);

    auto header = sinks.at(0)->getInputs().front().getHeader();
    auto format = getInputFormatFromASTInsertQuery(key.query, false, header, insert_context, nullptr);

    size_t total_rows = 0;
    InsertData::EntryPtr current_entry;

    auto on_error = [&](const MutableColumns & result_columns, Exception & e)
    {
        LOG_ERROR(log, "Failed parsing for query '{}' with query id {}. {}",
            queryToString(key.query), current_entry->query_id, e.displayText());

        for (const auto & column : result_columns)
            if (column->size() > total_rows)
                column->popBack(column->size() - total_rows);

        current_entry->finish(std::current_exception());
        return 0;
    };

    StreamingFormatExecutor executor(header, format, std::move(on_error));

    std::unique_ptr<ReadBuffer> buffer;
    for (const auto & entry : data->entries)
    {
        buffer = std::make_unique<ReadBufferFromString>(entry->bytes);

        format->resetParser();
        format->setReadBuffer(*buffer);
        current_entry = entry;
        total_rows += executor.execute();
    }

    auto chunk = Chunk(executor.getResultColumns(), total_rows);
    size_t total_bytes = chunk.bytes();

    auto source = std::make_shared<SourceFromSingleChunk>(header, std::move(chunk));
    Pipe pipe(source);

    QueryPipeline out_pipeline;
    out_pipeline.init(std::move(pipe));
    out_pipeline.resize(1);
    out_pipeline.setSinks([&](const Block &, Pipe::StreamType) { return sinks.at(0); });

    auto out_executor = out_pipeline.execute();
    out_executor->execute(out_pipeline.getNumThreads());

    LOG_INFO(log, "Flushed {} rows, {} bytes for query '{}'",
        total_rows, total_bytes, queryToString(key.query));

    for (const auto & entry : data->entries)
        if (!entry->isFinished())
            entry->finish();
}
catch (...)
{
    tryLogCurrentException("AsynchronousInsertQueue", __PRETTY_FUNCTION__);

    for (const auto & entry : data->entries)
        entry->finish(std::current_exception());
}

}
