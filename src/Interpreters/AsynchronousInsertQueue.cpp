#include <Interpreters/AsynchronousInsertQueue.h>

#include <Core/Settings.h>
#include <QueryPipeline/BlockIO.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/AsynchronousInsertLog.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
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
#include <Common/DateLUT.h>
#include <Access/Common/AccessFlags.h>
#include <Access/EnabledQuota.h>
#include <Formats/FormatFactory.h>
#include <Common/logger_useful.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>


namespace CurrentMetrics
{
    extern const Metric PendingAsyncInsert;
}

namespace ProfileEvents
{
    extern const Event AsyncInsertQuery;
    extern const Event AsyncInsertBytes;
    extern const Event FailedAsyncInsertQuery;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_FORMAT;
}

AsynchronousInsertQueue::InsertQuery::InsertQuery(const ASTPtr & query_, const Settings & settings_)
    : query(query_->clone()), settings(settings_)
{
}

AsynchronousInsertQueue::InsertQuery::InsertQuery(const InsertQuery & other)
    : query(other.query->clone()), settings(other.settings)
{
}

AsynchronousInsertQueue::InsertQuery &
AsynchronousInsertQueue::InsertQuery::operator=(const InsertQuery & other)
{
    if (this != &other)
    {
        query = other.query->clone();
        settings = other.settings;
    }

    return *this;
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

AsynchronousInsertQueue::InsertData::Entry::Entry(String && bytes_, String && query_id_)
    : bytes(std::move(bytes_))
    , query_id(std::move(query_id_))
    , create_time(std::chrono::system_clock::now())
{
}

void AsynchronousInsertQueue::InsertData::Entry::finish(std::exception_ptr exception_)
{
    std::lock_guard lock(mutex);
    finished = true;
    if (exception_)
        ProfileEvents::increment(ProfileEvents::FailedAsyncInsertQuery, 1);
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


AsynchronousInsertQueue::AsynchronousInsertQueue(ContextPtr context_, size_t pool_size, Milliseconds cleanup_timeout_)
    : WithContext(context_)
    , cleanup_timeout(cleanup_timeout_)
    , pool(pool_size)
    , dump_by_first_update_thread(&AsynchronousInsertQueue::busyCheck, this)
    , cleanup_thread(&AsynchronousInsertQueue::cleanup, this)
{
    using namespace std::chrono;

    assert(pool_size);
}

AsynchronousInsertQueue::~AsynchronousInsertQueue()
{
    /// TODO: add a setting for graceful shutdown.

    LOG_TRACE(log, "Shutting down the asynchronous insertion queue");

    shutdown = true;
    {
        std::lock_guard lock(deadline_mutex);
        are_tasks_available.notify_one();
    }
    {
        std::lock_guard lock(cleanup_mutex);
        cleanup_can_run.notify_one();
    }

    assert(dump_by_first_update_thread.joinable());
    dump_by_first_update_thread.join();

    assert(cleanup_thread.joinable());
    cleanup_thread.join();

    pool.wait();

    std::lock_guard lock(currently_processing_mutex);
    for (const auto & [_, entry] : currently_processing_queries)
    {
        if (!entry->isFinished())
            entry->finish(std::make_exception_ptr(Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Wait for async insert timeout exceeded)")));
    }

    LOG_TRACE(log, "Asynchronous insertion queue finished");
}

void AsynchronousInsertQueue::scheduleDataProcessingJob(const InsertQuery & key, InsertDataPtr data, ContextPtr global_context)
{
    /// Wrap 'unique_ptr' with 'shared_ptr' to make this
    /// lambda copyable and allow to save it to the thread pool.
    pool.scheduleOrThrowOnError([key, global_context, data = std::make_shared<InsertDataPtr>(std::move(data))]() mutable
    {
        processData(key, std::move(*data), std::move(global_context));
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

    if (!FormatFactory::instance().isInputFormat(insert_query.format))
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown input format {}", insert_query.format);

    /// For table functions we check access while executing
    /// InterpreterInsertQuery::getTable() -> ITableFunction::execute().
    if (insert_query.table_id)
        query_context->checkAccess(AccessType::INSERT, insert_query.table_id, sample_block.getNames());

    String bytes;
    {
        auto read_buf = getReadBufferFromASTInsertQuery(query);
        WriteBufferFromString write_buf(bytes);
        copyData(*read_buf, write_buf);
    }

    if (auto quota = query_context->getQuota())
        quota->used(QuotaType::WRITTEN_BYTES, bytes.size());

    auto entry = std::make_shared<InsertData::Entry>(std::move(bytes), query_context->getCurrentQueryId());
    InsertQuery key{query, settings};

    {
        /// Firstly try to get entry from queue without exclusive lock.
        std::shared_lock read_lock(rwlock);
        if (auto it = queue.find(key); it != queue.end())
        {
            pushImpl(std::move(entry), it);
            return;
        }
    }

    std::lock_guard write_lock(rwlock);
    auto it = queue.emplace(key, std::make_shared<Container>()).first;
    pushImpl(std::move(entry), it);
}

void AsynchronousInsertQueue::pushImpl(InsertData::EntryPtr entry, QueueIterator it)
{
    auto & [data_mutex, data] = *it->second;
    std::lock_guard data_lock(data_mutex);

    if (!data)
    {
        auto now = std::chrono::steady_clock::now();
        data = std::make_unique<InsertData>(now);

        std::lock_guard lock(deadline_mutex);
        deadline_queue.insert({now + Milliseconds{it->first.settings.async_insert_busy_timeout_ms}, it});
        are_tasks_available.notify_one();
    }

    size_t entry_data_size = entry->bytes.size();

    data->size += entry_data_size;
    data->entries.emplace_back(entry);

    {
        std::lock_guard currently_processing_lock(currently_processing_mutex);
        currently_processing_queries.emplace(entry->query_id, entry);
    }

    LOG_TRACE(log, "Have {} pending inserts with total {} bytes of data for query '{}'",
        data->entries.size(), data->size, queryToString(it->first.query));

    /// Here we check whether we hit the limit on maximum data size in the buffer.
    /// And use setting from query context!
    /// It works, because queries with the same set of settings are already grouped together.
    if (data->size > it->first.settings.async_insert_max_data_size)
        scheduleDataProcessingJob(it->first, std::move(data), getContext());

    CurrentMetrics::add(CurrentMetrics::PendingAsyncInsert);
    ProfileEvents::increment(ProfileEvents::AsyncInsertQuery);
    ProfileEvents::increment(ProfileEvents::AsyncInsertBytes, entry_data_size);
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
    while (!shutdown)
    {
        std::vector<QueueIterator> entries_to_flush;
        {
            std::unique_lock deadline_lock(deadline_mutex);
            are_tasks_available.wait_for(deadline_lock, Milliseconds(getContext()->getSettingsRef().async_insert_busy_timeout_ms), [this]()
            {
                if (shutdown)
                    return true;

                if (!deadline_queue.empty() && deadline_queue.begin()->first < std::chrono::steady_clock::now())
                    return true;

                return false;
            });

            if (shutdown)
                return;

            const auto now = std::chrono::steady_clock::now();

            while (true)
            {
                if (deadline_queue.empty() || deadline_queue.begin()->first > now)
                    break;

                entries_to_flush.emplace_back(deadline_queue.begin()->second);
                deadline_queue.erase(deadline_queue.begin());
            }
        }

        std::shared_lock read_lock(rwlock);
        for (auto & entry : entries_to_flush)
        {
            auto & [key, elem] = *entry;
            std::lock_guard data_lock(elem->mutex);
            if (!elem->data)
                continue;

            scheduleDataProcessingJob(key, std::move(elem->data), getContext());
        }
    }
}

void AsynchronousInsertQueue::cleanup()
{
    while (true)
    {
        {
            std::unique_lock cleanup_lock(cleanup_mutex);
            cleanup_can_run.wait_for(cleanup_lock, Milliseconds(cleanup_timeout), [this]() -> bool { return shutdown; });

            if (shutdown)
                return;
        }

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
            std::lock_guard write_lock(rwlock);
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
                LOG_TRACE(log, "Removed stale entries for {} queries from asynchronous insertion queue", total_removed);
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

                LOG_TRACE(log, "Removed {} finished entries from asynchronous insertion queue", ids_to_remove.size());
            }
        }
    }
}


static void appendElementsToLogSafe(
    AsynchronousInsertLog & log,
    std::vector<AsynchronousInsertLogElement> elements,
    std::chrono::time_point<std::chrono::system_clock> flush_time,
    const String & flush_query_id,
    const String & flush_exception)
try
{
    using Status = AsynchronousInsertLogElement::Status;

    for (auto & elem : elements)
    {
        elem.flush_time = timeInSeconds(flush_time);
        elem.flush_time_microseconds = timeInMicroseconds(flush_time);
        elem.flush_query_id = flush_query_id;
        elem.exception = flush_exception;
        elem.status = flush_exception.empty() ? Status::Ok : Status::FlushError;
        log.add(elem);
    }
}
catch (...)
{
    tryLogCurrentException("AsynchronousInsertQueue", "Failed to add elements to AsynchronousInsertLog");
}

// static
void AsynchronousInsertQueue::processData(InsertQuery key, InsertDataPtr data, ContextPtr global_context)
try
{
    if (!data)
        return;

    SCOPE_EXIT(CurrentMetrics::sub(CurrentMetrics::PendingAsyncInsert, data->entries.size()));

    const auto * log = &Poco::Logger::get("AsynchronousInsertQueue");
    const auto & insert_query = assert_cast<const ASTInsertQuery &>(*key.query);
    auto insert_context = Context::createCopy(global_context);

    /// 'resetParser' doesn't work for parallel parsing.
    key.settings.set("input_format_parallel_parsing", false);
    insert_context->makeQueryContext();
    insert_context->setSettings(key.settings);

    /// Set initial_query_id, because it's used in InterpreterInsertQuery for table lock.
    insert_context->getClientInfo().query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    insert_context->setCurrentQueryId("");

    InterpreterInsertQuery interpreter(key.query, insert_context, key.settings.insert_allow_materialized_columns, false, false, true);
    auto pipeline = interpreter.execute().pipeline;
    assert(pipeline.pushing());

    auto header = pipeline.getHeader();
    auto format = getInputFormatFromASTInsertQuery(key.query, false, header, insert_context, nullptr);

    size_t total_rows = 0;
    InsertData::EntryPtr current_entry;
    String current_exception;

    auto on_error = [&](const MutableColumns & result_columns, Exception & e)
    {
        current_exception = e.displayText();
        LOG_ERROR(log, "Failed parsing for query '{}' with query id {}. {}",
            queryToString(key.query), current_entry->query_id, current_exception);

        for (const auto & column : result_columns)
            if (column->size() > total_rows)
                column->popBack(column->size() - total_rows);

        current_entry->finish(std::current_exception());
        return 0;
    };

    std::shared_ptr<ISimpleTransform> adding_defaults_transform;
    if (insert_context->getSettingsRef().input_format_defaults_for_omitted_fields && insert_query.table_id)
    {
        StoragePtr storage = DatabaseCatalog::instance().getTable(insert_query.table_id, insert_context);
        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        const auto & columns = metadata_snapshot->getColumns();
        if (columns.hasDefaults())
            adding_defaults_transform = std::make_shared<AddingDefaultsTransform>(header, columns, *format, insert_context);
    }

    auto insert_log = global_context->getAsynchronousInsertLog();
    std::vector<AsynchronousInsertLogElement> log_elements;

    if (insert_log)
        log_elements.reserve(data->entries.size());

    StreamingFormatExecutor executor(header, format, std::move(on_error), std::move(adding_defaults_transform));
    std::unique_ptr<ReadBuffer> last_buffer;
    for (const auto & entry : data->entries)
    {
        auto buffer = std::make_unique<ReadBufferFromString>(entry->bytes);
        current_entry = entry;
        total_rows += executor.execute(*buffer);

        /// Keep buffer, because it still can be used
        /// in destructor, while resetting buffer at next iteration.
        last_buffer = std::move(buffer);

        if (insert_log)
        {
            AsynchronousInsertLogElement elem;
            elem.event_time = timeInSeconds(entry->create_time);
            elem.event_time_microseconds = timeInMicroseconds(entry->create_time);
            elem.query = key.query;
            elem.query_id = entry->query_id;
            elem.bytes = entry->bytes.size();
            elem.exception = current_exception;
            current_exception.clear();

            /// If there was a parsing error,
            /// the entry won't be flushed anyway,
            /// so add the log element immediately.
            if (!elem.exception.empty())
            {
                elem.status = AsynchronousInsertLogElement::ParsingError;
                insert_log->add(elem);
            }
            else
            {
                log_elements.push_back(elem);
            }
        }
    }

    format->addBuffer(std::move(last_buffer));
    auto insert_query_id = insert_context->getCurrentQueryId();

    if (total_rows == 0)
        return;

    try
    {
        auto chunk = Chunk(executor.getResultColumns(), total_rows);
        size_t total_bytes = chunk.bytes();

        auto source = std::make_shared<SourceFromSingleChunk>(header, std::move(chunk));
        pipeline.complete(Pipe(std::move(source)));

        CompletedPipelineExecutor completed_executor(pipeline);
        completed_executor.execute();

        LOG_INFO(log, "Flushed {} rows, {} bytes for query '{}'",
            total_rows, total_bytes, queryToString(key.query));
    }
    catch (...)
    {
        if (!log_elements.empty())
        {
            auto exception = getCurrentExceptionMessage(false);
            auto flush_time = std::chrono::system_clock::now();
            appendElementsToLogSafe(*insert_log, std::move(log_elements), flush_time, insert_query_id, exception);
        }
        throw;
    }

    for (const auto & entry : data->entries)
    {
        if (!entry->isFinished())
            entry->finish();
    }

    if (!log_elements.empty())
    {
        auto flush_time = std::chrono::system_clock::now();
        appendElementsToLogSafe(*insert_log, std::move(log_elements), flush_time, insert_query_id, "");
    }
}
catch (const Exception & e)
{
    finishWithException(key.query, data->entries, e);
}
catch (const Poco::Exception & e)
{
    finishWithException(key.query, data->entries, e);
}
catch (const std::exception & e)
{
    finishWithException(key.query, data->entries, e);
}
catch (...)
{
    finishWithException(key.query, data->entries, Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception"));
}

template <typename E>
void AsynchronousInsertQueue::finishWithException(
    const ASTPtr & query, const std::list<InsertData::EntryPtr> & entries, const E & exception)
{
    tryLogCurrentException("AsynchronousInsertQueue", fmt::format("Failed insertion for query '{}'", queryToString(query)));

    for (const auto & entry : entries)
    {
        if (!entry->isFinished())
        {
            /// Make a copy of exception to avoid concurrent usage of
            /// one exception object from several threads.
            entry->finish(std::make_exception_ptr(exception));
        }
    }
}

}
