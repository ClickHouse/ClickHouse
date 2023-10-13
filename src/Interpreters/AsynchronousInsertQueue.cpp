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
#include <IO/LimitReadBuffer.h>
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
    extern const Metric AsynchronousInsertThreads;
    extern const Metric AsynchronousInsertThreadsActive;
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
    extern const int BAD_ARGUMENTS;
}

AsynchronousInsertQueue::InsertQuery::InsertQuery(const ASTPtr & query_, const Settings & settings_)
    : query(query_->clone())
    , query_str(queryToString(query))
    , settings(settings_)
    , hash(calculateHash())
{
}

AsynchronousInsertQueue::InsertQuery::InsertQuery(const InsertQuery & other)
    : query(other.query->clone())
    , query_str(other.query_str)
    , settings(other.settings)
    , hash(other.hash)
{
}

AsynchronousInsertQueue::InsertQuery &
AsynchronousInsertQueue::InsertQuery::operator=(const InsertQuery & other)
{
    if (this != &other)
    {
        query = other.query->clone();
        query_str = other.query_str;
        settings = other.settings;
        hash = other.hash;
    }

    return *this;
}

UInt128 AsynchronousInsertQueue::InsertQuery::calculateHash() const
{
    SipHash siphash;
    query->updateTreeHash(siphash);

    for (const auto & setting : settings.allChanged())
    {
        siphash.update(setting.getName());
        applyVisitor(FieldVisitorHash(siphash), setting.getValue());
    }

    UInt128 res;
    siphash.get128(res);
    return res;
}

bool AsynchronousInsertQueue::InsertQuery::operator==(const InsertQuery & other) const
{
    return query_str == other.query_str && settings == other.settings;
}

AsynchronousInsertQueue::InsertData::Entry::Entry(String && bytes_, String && query_id_)
    : bytes(std::move(bytes_))
    , query_id(std::move(query_id_))
    , create_time(std::chrono::system_clock::now())
{
}

void AsynchronousInsertQueue::InsertData::Entry::finish(std::exception_ptr exception_)
{
    if (finished.exchange(true))
        return;

    if (exception_)
    {
        promise.set_exception(exception_);
        ProfileEvents::increment(ProfileEvents::FailedAsyncInsertQuery, 1);
    }
    else
    {
        promise.set_value();
    }
}

AsynchronousInsertQueue::AsynchronousInsertQueue(ContextPtr context_, size_t pool_size_)
    : WithContext(context_)
    , pool_size(pool_size_)
    , queue_shards(pool_size)
    , pool(CurrentMetrics::AsynchronousInsertThreads, CurrentMetrics::AsynchronousInsertThreadsActive, pool_size)
{
    if (!pool_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "pool_size cannot be zero");

    for (size_t i = 0; i < pool_size; ++i)
        dump_by_first_update_threads.emplace_back([this, i] { processBatchDeadlines(i); });
}

AsynchronousInsertQueue::~AsynchronousInsertQueue()
{
    /// TODO: add a setting for graceful shutdown.

    LOG_TRACE(log, "Shutting down the asynchronous insertion queue");
    shutdown = true;

    for (size_t i = 0; i < pool_size; ++i)
    {
        auto & shard = queue_shards[i];

        shard.are_tasks_available.notify_one();
        assert(dump_by_first_update_threads[i].joinable());
        dump_by_first_update_threads[i].join();

        {
            std::lock_guard lock(shard.mutex);

            for (auto & [_, elem] : shard.queue)
            {
                for (const auto & entry : elem.data->entries)
                {
                    entry->finish(std::make_exception_ptr(Exception(
                        ErrorCodes::TIMEOUT_EXCEEDED, "Wait for async insert timeout exceeded)")));
                }
            }
        }
    }

    pool.wait();
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

AsynchronousInsertQueue::PushResult
AsynchronousInsertQueue::push(ASTPtr query, ContextPtr query_context)
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
        /// Read at most 'async_insert_max_data_size' bytes of data.
        /// If limit is exceeded we will fallback to synchronous insert
        /// to avoid buffering of huge amount of data in memory.

        auto read_buf = getReadBufferFromASTInsertQuery(query);
        LimitReadBuffer limit_buf(*read_buf, settings.async_insert_max_data_size, /* trow_exception */ false, /* exact_limit */ {});

        WriteBufferFromString write_buf(bytes);
        copyData(limit_buf, write_buf);

        if (!read_buf->eof())
        {
            write_buf.finalize();

            /// Concat read buffer with already extracted from insert
            /// query data and with the rest data from insert query.
            std::vector<std::unique_ptr<ReadBuffer>> buffers;
            buffers.emplace_back(std::make_unique<ReadBufferFromOwnString>(bytes));
            buffers.emplace_back(std::move(read_buf));

            return PushResult
            {
                .status = PushResult::TOO_MUCH_DATA,
                .insert_data_buffer = std::make_unique<ConcatReadBuffer>(std::move(buffers)),
            };
        }
    }

    if (auto quota = query_context->getQuota())
        quota->used(QuotaType::WRITTEN_BYTES, bytes.size());

    auto entry = std::make_shared<InsertData::Entry>(std::move(bytes), query_context->getCurrentQueryId());

    InsertQuery key{query, settings};
    InsertDataPtr data_to_process;
    std::future<void> insert_future;

    auto shard_num = key.hash % pool_size;
    auto & shard = queue_shards[shard_num];

    {
        std::lock_guard lock(shard.mutex);

        auto [it, inserted] = shard.iterators.try_emplace(key.hash);
        if (inserted)
        {
            auto now = std::chrono::steady_clock::now();
            auto timeout = now + Milliseconds{key.settings.async_insert_busy_timeout_ms};
            it->second = shard.queue.emplace(timeout, Container{key, std::make_unique<InsertData>()}).first;
        }

        auto queue_it = it->second;
        auto & data = queue_it->second.data;
        size_t entry_data_size = entry->bytes.size();

        assert(data);
        data->size_in_bytes += entry_data_size;
        ++data->query_number;
        data->entries.emplace_back(entry);
        insert_future = entry->getFuture();

        LOG_TRACE(log, "Have {} pending inserts with total {} bytes of data for query '{}'",
            data->entries.size(), data->size_in_bytes, key.query_str);

        /// Here we check whether we hit the limit on maximum data size in the buffer.
        /// And use setting from query context.
        /// It works, because queries with the same set of settings are already grouped together.
        if (data->size_in_bytes >= key.settings.async_insert_max_data_size
            || (data->query_number >= key.settings.async_insert_max_query_number && key.settings.async_insert_deduplicate))
        {
            data_to_process = std::move(data);
            shard.iterators.erase(it);
            shard.queue.erase(queue_it);
        }

        CurrentMetrics::add(CurrentMetrics::PendingAsyncInsert);
        ProfileEvents::increment(ProfileEvents::AsyncInsertQuery);
        ProfileEvents::increment(ProfileEvents::AsyncInsertBytes, entry_data_size);
    }

    if (data_to_process)
        scheduleDataProcessingJob(key, std::move(data_to_process), getContext());
    else
        shard.are_tasks_available.notify_one();

    return PushResult
    {
        .status = PushResult::OK,
        .future = std::move(insert_future),
    };
}

void AsynchronousInsertQueue::processBatchDeadlines(size_t shard_num)
{
    auto & shard = queue_shards[shard_num];

    while (!shutdown)
    {
        std::vector<Container> entries_to_flush;
        {
            std::unique_lock lock(shard.mutex);

            shard.are_tasks_available.wait_for(lock,
                Milliseconds(getContext()->getSettingsRef().async_insert_busy_timeout_ms), [&shard, this]
            {
                if (shutdown)
                    return true;

                if (!shard.queue.empty() && shard.queue.begin()->first < std::chrono::steady_clock::now())
                    return true;

                return false;
            });

            if (shutdown)
                return;

            const auto now = std::chrono::steady_clock::now();

            while (true)
            {
                if (shard.queue.empty() || shard.queue.begin()->first > now)
                    break;

                auto it = shard.queue.begin();
                shard.iterators.erase(it->second.key.hash);

                entries_to_flush.emplace_back(std::move(it->second));
                shard.queue.erase(it);
            }
        }

        for (auto & entry : entries_to_flush)
            scheduleDataProcessingJob(entry.key, std::move(entry.data), getContext());
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
            key.query_str, current_entry->query_id, current_exception);

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
    auto chunk_info = std::make_shared<ChunkOffsets>();
    for (const auto & entry : data->entries)
    {
        auto buffer = std::make_unique<ReadBufferFromString>(entry->bytes);
        current_entry = entry;
        total_rows += executor.execute(*buffer);
        chunk_info->offsets.push_back(total_rows);

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
        chunk.setChunkInfo(std::move(chunk_info));
        size_t total_bytes = chunk.bytes();

        auto source = std::make_shared<SourceFromSingleChunk>(header, std::move(chunk));
        pipeline.complete(Pipe(std::move(source)));

        CompletedPipelineExecutor completed_executor(pipeline);
        completed_executor.execute();

        LOG_INFO(log, "Flushed {} rows, {} bytes for query '{}'",
            total_rows, total_bytes, key.query_str);
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
