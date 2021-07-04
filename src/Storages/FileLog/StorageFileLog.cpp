#include <Storages/FileLog/FileLogBlockInputStream.h>
#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <Storages/FileLog/StorageFileLog.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    const auto RESCHEDULE_MS = 500;
    const auto MAX_THREAD_WORK_DURATION_MS = 60000;  // once per minute leave do reschedule (we can't lock threads in pool forever)
}

StorageFileLog::StorageFileLog(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & path_,
    const String & format_name_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , path(path_)
    , format_name(format_name_)
    , log(&Poco::Logger::get("StorageFile (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    auto thread = getContext()->getMessageBrokerSchedulePool().createTask(log->name(), [this] { threadFunc(); });
    thread->deactivate();
    task = std::make_shared<TaskContext>(std::move(thread));
}

Pipe StorageFileLog::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /* query_info */,
    ContextPtr local_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    unsigned /* num_streams */)
{
    auto modified_context = Context::createCopy(local_context);

    return Pipe(std::make_shared<SourceFromInputStream>(
        std::make_shared<FileLogBlockInputStream>(*this, metadata_snapshot, modified_context, column_names, 1)));
}

void StorageFileLog::startup()
{
    try
    {
        createReadBuffer();
    }
    catch (const Exception &)
    {
        tryLogCurrentException(log);
    }

    task->holder->activateAndSchedule();
}


void StorageFileLog::shutdown()
{
    task->stream_cancelled = true;

    LOG_TRACE(log, "Waiting for cleanup");
    task->holder->deactivate();

    LOG_TRACE(log, "Closing files");
    destroyReadBuffer();
}

size_t StorageFileLog::getMaxBlockSize() const
{
    return getContext()->getSettingsRef().max_insert_block_size.value;
}

size_t StorageFileLog::getPollMaxBatchSize() const
{
    size_t batch_size = getContext()->getSettingsRef().max_block_size.value;

    return std::min(batch_size,getMaxBlockSize());
}

size_t StorageFileLog::getPollTimeoutMillisecond() const
{
    return getContext()->getSettingsRef().stream_poll_timeout_ms.totalMilliseconds();
}


bool StorageFileLog::checkDependencies(const StorageID & table_id)
{
    // Check if all dependencies are attached
    auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (dependencies.empty())
        return true;

    for (const auto & db_tab : dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(db_tab, getContext());
        if (!table)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(table.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;

        // Check all its dependencies
        if (!checkDependencies(db_tab))
            return false;
    }

    return true;
}

void StorageFileLog::createReadBuffer()
{
    auto new_context = Context::createCopy(getContext());
    buffer = std::make_shared<ReadBufferFromFileLog>(path, log, getPollMaxBatchSize(), getPollTimeoutMillisecond(), new_context);
}

void StorageFileLog::destroyReadBuffer()
{
    if (buffer)
        buffer->close();
}

void StorageFileLog::threadFunc()
{
    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        size_t dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();
        if (dependencies_count)
        {
            auto start_time = std::chrono::steady_clock::now();

            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!task->stream_cancelled)
            {
                if (!checkDependencies(table_id))
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                auto stream_is_stalled = streamToViews();
                if (stream_is_stalled)
                {
                    LOG_TRACE(log, "Stream stalled. Reschedule.");
                    break;
                }

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts-start_time);
                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                {
                    LOG_TRACE(log, "Thread work duration limit exceeded. Reschedule.");
                    break;
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    // Wait for attached views
    if (!task->stream_cancelled)
        task->holder->scheduleAfter(RESCHEDULE_MS);
}


bool StorageFileLog::streamToViews()
{
    Stopwatch watch;

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    size_t block_size = getMaxBlockSize();

    auto new_context = Context::createCopy(getContext());

    InterpreterInsertQuery interpreter(insert, new_context, false, true, true);
    auto block_io = interpreter.execute();

    auto stream = std::make_shared<FileLogBlockInputStream>(
        *this, metadata_snapshot, new_context, block_io.out->getHeader().getNames(), block_size);

    StreamLocalLimits limits;

    limits.speed_limits.max_execution_time = getContext()->getSettingsRef().stream_flush_interval_ms;

    limits.timeout_overflow_mode = OverflowMode::BREAK;
    stream->setLimits(limits);

    std::atomic<bool> stub = {false};
    size_t rows = 0;
    copyData(
        *stream, *block_io.out, [&rows](const Block & block) { rows += block.rows(); }, &stub);

    bool stream_is_stalled = false;

    stream_is_stalled = stream->as<FileLogBlockInputStream>()->isStalled();

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(log, "Pushing {} rows to {} took {} ms.",
        formatReadableQuantity(rows), table_id.getNameForLogs(), milliseconds);

    return stream_is_stalled;
}

void registerStorageFileLog(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();

        if (args_count != 2)
            throw Exception("Arguments size of StorageFileLog should be 2, path and format name", ErrorCodes::BAD_ARGUMENTS);

        auto path_ast = evaluateConstantExpressionAsLiteral(engine_args[0], args.getContext());
        auto format_ast = evaluateConstantExpressionAsLiteral(engine_args[1], args.getContext());

        auto path = path_ast->as<ASTLiteral &>().value.safeGet<String>();
        auto format = format_ast->as<ASTLiteral &>().value.safeGet<String>();

        return StorageFileLog::create(args.table_id, args.getContext(), args.columns, path, format);
    };

    factory.registerStorage(
        "FileLog",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
        });
}

NamesAndTypesList StorageFileLog::getVirtuals() const
{
    auto result = NamesAndTypesList{};
    return result;
}

Names StorageFileLog::getVirtualColumnNames()
{
    auto result = Names{};
    return result;
}

}
