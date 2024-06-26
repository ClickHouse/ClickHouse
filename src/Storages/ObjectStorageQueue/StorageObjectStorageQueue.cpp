#include <optional>

#include <Common/ProfileEvents.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sources/NullSource.h>
#include <Formats/FormatFactory.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>
#include <Storages/ObjectStorageQueue/StorageObjectStorageQueue.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadataFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int QUERY_NOT_ALLOWED;
}

namespace
{
    std::string chooseZooKeeperPath(const StorageID & table_id, const Settings & settings, const ObjectStorageQueueSettings & queue_settings)
    {
        std::string zk_path_prefix = settings.s3queue_default_zookeeper_path.value;
        if (zk_path_prefix.empty())
            zk_path_prefix = "/";

        std::string result_zk_path;
        if (queue_settings.keeper_path.changed)
        {
            /// We do not add table uuid here on purpose.
            result_zk_path = fs::path(zk_path_prefix) / queue_settings.keeper_path.value;
        }
        else
        {
            auto database_uuid = DatabaseCatalog::instance().getDatabase(table_id.database_name)->getUUID();
            result_zk_path = fs::path(zk_path_prefix) / toString(database_uuid) / toString(table_id.uuid);
        }
        return zkutil::extractZooKeeperPath(result_zk_path, true);
    }

    void checkAndAdjustSettings(
        ObjectStorageQueueSettings & queue_settings,
        ASTStorage * engine_args,
        bool is_attach,
        const LoggerPtr & log)
    {
        if (!is_attach && !queue_settings.mode.changed)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `mode` (Unordered/Ordered) is not specified, but is required.");
        }
        /// In case !is_attach, we leave Ordered mode as default for compatibility.

        if (!queue_settings.processing_threads_num)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `processing_threads_num` cannot be set to zero");
        }

        if (queue_settings.cleanup_interval_min_ms > queue_settings.cleanup_interval_max_ms)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Setting `cleanup_interval_min_ms` ({}) must be less or equal to `cleanup_interval_max_ms` ({})",
                            queue_settings.cleanup_interval_min_ms, queue_settings.cleanup_interval_max_ms);
        }

        if (!is_attach && !queue_settings.processing_threads_num.changed)
        {
            queue_settings.processing_threads_num = std::max<uint32_t>(getNumberOfPhysicalCPUCores(), 16);
            engine_args->settings->as<ASTSetQuery>()->changes.insertSetting(
                "processing_threads_num",
                queue_settings.processing_threads_num.value);

            LOG_TRACE(log, "Set `processing_threads_num` to {}", queue_settings.processing_threads_num);
        }
    }

    std::shared_ptr<ObjectStorageQueueLog> getQueueLog(const ObjectStoragePtr & storage, const ContextPtr & context, const ObjectStorageQueueSettings & table_settings)
    {
        const auto & settings = context->getSettingsRef();
        switch (storage->getType())
        {
            case DB::ObjectStorageType::S3:
            {
                if (table_settings.enable_logging_to_queue_log || settings.s3queue_enable_logging_to_s3queue_log)
                    return context->getS3QueueLog();
                return nullptr;
            }
            case DB::ObjectStorageType::Azure:
            {
                if (table_settings.enable_logging_to_queue_log)
                    return context->getAzureQueueLog();
                return nullptr;
            }
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected object storage type: {}", storage->getType());
        }

    }
}

StorageObjectStorageQueue::StorageObjectStorageQueue(
    std::unique_ptr<ObjectStorageQueueSettings> queue_settings_,
    const ConfigurationPtr configuration_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    ASTStorage * engine_args,
    LoadingStrictnessLevel mode)
    : IStorage(table_id_)
    , WithContext(context_)
    , queue_settings(std::move(queue_settings_))
    , zk_path(chooseZooKeeperPath(table_id_, context_->getSettingsRef(), *queue_settings))
    , configuration{configuration_}
    , format_settings(format_settings_)
    , reschedule_processing_interval_ms(queue_settings->polling_min_timeout_ms)
    , log(getLogger(fmt::format("Storage{}Queue ({})", configuration->getEngineName(), table_id_.getFullTableName())))
{
    if (configuration->getPath().empty())
    {
        configuration->setPath("/*");
    }
    else if (configuration->getPath().ends_with('/'))
    {
        configuration->setPath(configuration->getPath() + '*');
    }
    else if (!configuration->isPathWithGlobs())
    {
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "ObjectStorageQueue url must either end with '/' or contain globs");
    }

    checkAndAdjustSettings(*queue_settings, engine_args, mode > LoadingStrictnessLevel::CREATE, log);

    object_storage = configuration->createObjectStorage(context_, /* is_readonly */true);
    FormatFactory::instance().checkFormatName(configuration->format);
    configuration->check(context_);

    ColumnsDescription columns{columns_};
    resolveSchemaAndFormat(columns, configuration->format, object_storage, configuration, format_settings, context_);
    configuration->check(context_);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.getColumns()));
    setInMemoryMetadata(storage_metadata);

    LOG_INFO(log, "Using zookeeper path: {}", zk_path.string());
    task = getContext()->getSchedulePool().createTask("ObjectStorageQueueStreamingTask", [this] { threadFunc(); });

    /// Get metadata manager from ObjectStorageQueueMetadataFactory,
    /// it will increase the ref count for the metadata object.
    /// The ref count is decreased when StorageObjectStorageQueue::drop() method is called.
    files_metadata = ObjectStorageQueueMetadataFactory::instance().getOrCreate(zk_path, *queue_settings);
    try
    {
        files_metadata->initialize(configuration_, storage_metadata);
    }
    catch (...)
    {
        ObjectStorageQueueMetadataFactory::instance().remove(zk_path);
        throw;
    }
}

void StorageObjectStorageQueue::startup()
{
    if (task)
        task->activateAndSchedule();
}

void StorageObjectStorageQueue::shutdown(bool is_drop)
{
    table_is_being_dropped = is_drop;
    shutdown_called = true;

    LOG_TRACE(log, "Shutting down storage...");
    if (task)
    {
        task->deactivate();
    }

    if (files_metadata)
    {
        files_metadata->shutdown();
        files_metadata.reset();
    }
    LOG_TRACE(log, "Shut down storage");
}

void StorageObjectStorageQueue::drop()
{
    ObjectStorageQueueMetadataFactory::instance().remove(zk_path);
}

bool StorageObjectStorageQueue::supportsSubsetOfColumns(const ContextPtr & context_) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context_, format_settings);
}

class ReadFromObjectStorageQueue : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromObjectStorageQueue"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    ReadFromObjectStorageQueue(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        ReadFromFormatInfo info_,
        std::shared_ptr<StorageObjectStorageQueue> storage_,
        size_t max_block_size_)
        : SourceStepWithFilter(
            DataStream{.header = std::move(sample_block)},
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , info(std::move(info_))
        , storage(std::move(storage_))
        , max_block_size(max_block_size_)
    {
    }

private:
    ReadFromFormatInfo info;
    std::shared_ptr<StorageObjectStorageQueue> storage;
    size_t max_block_size;

    std::shared_ptr<StorageObjectStorageQueue::FileIterator> iterator;

    void createIterator(const ActionsDAG::Node * predicate);
};

void ReadFromObjectStorageQueue::createIterator(const ActionsDAG::Node * predicate)
{
    if (iterator)
        return;

    iterator = storage->createFileIterator(context, predicate);
}


void ReadFromObjectStorageQueue::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    createIterator(predicate);
}

void StorageObjectStorageQueue::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t)
{
    if (!local_context->getSettingsRef().stream_like_engine_allow_direct_select)
    {
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed. "
                        "To enable use setting `stream_like_engine_allow_direct_select`");
    }

    if (mv_attached)
    {
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED,
                        "Cannot read from {} with attached materialized views", getName());
    }

    auto this_ptr = std::static_pointer_cast<StorageObjectStorageQueue>(shared_from_this());
    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(local_context));

    auto reading = std::make_unique<ReadFromObjectStorageQueue>(
        column_names,
        query_info,
        storage_snapshot,
        local_context,
        read_from_format_info.source_header,
        read_from_format_info,
        std::move(this_ptr),
        max_block_size);

    query_plan.addStep(std::move(reading));
}

void ReadFromObjectStorageQueue::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes;
    const size_t adjusted_num_streams = storage->queue_settings->processing_threads_num;

    createIterator(nullptr);
    for (size_t i = 0; i < adjusted_num_streams; ++i)
        pipes.emplace_back(storage->createSource(
                               i/* processor_id */,
                               info,
                               iterator,
                               max_block_size,
                               context,
                               true/* commit_once_processed */));

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(info.source_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

std::shared_ptr<ObjectStorageQueueSource> StorageObjectStorageQueue::createSource(
    size_t processor_id,
    const ReadFromFormatInfo & info,
    std::shared_ptr<StorageObjectStorageQueue::FileIterator> file_iterator,
    size_t max_block_size,
    ContextPtr local_context,
    bool commit_once_processed)
{
    auto internal_source = std::make_unique<StorageObjectStorageSource>(
        getName(),
        object_storage,
        configuration,
        info,
        format_settings,
        local_context,
        max_block_size,
        file_iterator,
        local_context->getSettingsRef().max_download_threads,
        false);

    auto file_deleter = [=, this](const std::string & path) mutable
    {
        object_storage->removeObject(StoredObject(path));
    };

    return std::make_shared<ObjectStorageQueueSource>(
        getName(),
        processor_id,
        info.source_header,
        std::move(internal_source),
        files_metadata,
        queue_settings->after_processing,
        file_deleter,
        info.requested_virtual_columns,
        local_context,
        shutdown_called,
        table_is_being_dropped,
        getQueueLog(object_storage, local_context, *queue_settings),
        getStorageID(),
        log,
        queue_settings->max_processed_files_before_commit,
        queue_settings->max_processed_rows_before_commit,
        queue_settings->max_processed_bytes_before_commit,
        queue_settings->max_processing_time_sec_before_commit,
        commit_once_processed);
}

bool StorageObjectStorageQueue::hasDependencies(const StorageID & table_id)
{
    // Check if all dependencies are attached
    auto view_ids = DatabaseCatalog::instance().getDependentViews(table_id);
    LOG_TEST(log, "Number of attached views {} for {}", view_ids.size(), table_id.getNameForLogs());

    if (view_ids.empty())
        return false;

    // Check the dependencies are ready?
    for (const auto & view_id : view_ids)
    {
        auto view = DatabaseCatalog::instance().tryGetTable(view_id, getContext());
        if (!view)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(view.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;
    }

    return true;
}

void StorageObjectStorageQueue::threadFunc()
{
    if (shutdown_called)
        return;

    try
    {
        const size_t dependencies_count = DatabaseCatalog::instance().getDependentViews(getStorageID()).size();
        if (dependencies_count)
        {
            mv_attached.store(true);
            SCOPE_EXIT({ mv_attached.store(false); });

            LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

            if (streamToViews())
            {
                /// Reset the reschedule interval.
                reschedule_processing_interval_ms = queue_settings->polling_min_timeout_ms;
            }
            else
            {
                /// Increase the reschedule interval.
                reschedule_processing_interval_ms += queue_settings->polling_backoff_ms;
            }

            LOG_DEBUG(log, "Stopped streaming to {} attached views", dependencies_count);
        }
        else
        {
            LOG_TEST(log, "No attached dependencies");
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to process data: {}", getCurrentExceptionMessage(true));
    }

    if (!shutdown_called)
    {
        LOG_TRACE(log, "Reschedule processing thread in {} ms", reschedule_processing_interval_ms);
        task->scheduleAfter(reschedule_processing_interval_ms);
    }
}

bool StorageObjectStorageQueue::streamToViews()
{
    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
    auto queue_context = Context::createCopy(getContext());
    queue_context->makeQueryContext();

    auto file_iterator = createFileIterator(queue_context, nullptr);
    size_t total_rows = 0;

    while (!shutdown_called && !file_iterator->isFinished())
    {
        InterpreterInsertQuery interpreter(insert, queue_context, false, true, true);
        auto block_io = interpreter.execute();
        auto read_from_format_info = prepareReadingFromFormat(
            block_io.pipeline.getHeader().getNames(),
            storage_snapshot,
            supportsSubsetOfColumns(queue_context));

        Pipes pipes;
        std::vector<std::shared_ptr<ObjectStorageQueueSource>> sources;

        pipes.reserve(queue_settings->processing_threads_num);
        sources.reserve(queue_settings->processing_threads_num);

        for (size_t i = 0; i < queue_settings->processing_threads_num; ++i)
        {
            auto source = createSource(
                i/* processor_id */,
                read_from_format_info,
                file_iterator,
                DBMS_DEFAULT_BUFFER_SIZE,
                queue_context,
                false/* commit_once_processed */);

            pipes.emplace_back(source);
            sources.emplace_back(source);
        }
        auto pipe = Pipe::unitePipes(std::move(pipes));

        block_io.pipeline.complete(std::move(pipe));
        block_io.pipeline.setNumThreads(queue_settings->processing_threads_num);
        block_io.pipeline.setConcurrencyControl(queue_context->getSettingsRef().use_concurrency_control);

        std::atomic_size_t rows = 0;
        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });

        try
        {
            CompletedPipelineExecutor executor(block_io.pipeline);
            executor.execute();
        }
        catch (...)
        {
            for (auto & source : sources)
                source->commit(/* success */false, getCurrentExceptionMessage(true));

            file_iterator->releaseFinishedBuckets();
            throw;
        }

        for (auto & source : sources)
            source->commit(/* success */true);

        file_iterator->releaseFinishedBuckets();
        total_rows += rows;
    }

    return total_rows > 0;
}

zkutil::ZooKeeperPtr StorageObjectStorageQueue::getZooKeeper() const
{
    return getContext()->getZooKeeper();
}

std::shared_ptr<StorageObjectStorageQueue::FileIterator> StorageObjectStorageQueue::createFileIterator(ContextPtr local_context, const ActionsDAG::Node * predicate)
{
    auto settings = configuration->getQuerySettings(local_context);
    auto glob_iterator = std::make_unique<StorageObjectStorageSource::GlobIterator>(
        object_storage, configuration, predicate, getVirtualsList(), local_context, nullptr, settings.list_object_keys_size, settings.throw_on_zero_files_match);

    return std::make_shared<FileIterator>(files_metadata, std::move(glob_iterator), shutdown_called, log);
}

}
