#include <optional>
#include "config.h"

#include <Common/ProfileEvents.h>
#include <IO/S3Common.h>
#include <IO/CompressionMethod.h>
#include <Formats/FormatFactory.h>
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
#include <Storages/S3Queue/S3QueueTableMetadata.h>
#include <Storages/S3Queue/StorageS3Queue.h>
#include <Storages/S3Queue/S3QueueMetadata.h>
#include <Storages/S3Queue/S3QueueMetadataFactory.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/Utils.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event S3DeleteObjects;
    extern const Event S3ListObjects;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int S3_ERROR;
    extern const int QUERY_NOT_ALLOWED;
}

namespace
{
    std::string chooseZooKeeperPath(const StorageID & table_id, const Settings & settings, const S3QueueSettings & s3queue_settings)
    {
        std::string zk_path_prefix = settings.s3queue_default_zookeeper_path.value;
        if (zk_path_prefix.empty())
            zk_path_prefix = "/";

        std::string result_zk_path;
        if (s3queue_settings.keeper_path.changed)
        {
            /// We do not add table uuid here on purpose.
            result_zk_path = fs::path(zk_path_prefix) / s3queue_settings.keeper_path.value;
        }
        else
        {
            auto database_uuid = DatabaseCatalog::instance().getDatabase(table_id.database_name)->getUUID();
            result_zk_path = fs::path(zk_path_prefix) / toString(database_uuid) / toString(table_id.uuid);
        }
        return zkutil::extractZooKeeperPath(result_zk_path, true);
    }

    void checkAndAdjustSettings(S3QueueSettings & s3queue_settings, const Settings & settings, bool is_attach)
    {
        if (!is_attach && !s3queue_settings.mode.changed)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `mode` (Unordered/Ordered) is not specified, but is required.");
        }
        /// In case !is_attach, we leave Ordered mode as default for compatibility.

        if (!s3queue_settings.s3queue_processing_threads_num)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `s3queue_processing_threads_num` cannot be set to zero");
        }

        if (!s3queue_settings.s3queue_enable_logging_to_s3queue_log.changed)
        {
            s3queue_settings.s3queue_enable_logging_to_s3queue_log = settings.s3queue_enable_logging_to_s3queue_log;
        }

        if (s3queue_settings.s3queue_cleanup_interval_min_ms > s3queue_settings.s3queue_cleanup_interval_max_ms)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Setting `s3queue_cleanup_interval_min_ms` ({}) must be less or equal to `s3queue_cleanup_interval_max_ms` ({})",
                            s3queue_settings.s3queue_cleanup_interval_min_ms, s3queue_settings.s3queue_cleanup_interval_max_ms);
        }
    }
}

StorageS3Queue::StorageS3Queue(
    std::unique_ptr<S3QueueSettings> s3queue_settings_,
    const ConfigurationPtr configuration_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    ASTStorage * /* engine_args */,
    LoadingStrictnessLevel mode)
    : IStorage(table_id_)
    , WithContext(context_)
    , s3queue_settings(std::move(s3queue_settings_))
    , zk_path(chooseZooKeeperPath(table_id_, context_->getSettingsRef(), *s3queue_settings))
    , configuration{configuration_}
    , format_settings(format_settings_)
    , reschedule_processing_interval_ms(s3queue_settings->s3queue_polling_min_timeout_ms)
    , log(getLogger("StorageS3Queue (" + table_id_.getFullTableName() + ")"))
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
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "S3Queue url must either end with '/' or contain globs");
    }

    checkAndAdjustSettings(*s3queue_settings, context_->getSettingsRef(), mode > LoadingStrictnessLevel::CREATE);

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
    task = getContext()->getSchedulePool().createTask("S3QueueStreamingTask", [this] { threadFunc(); });

    /// Get metadata manager from S3QueueMetadataFactory,
    /// it will increase the ref count for the metadata object.
    /// The ref count is decreased when StorageS3Queue::drop() method is called.
    files_metadata = S3QueueMetadataFactory::instance().getOrCreate(zk_path, *s3queue_settings);
    try
    {
        files_metadata->initialize(configuration_, storage_metadata);
    }
    catch (...)
    {
        S3QueueMetadataFactory::instance().remove(zk_path);
        throw;
    }
}

void StorageS3Queue::startup()
{
    if (task)
        task->activateAndSchedule();
}

void StorageS3Queue::shutdown(bool is_drop)
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

void StorageS3Queue::drop()
{
    S3QueueMetadataFactory::instance().remove(zk_path);
}

bool StorageS3Queue::supportsSubsetOfColumns(const ContextPtr & context_) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context_, format_settings);
}

class ReadFromS3Queue : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromS3Queue"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    ReadFromS3Queue(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        ReadFromFormatInfo info_,
        std::shared_ptr<StorageS3Queue> storage_,
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
    std::shared_ptr<StorageS3Queue> storage;
    size_t max_block_size;

    std::shared_ptr<StorageS3Queue::FileIterator> iterator;

    void createIterator(const ActionsDAG::Node * predicate);
};

void ReadFromS3Queue::createIterator(const ActionsDAG::Node * predicate)
{
    if (iterator)
        return;

    iterator = storage->createFileIterator(context, predicate);
}


void ReadFromS3Queue::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    createIterator(predicate);
}

void StorageS3Queue::read(
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

    auto this_ptr = std::static_pointer_cast<StorageS3Queue>(shared_from_this());
    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(local_context));

    auto reading = std::make_unique<ReadFromS3Queue>(
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

void ReadFromS3Queue::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes;
    const size_t adjusted_num_streams = storage->s3queue_settings->s3queue_processing_threads_num;

    createIterator(nullptr);
    for (size_t i = 0; i < adjusted_num_streams; ++i)
        pipes.emplace_back(storage->createSource(
                               i,
                               info,
                               iterator,
                               max_block_size, context));

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(info.source_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

std::shared_ptr<StorageS3QueueSource> StorageS3Queue::createSource(
    size_t processor_id,
    const ReadFromFormatInfo & info,
    std::shared_ptr<StorageS3Queue::FileIterator> file_iterator,
    size_t max_block_size,
    ContextPtr local_context)
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
    auto s3_queue_log = s3queue_settings->s3queue_enable_logging_to_s3queue_log ? local_context->getS3QueueLog() : nullptr;
    return std::make_shared<StorageS3QueueSource>(
        getName(),
        processor_id,
        info.source_header,
        std::move(internal_source),
        files_metadata,
        s3queue_settings->after_processing,
        file_deleter,
        info.requested_virtual_columns,
        local_context,
        shutdown_called,
        table_is_being_dropped,
        s3_queue_log,
        getStorageID(),
        log);
}

bool StorageS3Queue::hasDependencies(const StorageID & table_id)
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

void StorageS3Queue::threadFunc()
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
                reschedule_processing_interval_ms = s3queue_settings->s3queue_polling_min_timeout_ms;
            }
            else
            {
                /// Increase the reschedule interval.
                reschedule_processing_interval_ms += s3queue_settings->s3queue_polling_backoff_ms;
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
        LOG_TRACE(log, "Reschedule S3 Queue processing thread in {} ms", reschedule_processing_interval_ms);
        task->scheduleAfter(reschedule_processing_interval_ms);
    }
}

bool StorageS3Queue::streamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    auto s3queue_context = Context::createCopy(getContext());
    s3queue_context->makeQueryContext();

    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, s3queue_context, false, true, true);
    auto block_io = interpreter.execute();
    auto file_iterator = createFileIterator(s3queue_context, nullptr);

    auto read_from_format_info = prepareReadingFromFormat(block_io.pipeline.getHeader().getNames(), storage_snapshot, supportsSubsetOfColumns(s3queue_context));

    Pipes pipes;
    pipes.reserve(s3queue_settings->s3queue_processing_threads_num);
    for (size_t i = 0; i < s3queue_settings->s3queue_processing_threads_num; ++i)
    {
        auto source = createSource(i, read_from_format_info, file_iterator, DBMS_DEFAULT_BUFFER_SIZE, s3queue_context);
        pipes.emplace_back(std::move(source));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));

    block_io.pipeline.complete(std::move(pipe));
    block_io.pipeline.setNumThreads(s3queue_settings->s3queue_processing_threads_num);
    block_io.pipeline.setConcurrencyControl(s3queue_context->getSettingsRef().use_concurrency_control);

    std::atomic_size_t rows = 0;
    block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });

    CompletedPipelineExecutor executor(block_io.pipeline);
    executor.execute();

    return rows > 0;
}

zkutil::ZooKeeperPtr StorageS3Queue::getZooKeeper() const
{
    return getContext()->getZooKeeper();
}

std::shared_ptr<StorageS3Queue::FileIterator> StorageS3Queue::createFileIterator(ContextPtr local_context, const ActionsDAG::Node * predicate)
{
    auto settings = configuration->getQuerySettings(local_context);
    auto glob_iterator = std::make_unique<StorageObjectStorageSource::GlobIterator>(
        object_storage, configuration, predicate, getVirtualsList(), local_context, nullptr, settings.list_object_keys_size, settings.throw_on_zero_files_match);

    return std::make_shared<FileIterator>(files_metadata, std::move(glob_iterator), shutdown_called, log);
}

#if USE_AWS_S3
void registerStorageS3Queue(StorageFactory & factory)
{
    factory.registerStorage(
        "S3Queue",
        [](const StorageFactory::Arguments & args)
        {
            auto & engine_args = args.engine_args;
            if (engine_args.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

            auto configuration = std::make_shared<StorageS3Configuration>();
            StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, args.getContext(), false);

            // Use format settings from global server context + settings from
            // the SETTINGS clause of the create query. Settings from current
            // session and user are ignored.
            std::optional<FormatSettings> format_settings;

            auto s3queue_settings = std::make_unique<S3QueueSettings>();
            if (args.storage_def->settings)
            {
                s3queue_settings->loadFromQuery(*args.storage_def);
                FormatFactorySettings user_format_settings;

                // Apply changed settings from global context, but ignore the
                // unknown ones, because we only have the format settings here.
                const auto & changes = args.getContext()->getSettingsRef().changes();
                for (const auto & change : changes)
                {
                    if (user_format_settings.has(change.name))
                        user_format_settings.set(change.name, change.value);
                    else
                        LOG_TRACE(getLogger("StorageS3"), "Remove: {}", change.name);
                    args.storage_def->settings->changes.removeSetting(change.name);
                }

                for (const auto & change : args.storage_def->settings->changes)
                {
                    if (user_format_settings.has(change.name))
                        user_format_settings.applyChange(change);
                }
                format_settings = getFormatSettings(args.getContext(), user_format_settings);
            }
            else
            {
                format_settings = getFormatSettings(args.getContext());
            }

            return std::make_shared<StorageS3Queue>(
                std::move(s3queue_settings),
                std::move(configuration),
                args.table_id,
                args.columns,
                args.constraints,
                args.comment,
                args.getContext(),
                format_settings,
                args.storage_def,
                args.mode);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}
#endif

}
