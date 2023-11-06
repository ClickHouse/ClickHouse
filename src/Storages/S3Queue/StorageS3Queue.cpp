#include "config.h"

#if USE_AWS_S3
#include <Common/ProfileEvents.h>
#include <IO/S3Common.h>
#include <IO/CompressionMethod.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/S3Queue/S3QueueTableMetadata.h>
#include <Storages/S3Queue/StorageS3Queue.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>
#include <Storages/S3Queue/S3QueueMetadataFactory.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/prepareReadingFromFormat.h>
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
    extern const int REPLICA_ALREADY_EXISTS;
    extern const int INCOMPATIBLE_COLUMNS;
}

namespace
{
    bool containsGlobs(const S3::URI & url)
    {
        return url.key.find_first_of("*?{") != std::string::npos;
    }

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

    void checkAndAdjustSettings(S3QueueSettings & s3queue_settings, const Settings & settings, Poco::Logger * log)
    {
        if (s3queue_settings.mode == S3QueueMode::ORDERED && s3queue_settings.s3queue_processing_threads_num > 1)
        {
            LOG_WARNING(log, "Parallel processing is not yet supported for Ordered mode");
            s3queue_settings.s3queue_processing_threads_num = 1;
        }

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
    const StorageS3::Configuration & configuration_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_)
    : IStorage(table_id_)
    , WithContext(context_)
    , s3queue_settings(std::move(s3queue_settings_))
    , zk_path(chooseZooKeeperPath(table_id_, context_->getSettingsRef(), *s3queue_settings))
    , after_processing(s3queue_settings->after_processing)
    , files_metadata(S3QueueMetadataFactory::instance().getOrCreate(zk_path, *s3queue_settings))
    , configuration{configuration_}
    , format_settings(format_settings_)
    , reschedule_processing_interval_ms(s3queue_settings->s3queue_polling_min_timeout_ms)
    , log(&Poco::Logger::get("StorageS3Queue (" + table_id_.table_name + ")"))
{
    if (configuration.url.key.ends_with('/'))
    {
        configuration.url.key += '*';
    }
    else if (!containsGlobs(configuration.url))
    {
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "S3Queue url must either end with '/' or contain globs");
    }

    checkAndAdjustSettings(*s3queue_settings, context_->getSettingsRef(), log);

    configuration.update(context_);
    FormatFactory::instance().checkFormatName(configuration.format);
    context_->getRemoteHostFilter().checkURL(configuration.url.uri);

    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        auto columns = StorageS3::getTableStructureFromDataImpl(configuration, format_settings, context_);
        storage_metadata.setColumns(columns);
    }
    else
    {
        storage_metadata.setColumns(columns_);
    }
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);

    createOrCheckMetadata(storage_metadata);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathAndFileVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
    task = getContext()->getSchedulePool().createTask("S3QueueStreamingTask", [this] { threadFunc(); });

    LOG_INFO(log, "Using zookeeper path: {}", zk_path.string());
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

    if (task)
    {
        task->deactivate();
    }

    if (files_metadata)
    {
        files_metadata->deactivateCleanupTask();
        files_metadata.reset();
    }
}

void StorageS3Queue::drop()
{
    S3QueueMetadataFactory::instance().remove(zk_path);
}

bool StorageS3Queue::supportsSubsetOfColumns(const ContextPtr & context_) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration.format, context_, format_settings);
}

Pipe StorageS3Queue::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
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

    Pipes pipes;
    const size_t adjusted_num_streams = std::min<size_t>(num_streams, s3queue_settings->s3queue_processing_threads_num);

    auto file_iterator = createFileIterator(local_context, query_info.query);
    for (size_t i = 0; i < adjusted_num_streams; ++i)
        pipes.emplace_back(createSource(file_iterator, column_names, storage_snapshot, max_block_size, local_context));
    return Pipe::unitePipes(std::move(pipes));
}

std::shared_ptr<StorageS3QueueSource> StorageS3Queue::createSource(
    std::shared_ptr<StorageS3Queue::FileIterator> file_iterator,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    size_t max_block_size,
    ContextPtr local_context)
{
    auto configuration_snapshot = updateConfigurationAndGetCopy(local_context);
    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(local_context), getVirtuals());

    auto internal_source = std::make_unique<StorageS3Source>(
        read_from_format_info, configuration.format, getName(), local_context, format_settings,
        max_block_size,
        configuration_snapshot.request_settings,
        configuration_snapshot.compression_method,
        configuration_snapshot.client,
        configuration_snapshot.url.bucket,
        configuration_snapshot.url.version_id,
        configuration_snapshot.url.uri.getHost() + std::to_string(configuration_snapshot.url.uri.getPort()),
        file_iterator, local_context->getSettingsRef().max_download_threads, false, /* query_info */ std::nullopt);

    auto file_deleter = [this, bucket = configuration_snapshot.url.bucket, client = configuration_snapshot.client](const std::string & path)
    {
        S3::DeleteObjectRequest request;
        request.WithKey(path).WithBucket(bucket);
        auto outcome = client->DeleteObject(request);
        if (!outcome.IsSuccess())
        {
            const auto & err = outcome.GetError();
            LOG_ERROR(log, "{} (Code: {})", err.GetMessage(), static_cast<size_t>(err.GetErrorType()));
        }
        else
        {
            LOG_TRACE(log, "Object with path {} was removed from S3", path);
        }
    };
    auto s3_queue_log = s3queue_settings->s3queue_enable_logging_to_s3queue_log ? local_context->getS3QueueLog() : nullptr;
    return std::make_shared<StorageS3QueueSource>(
        getName(), read_from_format_info.source_header, std::move(internal_source),
        files_metadata, after_processing, file_deleter, read_from_format_info.requested_virtual_columns,
        local_context, shutdown_called, table_is_being_dropped, s3_queue_log, getStorageID(), log);
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
        tryLogCurrentException(__PRETTY_FUNCTION__);
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
    auto query_configuration = updateConfigurationAndGetCopy(s3queue_context);

    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, s3queue_context, false, true, true);
    auto block_io = interpreter.execute();
    auto file_iterator = createFileIterator(s3queue_context, nullptr);

    Pipes pipes;
    pipes.reserve(s3queue_settings->s3queue_processing_threads_num);
    for (size_t i = 0; i < s3queue_settings->s3queue_processing_threads_num; ++i)
    {
        auto source = createSource(
            file_iterator, block_io.pipeline.getHeader().getNames(),
            storage_snapshot, DBMS_DEFAULT_BUFFER_SIZE, s3queue_context);

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

StorageS3Queue::Configuration StorageS3Queue::updateConfigurationAndGetCopy(ContextPtr local_context)
{
    configuration.update(local_context);
    return configuration;
}

zkutil::ZooKeeperPtr StorageS3Queue::getZooKeeper() const
{
    return getContext()->getZooKeeper();
}

void StorageS3Queue::createOrCheckMetadata(const StorageInMemoryMetadata & storage_metadata)
{
    auto zookeeper = getZooKeeper();
    zookeeper->createAncestors(zk_path);

    for (size_t i = 0; i < 1000; ++i)
    {
        Coordination::Requests requests;
        if (zookeeper->exists(zk_path / "metadata"))
        {
            checkTableStructure(zk_path, storage_metadata);
        }
        else
        {
            std::string metadata = S3QueueTableMetadata(configuration, *s3queue_settings, storage_metadata).toString();
            requests.emplace_back(zkutil::makeCreateRequest(zk_path, "", zkutil::CreateMode::Persistent));
            requests.emplace_back(zkutil::makeCreateRequest(zk_path / "processed", "", zkutil::CreateMode::Persistent));
            requests.emplace_back(zkutil::makeCreateRequest(zk_path / "failed", "", zkutil::CreateMode::Persistent));
            requests.emplace_back(zkutil::makeCreateRequest(zk_path / "processing", "", zkutil::CreateMode::Persistent));
            requests.emplace_back(zkutil::makeCreateRequest(zk_path / "metadata", metadata, zkutil::CreateMode::Persistent));
        }

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "It looks like the table {} was created by another server at the same moment, will retry", zk_path.string());
            continue;
        }
        else if (code != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(code, requests, responses);
        }
        return;
    }

    throw Exception(
        ErrorCodes::REPLICA_ALREADY_EXISTS,
        "Cannot create table, because it is created concurrently every time or because "
        "of wrong zk_path or because of logical error");
}


void StorageS3Queue::checkTableStructure(const String & zookeeper_prefix, const StorageInMemoryMetadata & storage_metadata)
{
    // Verify that list of columns and table settings match those specified in ZK (/metadata).
    // If not, throw an exception.

    auto zookeeper = getZooKeeper();
    String metadata_str = zookeeper->get(fs::path(zookeeper_prefix) / "metadata");
    auto metadata_from_zk = S3QueueTableMetadata::parse(metadata_str);

    S3QueueTableMetadata old_metadata(configuration, *s3queue_settings, storage_metadata);
    old_metadata.checkEquals(metadata_from_zk);

    auto columns_from_zk = ColumnsDescription::parse(metadata_from_zk.columns);
    const ColumnsDescription & old_columns = storage_metadata.getColumns();
    if (columns_from_zk != old_columns)
    {
        throw Exception(
            ErrorCodes::INCOMPATIBLE_COLUMNS,
            "Table columns structure in ZooKeeper is different from local table structure. Local columns:\n"
            "{}\nZookeeper columns:\n{}",
            old_columns.toString(),
            columns_from_zk.toString());
    }
}

std::shared_ptr<StorageS3Queue::FileIterator> StorageS3Queue::createFileIterator(ContextPtr local_context, ASTPtr query)
{
    auto glob_iterator = std::make_unique<StorageS3QueueSource::GlobIterator>(
        *configuration.client, configuration.url, query, virtual_columns, local_context,
        /* read_keys */nullptr, configuration.request_settings);
    return std::make_shared<FileIterator>(files_metadata, std::move(glob_iterator), shutdown_called);
}

void registerStorageS3QueueImpl(const String & name, StorageFactory & factory)
{
    factory.registerStorage(
        name,
        [](const StorageFactory::Arguments & args)
        {
            if (!args.attach && !args.getLocalContext()->getSettingsRef().allow_experimental_s3queue)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "S3Queue is experimental. "
                                "You can enable it with the `allow_experimental_s3queue` setting.");
            }

            auto & engine_args = args.engine_args;
            if (engine_args.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

            auto configuration = StorageS3::getConfiguration(engine_args, args.getLocalContext());

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
                        LOG_TRACE(&Poco::Logger::get("StorageS3"), "Remove: {}", change.name);
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
                format_settings);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

void registerStorageS3Queue(StorageFactory & factory)
{
    return registerStorageS3QueueImpl("S3Queue", factory);
}

}


#endif
