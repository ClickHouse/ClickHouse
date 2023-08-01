#include "config.h"


#if USE_AWS_S3

#    include <Databases/DatabaseReplicated.h>
#    include <IO/WriteBuffer.h>
#    include <IO/WriteHelpers.h>
#    include <Interpreters/InterpreterInsertQuery.h>
#    include <Processors/Executors/CompletedPipelineExecutor.h>
#    include <Common/ProfileEvents.h>
#    include <Common/ZooKeeper/ZooKeeper.h>
#    include <Common/isValidUTF8.h>
#    include "IO/ParallelReadBuffer.h"

#    include <Functions/FunctionsConversion.h>

#    include <IO/S3Common.h>

#    include <Interpreters/TreeRewriter.h>

#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTInsertQuery.h>

#    include <Storages/NamedCollectionsHelpers.h>
#    include <Storages/PartitionedSink.h>
#    include <Storages/S3Queue/S3QueueSource.h>
#    include <Storages/S3Queue/S3QueueTableMetadata.h>
#    include <Storages/S3Queue/StorageS3Queue.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/StorageMaterializedView.h>
#    include <Storages/StorageS3.h>
#    include <Storages/StorageSnapshot.h>
#    include <Storages/VirtualColumnUtils.h>
#    include <Storages/getVirtualsForStorage.h>
#    include <Common/NamedCollections/NamedCollections.h>


#    include <Formats/FormatFactory.h>

#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/IOutputFormat.h>
#    include <Processors/Transforms/AddingDefaultsTransform.h>

#    include <QueryPipeline/QueryPipelineBuilder.h>

#    include <DataTypes/DataTypeString.h>

#    include <Common/parseGlobs.h>

#    include <filesystem>
#    include <Processors/ISource.h>
#    include <Processors/Sinks/SinkToStorage.h>
#    include <QueryPipeline/Pipe.h>

namespace fs = std::filesystem;

namespace ProfileEvents
{
extern const Event S3DeleteObjects;
extern const Event S3ListObjects;
}

namespace DB
{

static const String PARTITION_ID_WILDCARD = "{_partition_id}";
static const auto MAX_THREAD_WORK_DURATION_MS = 60000;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int S3_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int QUERY_NOT_ALLOWED;
    extern const int NO_ZOOKEEPER;
    extern const int REPLICA_ALREADY_EXISTS;
    extern const int INCOMPATIBLE_COLUMNS;
}


StorageS3Queue::StorageS3Queue(
    std::unique_ptr<S3QueueSettings> s3queue_settings_,
    const StorageS3::Configuration & configuration_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , WithContext(context_)
    , s3queue_settings(std::move(s3queue_settings_))
    , s3_configuration{configuration_}
    , keys({s3_configuration.url.key})
    , mode(s3queue_settings->mode)
    , after_processing(s3queue_settings->after_processing)
    , milliseconds_to_wait(s3queue_settings->s3queue_polling_min_timeout_ms)
    , format_name(configuration_.format)
    , compression_method(configuration_.compression_method)
    , name(s3_configuration.url.storage_name)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
    , log(&Poco::Logger::get("StorageS3Queue (" + table_id_.table_name + ")"))
{
    if (!withGlobs())
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "S3Queue engine can read only from url with globs");

    String setting_zookeeper_path = s3queue_settings->keeper_path;
    if (setting_zookeeper_path.empty())
    {
        auto database = DatabaseCatalog::instance().getDatabase(table_id_.database_name);
        bool is_in_replicated_database = database->getEngineName() == "Replicated";

        auto default_path = getContext()->getSettingsRef().s3queue_default_zookeeper_path.value;
        String zk_path_prefix;

        if (!default_path.empty())
        {
            zk_path_prefix = default_path;
        }
        else if (is_in_replicated_database)
        {
            LOG_INFO(log, "S3Queue engine zookeeper path is not specified. "
                     "Using replicated database zookeeper path");

            zk_path_prefix = fs::path(assert_cast<const DatabaseReplicated *>(database.get())->getZooKeeperPath()) / "s3queue";
        }
        else
        {
            throw Exception(ErrorCodes::NO_ZOOKEEPER,
                            "S3Queue keeper_path engine setting not specified, "
                            "s3queue_default_zookeeper_path_prefix not specified");
        }

        zookeeper_path = zkutil::extractZooKeeperPath(
            fs::path(zk_path_prefix) / toString(table_id_.uuid), /* check_starts_with_slash */ true, log);
    }
    else
    {
        /// We do not add table uuid here on purpose.
        zookeeper_path = zkutil::extractZooKeeperPath(s3queue_settings->keeper_path.value, /* check_starts_with_slash */ true, log);
    }

    LOG_INFO(log, "Using zookeeper path: {}", zookeeper_path);

    FormatFactory::instance().checkFormatName(format_name);
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(s3_configuration.url.uri);
    StorageInMemoryMetadata storage_metadata;
    s3_configuration.update(context_);

    if (columns_.empty())
    {
        auto columns = StorageS3::getTableStructureFromDataImpl(s3_configuration, format_settings, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    setZooKeeper();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const bool is_first_replica = createTableIfNotExists(metadata_snapshot);

    if (!is_first_replica)
    {
        checkTableStructure(zookeeper_path, metadata_snapshot);
    }

    queue_holder = std::make_unique<S3QueueHolder>(
        zookeeper_path,
        mode,
        getContext(),
        s3queue_settings->s3queue_tracked_files_limit.value,
        s3queue_settings->s3queue_tracked_file_ttl_sec.value,
        s3queue_settings->s3queue_loading_retries.value);

    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto columns = storage_metadata.getSampleBlock().getNamesAndTypesList();
    virtual_columns = getVirtualsForStorage(columns, default_virtuals);
    for (const auto & column : virtual_columns)
        virtual_block.insert({column.type->createColumn(), column.type, column.name});

    auto poll_thread = getContext()->getSchedulePool().createTask("S3QueueStreamingTask", [this] { threadFunc(); });
    task = std::make_shared<TaskContext>(std::move(poll_thread));
}


bool StorageS3Queue::supportsSubcolumns() const
{
    return FormatFactory::instance().checkIfFormatSupportsSubcolumns(format_name);
}

bool StorageS3Queue::supportsSubsetOfColumns() const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name);
}

Pipe StorageS3Queue::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /* num_streams */)
{
    if (!local_context->getSettingsRef().stream_like_engine_allow_direct_select)
        throw Exception(
            ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`");

    if (mv_attached)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageS3Queue with attached materialized views");

    auto query_s3_configuration = updateConfigurationAndGetCopy(local_context);

    Pipes pipes;

    std::unordered_set<String> column_names_set(column_names.begin(), column_names.end());
    std::vector<NameAndTypePair> requested_virtual_columns;

    for (const auto & virtual_column : getVirtuals())
    {
        if (column_names_set.contains(virtual_column.name))
            requested_virtual_columns.push_back(virtual_column);
    }

    std::shared_ptr<StorageS3Source::IIterator> iterator_wrapper = createFileIterator(local_context, query_info.query);

    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns())
    {
        auto fetch_columns = column_names;
        const auto & virtuals = getVirtuals();
        std::erase_if(
            fetch_columns,
            [&](const String & col)
            {
                return std::any_of(
                    virtuals.begin(), virtuals.end(), [&](const NameAndTypePair & virtual_col) { return col == virtual_col.name; });
            });

        if (fetch_columns.empty())
            fetch_columns.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()).name);

        columns_description = storage_snapshot->getDescriptionForColumns(fetch_columns);
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    const size_t max_download_threads = local_context->getSettingsRef().max_download_threads;
    auto zookeeper = getZooKeeper();

    return Pipe(std::make_shared<StorageS3QueueSource>(
        requested_virtual_columns,
        format_name,
        getName(),
        block_for_format,
        local_context,
        format_settings,
        columns_description,
        max_block_size,
        query_s3_configuration.request_settings,
        compression_method,
        query_s3_configuration.client,
        query_s3_configuration.url.bucket,
        query_s3_configuration.url.version_id,
        iterator_wrapper,
        queue_holder,
        after_processing,
        max_download_threads));
}

SinkToStoragePtr StorageS3Queue::write(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, bool)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Write is not supported by storage {}", getName());
}

void StorageS3Queue::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate is not supported by storage {}", getName());
}

NamesAndTypesList StorageS3Queue::getVirtuals() const
{
    return virtual_columns;
}

bool StorageS3Queue::supportsPartitionBy() const
{
    return true;
}

void StorageS3Queue::startup()
{
    if (task)
        task->holder->activateAndSchedule();
}

void StorageS3Queue::shutdown()
{
    shutdown_called = true;
    if (task)
    {
        task->stream_cancelled = true;
        task->holder->deactivate();
    }
}

size_t StorageS3Queue::getTableDependentCount() const
{
    auto table_id = getStorageID();
    // Check if at least one direct dependency is attached
    return DatabaseCatalog::instance().getDependentViews(table_id).size();
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
    bool reschedule = true;
    try
    {
        auto table_id = getStorageID();

        auto dependencies_count = getTableDependentCount();
        if (dependencies_count)
        {
            auto start_time = std::chrono::steady_clock::now();

            mv_attached.store(true);
            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!task->stream_cancelled)
            {
                if (!hasDependencies(table_id))
                {
                    /// For this case, we can not wait for watch thread to wake up
                    reschedule = true;
                    break;
                }

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);
                streamToViews();

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts - start_time);
                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                {
                    LOG_TRACE(log, "Thread work duration limit exceeded. Reschedule.");
                    reschedule = true;
                    break;
                }

                milliseconds_to_wait = s3queue_settings->s3queue_polling_min_timeout_ms;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    mv_attached.store(false);

    if (reschedule && !shutdown_called)
    {
        LOG_TRACE(log, "Reschedule S3 Queue thread func.");
        /// Reschedule with backoff.
        if (milliseconds_to_wait < s3queue_settings->s3queue_polling_max_timeout_ms)
            milliseconds_to_wait += s3queue_settings->s3queue_polling_backoff_ms;
        task->holder->scheduleAfter(milliseconds_to_wait);
    }
}


void StorageS3Queue::streamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    size_t block_size = 100;

    auto s3queue_context = Context::createCopy(getContext());
    s3queue_context->makeQueryContext();
    auto query_s3_configuration = updateConfigurationAndGetCopy(s3queue_context);

    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, s3queue_context, false, true, true);
    auto block_io = interpreter.execute();
    auto column_names = block_io.pipeline.getHeader().getNames();

    // Create a stream for each consumer and join them in a union stream
    std::vector<NameAndTypePair> requested_virtual_columns;

    for (const auto & virtual_column : getVirtuals())
    {
        requested_virtual_columns.push_back(virtual_column);
    }

    std::shared_ptr<StorageS3Source::IIterator> iterator_wrapper = createFileIterator(s3queue_context, nullptr);
    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns())
    {
        auto fetch_columns = column_names;
        const auto & virtuals = getVirtuals();
        std::erase_if(
            fetch_columns,
            [&](const String & col)
            {
                return std::any_of(
                    virtuals.begin(), virtuals.end(), [&](const NameAndTypePair & virtual_col) { return col == virtual_col.name; });
            });

        if (fetch_columns.empty())
            fetch_columns.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()).name);

        columns_description = storage_snapshot->getDescriptionForColumns(fetch_columns);
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    const size_t max_download_threads = s3queue_context->getSettingsRef().max_download_threads;

    Pipes pipes;

    auto zookeeper = getZooKeeper();
    auto pipe = Pipe(std::make_shared<StorageS3QueueSource>(
        requested_virtual_columns,
        format_name,
        getName(),
        block_for_format,
        s3queue_context,
        format_settings,
        columns_description,
        block_size,
        query_s3_configuration.request_settings,
        compression_method,
        query_s3_configuration.client,
        query_s3_configuration.url.bucket,
        query_s3_configuration.url.version_id,
        iterator_wrapper,
        queue_holder,
        after_processing,
        max_download_threads));


    std::atomic_size_t rows = 0;
    {
        block_io.pipeline.complete(std::move(pipe));
        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
}

StorageS3Queue::Configuration StorageS3Queue::updateConfigurationAndGetCopy(ContextPtr local_context)
{
    s3_configuration.update(local_context);
    return s3_configuration;
}

void StorageS3Queue::setZooKeeper()
{
    std::lock_guard lock(current_zookeeper_mutex);
    current_zookeeper = getContext()->getZooKeeper();
}

zkutil::ZooKeeperPtr StorageS3Queue::tryGetZooKeeper() const
{
    std::lock_guard lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr StorageS3Queue::getZooKeeper() const
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "Cannot get ZooKeeper");
    return res;
}


bool StorageS3Queue::createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot)
{
    auto zookeeper = getZooKeeper();
    zookeeper->createAncestors(zookeeper_path);

    for (size_t i = 0; i < zk_create_table_retries; ++i)
    {
        Coordination::Requests ops;
        bool is_first_replica = true;
        if (zookeeper->exists(zookeeper_path + "/metadata"))
        {
            if (!zookeeper->exists(zookeeper_path + "/processing"))
                ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/processing", "", zkutil::CreateMode::Ephemeral));
            LOG_DEBUG(log, "This table {} is already created, will use existing metadata for checking engine settings", zookeeper_path);
            is_first_replica = false;
        }
        else
        {
            String metadata_str = S3QueueTableMetadata(s3_configuration, *s3queue_settings).toString();
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/processed", "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/failed", "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/processing", "", zkutil::CreateMode::Ephemeral));
            ops.emplace_back(zkutil::makeCreateRequest(
                zookeeper_path + "/columns", metadata_snapshot->getColumns().toString(), zkutil::CreateMode::Persistent));

            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", metadata_str, zkutil::CreateMode::Persistent));
        }

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "It looks like the table {} was created by another server at the same moment, will retry", zookeeper_path);
            continue;
        }
        else if (code != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }

        return is_first_replica;
    }

    throw Exception(
        ErrorCodes::REPLICA_ALREADY_EXISTS,
        "Cannot create table, because it is created concurrently every time or because "
        "of wrong zookeeper_path or because of logical error");
}


/** Verify that list of columns and table settings match those specified in ZK (/metadata).
  * If not, throw an exception.
  */
void StorageS3Queue::checkTableStructure(const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot)
{
    auto zookeeper = getZooKeeper();

    S3QueueTableMetadata old_metadata(s3_configuration, *s3queue_settings);

    Coordination::Stat metadata_stat;
    String metadata_str = zookeeper->get(fs::path(zookeeper_prefix) / "metadata", &metadata_stat);
    auto metadata_from_zk = S3QueueTableMetadata::parse(metadata_str);
    old_metadata.checkEquals(metadata_from_zk);

    Coordination::Stat columns_stat;
    auto columns_from_zk = ColumnsDescription::parse(zookeeper->get(fs::path(zookeeper_prefix) / "columns", &columns_stat));

    const ColumnsDescription & old_columns = metadata_snapshot->getColumns();
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


std::shared_ptr<StorageS3QueueSource::IIterator>
StorageS3Queue::createFileIterator(ContextPtr local_context, ASTPtr query)
{
    auto it = std::make_shared<StorageS3QueueSource::QueueGlobIterator>(
        *s3_configuration.client,
        s3_configuration.url,
        query,
        virtual_block,
        local_context,
        s3queue_settings->s3queue_polling_size.value,
        s3_configuration.request_settings);

    auto lock = queue_holder->acquireLock();
    S3QueueHolder::S3FilesCollection files_to_skip = queue_holder->getProcessedAndFailedFiles();

    Strings files_to_process;
    if (mode == S3QueueMode::UNORDERED)
    {
        files_to_process = it->filterProcessingFiles(mode, files_to_skip);
    }
    else
    {
        String max_processed_file = queue_holder->getMaxProcessedFile();
        files_to_process = it->filterProcessingFiles(mode, files_to_skip, max_processed_file);
    }

    LOG_TEST(log, "Found files to process: {}", fmt::join(files_to_process, ", "));

    queue_holder->setFilesProcessing(files_to_process);
    return it;
}

void StorageS3Queue::drop()
{
    auto zk_client = getZooKeeper();
    if (zk_client->exists(zookeeper_path))
        zk_client->removeRecursive(zookeeper_path);
}

void registerStorageS3QueueImpl(const String & name, StorageFactory & factory)
{
    factory.registerStorage(
        name,
        [](const StorageFactory::Arguments & args)
        {
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

            ASTPtr partition_by;
            if (args.storage_def->partition_by)
                partition_by = args.storage_def->partition_by->clone();

            return std::make_shared<StorageS3Queue>(
                std::move(s3queue_settings),
                std::move(configuration),
                args.table_id,
                args.columns,
                args.constraints,
                args.comment,
                args.getContext(),
                format_settings,
                partition_by);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true, // for partition by
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
