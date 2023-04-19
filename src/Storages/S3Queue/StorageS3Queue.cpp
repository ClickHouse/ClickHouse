#include "config.h"
#include <Common/ProfileEvents.h>
#include "IO/ParallelReadBuffer.h"
#include "IO/IOThreadPool.h"
#include "Parsers/ASTCreateQuery.h"

#if USE_AWS_S3

#include <Common/isValidUTF8.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <Functions/FunctionsConversion.h>

#include <IO/S3Common.h>
#include <IO/S3/Requests.h>

#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>
#include <Storages/S3Queue/StorageS3Queue.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/getVirtualsForStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Storages/ReadFromStorageProgress.h>
#include <Storages/StorageMaterializedView.h>


#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/StoredObject.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/narrowPipe.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <DataTypes/DataTypeString.h>

#include <aws/core/auth/AWSCredentials.h>

#include <Common/parseGlobs.h>
#include <Common/quoteString.h>
#include <re2/re2.h>

#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace ProfileEvents
{
extern const Event S3DeleteObjects;
extern const Event S3ListObjects;
}

namespace DB
{

static const String PARTITION_ID_WILDCARD = "{_partition_id}";

static const std::unordered_set<std::string_view> required_configuration_keys = {
    "url",
};
static const std::unordered_set<std::string_view> optional_configuration_keys = {
    "format",
    "compression",
    "compression_method",
    "structure",
    "access_key_id",
    "secret_access_key",
    "filename",
    "use_environment_credentials",
    "max_single_read_retries",
    "min_upload_part_size",
    "upload_part_size_multiply_factor",
    "upload_part_size_multiply_parts_count_threshold",
    "max_single_part_upload_size",
    "max_connections",
};

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int S3_ERROR;
    extern const int UNEXPECTED_EXPRESSION;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int FILE_DOESNT_EXIST;
    extern const int QUERY_NOT_ALLOWED;
    extern const int NO_ZOOKEEPER;
    extern const int REPLICA_ALREADY_EXISTS;
}

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

const String StorageS3Queue::default_zookeeper_name = "default";


StorageS3Queue::StorageS3Queue(
    const String & zookeeper_path_,
    const String & mode_,
    const StorageS3::Configuration & configuration_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , WithContext(context_)
    , s3_configuration{configuration_}
    , keys({s3_configuration.url.key})
    , format_name(configuration_.format)
    , compression_method(configuration_.compression_method)
    , name(s3_configuration.url.storage_name)
    , distributed_processing(distributed_processing_)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
    , is_key_with_globs(s3_configuration.url.key.find_first_of("*?{") != std::string::npos)
    , log(&Poco::Logger::get("StorageS3Queue (" + table_id_.table_name + ")"))
    , mode(mode_)
    , zookeeper_name(zkutil::extractZooKeeperName(zookeeper_path_))
    , zookeeper_path(zkutil::extractZooKeeperPath(zookeeper_path_, /* check_starts_with_slash */ true, log))
{
    FormatFactory::instance().checkFormatName(format_name);
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(s3_configuration.url.uri);

    StorageInMemoryMetadata storage_metadata;

    StorageS3::updateConfiguration(context_, s3_configuration);
    if (columns_.empty())
    {
        auto columns = StorageS3::getTableStructureFromDataImpl(
            format_name,
            s3_configuration,
            compression_method,
            is_key_with_globs,
            format_settings,
            context_);

        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto columns = storage_metadata.getSampleBlock().getNamesAndTypesList();
    virtual_columns = getVirtualsForStorage(columns, default_virtuals);
    for (const auto & column : virtual_columns)
        virtual_block.insert({column.type->createColumn(), column.type, column.name});

    auto thread = context_->getSchedulePool().createTask("S3QueueStreamingTask", [this] { threadFunc(); });
    setZooKeeper();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    createTableIfNotExists(metadata_snapshot);
    task = std::make_shared<TaskContext>(std::move(thread));
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
    size_t num_streams)
{
//    if (!local_context->getSettingsRef().stream_like_engine_allow_direct_select)
//        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED,
//                        "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`");

    if (mv_attached)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageRabbitMQ with attached materialized views");

    auto query_s3_configuration = StorageS3::copyAndUpdateConfiguration(local_context, s3_configuration);

    bool has_wildcards =
        query_s3_configuration.url.bucket.find(PARTITION_ID_WILDCARD) != String::npos
        || keys.back().find(PARTITION_ID_WILDCARD) != String::npos;

    if (partition_by && has_wildcards)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading from a partitioned S3 storage is not implemented yet");

    Pipes pipes;

    std::unordered_set<String> column_names_set(column_names.begin(), column_names.end());
    std::vector<NameAndTypePair> requested_virtual_columns;

    for (const auto & virtual_column : getVirtuals())
    {
        if (column_names_set.contains(virtual_column.name))
            requested_virtual_columns.push_back(virtual_column);
    }

    std::shared_ptr<StorageS3Source::IIterator> iterator_wrapper = StorageS3::createFileIterator(
        query_s3_configuration,
        keys,
        is_key_with_globs,
        distributed_processing,
        local_context,
        query_info.query,
        virtual_block);

    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns())
    {
        auto fetch_columns = column_names;
        const auto & virtuals = getVirtuals();
        std::erase_if(
            fetch_columns,
            [&](const String & col)
            { return std::any_of(virtuals.begin(), virtuals.end(), [&](const NameAndTypePair & virtual_col){ return col == virtual_col.name; }); });

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
    // const size_t max_download_threads = 1;
    LOG_WARNING(log, "num_streams");

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageS3Source>(
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
            max_download_threads));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    LOG_WARNING(log, "unitePipes");

    narrowPipe(pipe, num_streams);
    LOG_WARNING(log, "narrowPipe");

    return pipe;
}

NamesAndTypesList StorageS3Queue::getVirtuals() const
{
    return virtual_columns;
}

Names StorageS3Queue::getVirtualColumnNames()
{
    return {"_path", "_file"};
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
    LOG_TRACE(log, "Deactivating background tasks");

    if (task)
    {
        task->stream_cancelled = true;

        /// Reader thread may wait for wake up
//        wakeUp();

        LOG_TRACE(log, "Waiting for cleanup");
        task->holder->deactivate();
        /// If no reading call and threadFunc, the log files will never
        /// be opened, also just leave the work of close files and
        /// store meta to streams. because if we close files in here,
        /// may result in data race with unfinishing reading pipeline
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
            // auto start_time = std::chrono::steady_clock::now();

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

                //                if (streamToViews())
                //                {
                //                    LOG_TRACE(log, "Stream stalled. Reschedule.");
                //                    break;
                //                }

                //                auto ts = std::chrono::steady_clock::now();
                //                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts-start_time);
                //                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                //                {
                //                    LOG_TRACE(log, "Thread work duration limit exceeded. Reschedule.");
                //                    reschedule = true;
                //                    break;
                //                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    mv_attached.store(false);

    // Wait for attached views
    if (reschedule && !shutdown_called)
    {
        LOG_TRACE(log, "Reschedule S3 Queue thread func.");
        /// Reschedule with backoff.
        task->holder->scheduleAfter(milliseconds_to_wait);
    }
}


void StorageS3Queue::setZooKeeper()
{
    std::lock_guard lock(current_zookeeper_mutex);
    LOG_WARNING(log, "zookeeper name {}", zookeeper_name);
    if (zookeeper_name == default_zookeeper_name)
    {
        current_zookeeper = getContext()->getZooKeeper();
    }
    else
    {
        current_zookeeper = getContext()->getAuxiliaryZooKeeper(zookeeper_name);
    }
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

    for (size_t i = 0; i < 1000; ++i)
    {
        if (zookeeper->exists(zookeeper_path + ""))
        {
            LOG_DEBUG(log, "This table {} is already created, will add new replica", zookeeper_path);
            return false;
        }
        /// We write metadata of table so that the replicas can check table parameters with them.
        // String metadata_str = ReplicatedMergeTreeTableMetadata(*this, metadata_snapshot).toString();

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/processed", "",
                                                   zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/failed", "",
                                                   zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/processing", "",
                                                   zkutil::CreateMode::Ephemeral));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/columns", metadata_snapshot->getColumns().toString(),
                                                   zkutil::CreateMode::Persistent));
//        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", metadata_str,
//                                                   zkutil::CreateMode::Persistent));

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

        return true;
    }

    /// Do not use LOGICAL_ERROR code, because it may happen if user has specified wrong zookeeper_path
    throw Exception(ErrorCodes::REPLICA_ALREADY_EXISTS,
                    "Cannot create table, because it is created concurrently every time or because "
                    "of wrong zookeeper_path or because of logical error");
}

void registerStorageS3QueueImpl(const String & name, StorageFactory & factory)
{
    factory.registerStorage(name, [](const StorageFactory::Arguments & args)
                            {
                                auto & engine_args = args.engine_args;
                                if (engine_args.empty())
                                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

                                auto configuration = StorageS3::getConfiguration(engine_args, args.getLocalContext());
                                // Use format settings from global server context + settings from
                                // the SETTINGS clause of the create query. Settings from current
                                // session and user are ignored.
                                auto s3queue_settings = std::make_unique<S3QueueSettings>();
                                std::optional<FormatSettings> format_settings;

                                if (args.storage_def->settings)
                                {
                                    s3queue_settings->loadFromQuery(*args.storage_def);

                                }
                                format_settings = getFormatSettings(args.getContext());

                                ASTPtr partition_by;
                                if (args.storage_def->partition_by)
                                    partition_by = args.storage_def->partition_by->clone();

                                String keeper_path = s3queue_settings->keeper_path;
                                String mode = s3queue_settings->mode;

                                return std::make_shared<StorageS3Queue>(
                                    keeper_path,
                                    mode,
                                    configuration,
                                    args.table_id,
                                    args.columns,
                                    args.constraints,
                                    args.comment,
                                    args.getContext(),
                                    format_settings,
                                    /* distributed_processing_ */false,
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
