#include <optional>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/IPartitionStrategy.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>

#include <Storages/VirtualColumnUtils.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/extractTableFunctionFromSelectQuery.h>
#include <Storages/ObjectStorage/StorageObjectStorageStableTaskDistributor.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool use_hive_partitioning;
    extern const SettingsBool cluster_function_process_archive_on_multiple_nodes;
    extern const SettingsObjectStorageGranularityLevel cluster_table_function_split_granularity;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsString object_storage_cluster;
    extern const SettingsInt64 delta_lake_snapshot_start_version;
    extern const SettingsInt64 delta_lake_snapshot_end_version;
    extern const SettingsUInt64 lock_object_storage_task_distribution_ms;
    extern const SettingsBool allow_experimental_iceberg_read_optimization;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SETTING_VALUE;
}

String StorageObjectStorageCluster::getPathSample(ContextPtr context)
{
    auto query_settings = configuration->getQuerySettings(context);
    /// We don't want to throw an exception if there are no files with specified path.
    query_settings.throw_on_zero_files_match = false;

    if (!configuration->isArchive())
    {
        const auto & path = configuration->getPathForRead();
        if (!path.hasGlobs())
            return path.path;
    }

    auto file_iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        query_settings,
        object_storage,
        nullptr, // storage_metadata
        false, // distributed_processing
        context,
        {}, // predicate
        {},
        {}, // virtual_columns
        {}, // hive_columns
        nullptr, // read_keys
        {}, // file_progress_callback
        false, // ignore_archive_globs
        true // skip_object_metadata
    );

    if (auto file = file_iterator->next(0))
        return file->getPath();

    return "";
}

StorageObjectStorageCluster::StorageObjectStorageCluster(
    const String & cluster_name_,
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_in_table_or_function_definition,
    const ConstraintsDescription & constraints_,
    const ASTPtr & partition_by,
    const ASTPtr & order_by,
    ContextPtr context_,
    const String & comment_,
    std::optional<FormatSettings> format_settings_,
    LoadingStrictnessLevel mode_,
    std::shared_ptr<DataLake::ICatalog> catalog,
    bool if_not_exists,
    bool is_datalake_query,
    bool is_table_function,
    bool lazy_init)
    : IStorageCluster(
        cluster_name_, table_id_, getLogger(fmt::format("{}({})", configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , object_storage(object_storage_)
    , cluster_name_in_settings(false)
{
    configuration->initPartitionStrategy(partition_by, columns_in_table_or_function_definition, context_);

    const bool need_resolve_columns_or_format = columns_in_table_or_function_definition.empty() || (configuration->getFormat() == "auto");
    const bool do_lazy_init = lazy_init && !need_resolve_columns_or_format && catalog;

    auto log = getLogger("StorageObjectStorageCluster");

    bool is_delta_lake_cdf = context_->getSettingsRef()[Setting::delta_lake_snapshot_start_version] != -1
            || context_->getSettingsRef()[Setting::delta_lake_snapshot_end_version] != -1;

    if (!is_table_function && is_delta_lake_cdf)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta lake CDF is allowed only for deltaLake table function");
    }

    if (!is_table_function && !columns_in_table_or_function_definition.empty() && !is_datalake_query && mode_ == LoadingStrictnessLevel::CREATE)
    {
        LOG_DEBUG(log, "Creating new storage with specified columns");
        configuration->create(
            object_storage, context_, columns_in_table_or_function_definition, partition_by, order_by, if_not_exists, catalog, table_id_);
    }

    bool updated_configuration = false;
    try
    {
        if (!do_lazy_init)
        {
            if (is_table_function)
                configuration->lazyInitializeIfNeeded(object_storage, context_);
            else
                configuration->update(object_storage, context_);
            updated_configuration = true;
        }
    }
    catch (...)
    {
        // If we don't have format or schema yet, we can't ignore failed configuration update,
        // because relevant configuration is crucial for format and schema inference
        if (mode_ <= LoadingStrictnessLevel::CREATE || need_resolve_columns_or_format)
        {
            throw;
        }
        tryLogCurrentException(log);
    }

    ColumnsDescription columns{columns_in_table_or_function_definition};

    if (configuration->getRawPath().hasSchemaHashWildcard())
    {
        if (configuration->isDataLakeConfiguration())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The _schema_hash placeholder is not supported for DataLake engines");

        if (configuration->getPartitionStrategyType() == PartitionStrategyFactory::StrategyType::HIVE)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The _schema_hash placeholder is not supported with hive partition strategy");

        if (columns.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot use _schema_hash placeholder without explicitly specifying columns");

        configuration->setSchemaHash(StorageObjectStorageConfiguration::computeSchemaHash(columns));
    }

    std::string sample_path;
    if (need_resolve_columns_or_format)
        resolveSchemaAndFormat(columns, object_storage, configuration, {}, sample_path, context_);
    else
        validateSupportedColumns(columns, *configuration);
    configuration->check(context_);

    if (updated_configuration && sample_path.empty()
            && context_->getSettingsRef()[Setting::use_hive_partitioning]
            && !configuration->isDataLakeConfiguration()
            && !configuration->getPartitionStrategy())
    {
        sample_path = getPathSample(context_);
    }

    /// Not grabbing the file_columns because it is not necessary to do it here.
    std::tie(hive_partition_columns_to_read_from_file_path, std::ignore) = HivePartitioningUtils::setupHivePartitioningForObjectStorage(
        columns,
        configuration,
        sample_path,
        columns_in_table_or_function_definition.empty(),
        std::nullopt,
        context_);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);

    if (!do_lazy_init && is_table_function && configuration->isDataLakeConfiguration())
    {
        /// For datalake table functions, always pin the current snapshot version so that
        /// query execution uses the same snapshot as query analysis (logical-race fix).
        /// Additionally reload columns from the snapshot when the per-format setting is enabled.
        if (auto state = configuration->getTableStateSnapshot(context_))
        {
            metadata.setDataLakeTableState(*state);
            if (configuration->shouldReloadSchemaForConsistency(context_))
            {
                if (auto metadata_snapshot = configuration->buildStorageMetadataFromState(*state, context_))
                    metadata = *metadata_snapshot;
            }
        }
    }

    metadata.setConstraints(constraints_);

    if (configuration->partition_strategy)
    {
        metadata.partition_key = configuration->partition_strategy->getPartitionKeyDescription();
    }

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(
        metadata.columns,
        context_,
        /* format_settings */std::nullopt,
        configuration->getPartitionStrategyType(),
        sample_path));

    setInMemoryMetadata(metadata);

    const auto can_use_parallel_replicas = !cluster_name_.empty()
        && context_->getSettingsRef()[Setting::parallel_replicas_for_cluster_engines]
        && context_->canUseTaskBasedParallelReplicas()
        && !context_->isDistributed();

    bool can_use_distributed_iterator =
        context_->getClientInfo().collaborate_with_initiator &&
        can_use_parallel_replicas;

    pure_storage = std::make_shared<StorageObjectStorage>(
        configuration,
        object_storage,
        context_,
        getStorageID(),
        IStorageCluster::getInMemoryMetadata().getColumns(),
        IStorageCluster::getInMemoryMetadata().getConstraints(),
        comment_,
        format_settings_,
        mode_,
        catalog,
        if_not_exists,
        is_datalake_query,
        /* distributed_processing */can_use_distributed_iterator,
        partition_by,
        order_by,
        /* is_table_function */is_table_function,
        /* lazy_init */lazy_init,
        updated_configuration,
        sample_path);

    auto virtuals_ = getVirtualsPtr();
    if (virtuals_)
        pure_storage->setVirtuals(*virtuals_);
    pure_storage->setInMemoryMetadata(IStorageCluster::getInMemoryMetadata());
}

std::string StorageObjectStorageCluster::getName() const
{
    return configuration->getEngineName();
}

std::optional<UInt64> StorageObjectStorageCluster::totalRows(ContextPtr query_context) const
{
    if (pure_storage)
        return pure_storage->totalRows(query_context);
    configuration->lazyInitializeIfNeeded(
        object_storage,
        query_context);
    return configuration->totalRows(query_context);
}

std::optional<UInt64> StorageObjectStorageCluster::totalBytes(ContextPtr query_context) const
{
    if (pure_storage)
        return pure_storage->totalBytes(query_context);
    configuration->lazyInitializeIfNeeded(
        object_storage,
        query_context);
    return configuration->totalBytes(query_context);
}

void StorageObjectStorageCluster::updateQueryForDistributedEngineIfNeeded(ASTPtr & query, ContextPtr context)
{
    // Change table engine on table function for distributed request
    // CREATE TABLE t (...) ENGINE=IcebergS3(...)
    // SELECT * FROM t
    // change on
    // SELECT * FROM icebergS3(...)
    // to execute on cluster nodes

    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query || !select_query->tables())
        return;

    auto * tables = select_query->tables()->as<ASTTablesInSelectQuery>();

    if (tables->children.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected SELECT query from table with engine {}, got '{}'",
            configuration->getEngineName(), query->formatForLogging());

    auto * table_expression = tables->children[0]->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>();

    if (!table_expression)
        return;

    if (!table_expression->database_and_table_name)
        return;

    auto & table_identifier_typed = table_expression->database_and_table_name->as<ASTTableIdentifier &>();

    auto table_alias = table_identifier_typed.tryGetAlias();

    auto storage_engine_name = configuration->getEngineName();
    if (storage_engine_name == "Iceberg")
    {
        switch (configuration->getType())
        {
            case ObjectStorageType::S3:
                storage_engine_name = "IcebergS3";
                break;
            case ObjectStorageType::Azure:
                storage_engine_name = "IcebergAzure";
                break;
            case ObjectStorageType::HDFS:
                storage_engine_name = "IcebergHDFS";
                break;
            default:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Can't find table function for engine {}",
                    storage_engine_name
                );
        }
    }

    static std::unordered_map<std::string, std::string> engine_to_function = {
        {"S3", "s3"},
        {"Azure", "azureBlobStorage"},
        {"HDFS", "hdfs"},
        {"Iceberg", "iceberg"},
        {"IcebergS3", "icebergS3"},
        {"IcebergAzure", "icebergAzure"},
        {"IcebergHDFS", "icebergHDFS"},
        {"IcebergLocal", "icebergLocal"},
        {"DeltaLake", "deltaLake"},
        {"DeltaLakeS3", "deltaLakeS3"},
        {"DeltaLakeAzure", "deltaLakeAzure"},
        {"DeltaLakeLocal", "deltaLakeLocal"},
        {"Hudi", "hudi"},
        {"COSN", "cosn"},
        {"GCS", "gcs"},
        {"OSS", "oss"},
    };

    auto p = engine_to_function.find(storage_engine_name);
    if (p == engine_to_function.end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't find table function for engine {}",
            storage_engine_name
        );
    }

    std::string table_function_name = p->second;

    auto function_ast = make_intrusive<ASTFunction>();
    function_ast->name = table_function_name;

    auto cluster_name = getClusterName(context);

    if (cluster_name.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't be here without cluster name, no cluster name in query {}",
            query->formatForLogging());
    }

    function_ast->arguments = configuration->createArgsWithAccessData();
    function_ast->children.push_back(function_ast->arguments);
    function_ast->setAlias(table_alias);

    ASTPtr function_ast_ptr(function_ast);

    table_expression->database_and_table_name = nullptr;
    table_expression->table_function = function_ast_ptr;
    table_expression->children[0] = function_ast_ptr;

    auto settings = select_query->settings();
    if (settings)
    {
        auto & settings_ast = settings->as<ASTSetQuery &>();
        settings_ast.changes.insertSetting("object_storage_cluster", cluster_name);
    }
    else
    {
        auto settings_ast_ptr = make_intrusive<ASTSetQuery>();
        settings_ast_ptr->is_standalone = false;
        settings_ast_ptr->changes.setSetting("object_storage_cluster", cluster_name);
        select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings_ast_ptr));
    }

    cluster_name_in_settings = true;
}

void StorageObjectStorageCluster::updateQueryToSendIfNeeded(
    ASTPtr & query,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    updateQueryForDistributedEngineIfNeeded(query, context);

    auto * table_function = extractTableFunctionFromSelectQuery(query);
    if (!table_function)
        return;
    auto * expression_list = table_function->arguments->as<ASTExpressionList>();
    if (!expression_list)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected SELECT query from table function {}, got '{}'",
            configuration->getEngineName(), query->formatForErrorMessage());
    }

    ASTs & args = expression_list->children;
    const auto & structure = storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription();
    if (args.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected empty list of arguments for {}Cluster table function",
            configuration->getEngineName());
    }

    ASTPtr object_storage_type_arg;
    configuration->extractDynamicStorageType(args, context, &object_storage_type_arg, !cluster_name_in_settings);

    ASTPtr settings_temporary_storage = nullptr;
    for (auto it = args.begin(); it != args.end(); ++it)
    {
        ASTSetQuery * settings_ast = (*it)->as<ASTSetQuery>();
        if (settings_ast)
        {
            settings_temporary_storage = std::move(*it);
            args.erase(it);
            break;
        }
    }

    if (cluster_name_in_settings || !endsWith(table_function->name, "Cluster"))
    {
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->getFormat(), context, /*with_structure=*/true);

        /// Convert to old-stype *Cluster table function.
        /// This allows to use old clickhouse versions in cluster.
        static std::unordered_map<std::string, std::string> function_to_cluster_function = {
            {"s3", "s3Cluster"},
            {"azureBlobStorage", "azureBlobStorageCluster"},
            {"hdfs", "hdfsCluster"},
            {"iceberg", "icebergCluster"},
            {"icebergS3", "icebergS3Cluster"},
            {"icebergAzure", "icebergAzureCluster"},
            {"icebergHDFS", "icebergHDFSCluster"},
            {"icebergLocal", "icebergLocalCluster"},
            {"deltaLake", "deltaLakeCluster"},
            {"deltaLakeS3", "deltaLakeS3Cluster"},
            {"deltaLakeAzure", "deltaLakeAzureCluster"},
            {"hudi", "hudiCluster"},
            {"paimonS3", "paimonS3Cluster"},
            {"paimonAzure", "paimonAzureCluster"},
        };

        auto p = function_to_cluster_function.find(table_function->name);
        if (p == function_to_cluster_function.end())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can't find cluster variant for table function {}",
                table_function->name);
        }

        table_function->name = p->second;

        auto cluster_name = getClusterName(context);
        auto cluster_name_arg = make_intrusive<ASTLiteral>(cluster_name);
        args.insert(args.begin(), cluster_name_arg);

        auto * select_query = query->as<ASTSelectQuery>();
        if (!select_query)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected SELECT query from table function {}",
                configuration->getEngineName());

        auto settings = select_query->settings();
        if (settings)
        {
            auto & settings_ast = settings->as<ASTSetQuery &>();
            if (settings_ast.changes.removeSetting("object_storage_cluster") && settings_ast.changes.empty())
            {
                select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, {});
            }
            /// No throw if not found - `object_storage_cluster` can be global setting.
        }
    }
    else
    {
        ASTPtr cluster_name_arg = args.front();
        args.erase(args.begin());
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->getFormat(), context, /*with_structure=*/true);
        args.insert(args.begin(), cluster_name_arg);
    }
    if (settings_temporary_storage)
    {
        args.insert(args.end(), std::move(settings_temporary_storage));
    }
    if (object_storage_type_arg)
        args.insert(args.end(), object_storage_type_arg);
}

void StorageObjectStorageCluster::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    if (!configuration->isDataLakeConfiguration())
        return;

    /// Always force an update to pick up the latest snapshot version.
    /// Using if_not_updated_before=true would leave latest_snapshot_version
    /// stale from the first query and silently omit new files.
    configuration->update(
        object_storage,
        query_context);

    auto state = configuration->getTableStateSnapshot(query_context);
    if (!state)
        return;

    auto new_metadata = *getInMemoryMetadataPtr();
    new_metadata.setDataLakeTableState(*state);

    if (configuration->shouldReloadSchemaForConsistency(query_context))
    {
        if (auto metadata_snapshot = configuration->buildStorageMetadataFromState(*state, query_context))
            new_metadata = *metadata_snapshot;
    }

    setInMemoryMetadata(new_metadata);

    if (pure_storage)
        pure_storage->setInMemoryMetadata(IStorageCluster::getInMemoryMetadata());
}

class TaskDistributor : public TaskIterator
{
public:
    TaskDistributor(std::shared_ptr<IObjectIterator> iterator,
        std::vector<std::string> && ids_of_hosts,
        bool send_over_whole_archive,
        uint64_t lock_object_storage_task_distribution_ms,
        ContextPtr context_,
        bool iceberg_read_optimization_enabled)
        : task_distributor(
            iterator,
            std::move(ids_of_hosts),
            send_over_whole_archive,
            lock_object_storage_task_distribution_ms,
            iceberg_read_optimization_enabled)
        , context(context_) {}
    ~TaskDistributor() override = default;
    bool supportRerunTask() const override { return true; }
    void rescheduleTasksFromReplica(size_t number_of_current_replica) override
    {
        task_distributor.rescheduleTasksFromReplica(number_of_current_replica);
    }

    ClusterFunctionReadTaskResponsePtr operator()(size_t number_of_current_replica) const override
    {
        auto task = task_distributor.getNextTask(number_of_current_replica);
        if (task)
            return std::make_shared<ClusterFunctionReadTaskResponse>(std::move(task), context);
        return std::make_shared<ClusterFunctionReadTaskResponse>();
    }

private:
    mutable StorageObjectStorageStableTaskDistributor task_distributor;
    ContextPtr context;
};

RemoteQueryExecutor::Extension StorageObjectStorageCluster::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate,
    const ActionsDAG * filter,
    const ContextPtr & local_context,
    ClusterPtr cluster,
    StorageMetadataPtr storage_metadata_snapshot) const
{
    auto iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        configuration->getQuerySettings(local_context),
        object_storage,
        storage_metadata_snapshot,
        /* distributed_processing */ false,
        local_context,
        predicate,
        filter,
        virtual_columns,
        hive_partition_columns_to_read_from_file_path,
        nullptr,
        local_context->getFileProgressCallback(),
        /*ignore_archive_globs=*/false,
        /*skip_object_metadata=*/true);

    if (local_context->getSettingsRef()[Setting::cluster_table_function_split_granularity] == ObjectStorageGranularityLevel::BUCKET)
    {
        iterator = std::make_shared<ObjectIteratorSplitByBuckets>(
            std::move(iterator),
            configuration->getFormat(),
            object_storage,
            local_context
        );
    }

    std::vector<std::string> ids_of_hosts;
    for (const auto & shard : cluster->getShardsInfo())
    {
        if (shard.per_replica_pools.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster {} with empty shard {}", cluster->getName(), shard.shard_num);
        for (const auto & replica : shard.per_replica_pools)
        {
            if (!replica)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster {}, shard {} with empty node", cluster->getName(), shard.shard_num);
            ids_of_hosts.push_back(replica->getAddress());
        }
    }

    uint64_t lock_object_storage_task_distribution_ms = local_context->getSettingsRef()[Setting::lock_object_storage_task_distribution_ms];

    /// Check value to avoid negative result after conversion in microseconds.
    /// Poco::Timestamp::TimeDiff is signed int 64.
    static const uint64_t lock_object_storage_task_distribution_ms_max = 0x0020000000000000ULL;
    if (lock_object_storage_task_distribution_ms > lock_object_storage_task_distribution_ms_max)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Value lock_object_storage_task_distribution_ms is too big: {}, allowed maximum is {}",
            lock_object_storage_task_distribution_ms,
            lock_object_storage_task_distribution_ms_max
        );

    auto callback = std::make_shared<TaskDistributor>(iterator,
        std::move(ids_of_hosts),
        /* send_over_whole_archive */!local_context->getSettingsRef()[Setting::cluster_function_process_archive_on_multiple_nodes],
        lock_object_storage_task_distribution_ms,
        local_context,
        /* iceberg_read_optimization_enabled */local_context->getSettingsRef()[Setting::allow_experimental_iceberg_read_optimization]);

    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

void StorageObjectStorageCluster::readFallBackToPure(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    pure_storage->read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

bool StorageObjectStorageCluster::isClusterSupported() const
{
    return configuration->isClusterSupported();
}

SinkToStoragePtr StorageObjectStorageCluster::writeFallBackToPure(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    bool async_insert)
{
    return pure_storage->write(query, metadata_snapshot, context, async_insert);
}

String StorageObjectStorageCluster::getClusterName(ContextPtr context) const
{
    /// StorageObjectStorageCluster is always created for cluster or non-cluster variants.
    /// User can specify cluster name in table definition or in setting `object_storage_cluster`
    /// only for several queries. When it specified in both places, priority is given to the query setting.
    /// When it is empty, non-cluster realization is used.

    if (!isClusterSupported())
        return "";

    auto cluster_name_from_settings = context->getSettingsRef()[Setting::object_storage_cluster].value;
    if (cluster_name_from_settings.empty())
        cluster_name_from_settings = getOriginalClusterName();
    return cluster_name_from_settings;
}

QueryProcessingStage::Enum StorageObjectStorageCluster::getQueryProcessingStage(
    ContextPtr context, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info) const
{
    /// Full query if fall back to pure storage.
    if (getClusterName(context).empty())
        return QueryProcessingStage::Enum::FetchColumns;

    /// Distributed storage.
    return IStorageCluster::getQueryProcessingStage(context, to_stage, storage_snapshot, query_info);
}

std::optional<QueryPipeline> StorageObjectStorageCluster::distributedWrite(
    const ASTInsertQuery & query,
    ContextPtr context)
{
    if (getClusterName(context).empty())
        return pure_storage->distributedWrite(query, context);
    return IStorageCluster::distributedWrite(query, context);
}

void StorageObjectStorageCluster::drop()
{
    if (pure_storage)
    {
        pure_storage->drop();
        return;
    }
    IStorageCluster::drop();
}

void StorageObjectStorageCluster::dropInnerTableIfAny(bool sync, ContextPtr context)
{
    if (getClusterName(context).empty())
    {
        pure_storage->dropInnerTableIfAny(sync, context);
        return;
    }
    IStorageCluster::dropInnerTableIfAny(sync, context);
}

void StorageObjectStorageCluster::truncate(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    TableExclusiveLockHolder & lock_holder)
{
    /// Full query if fall back to pure storage.
    if (getClusterName(local_context).empty())
    {
        pure_storage->truncate(query, metadata_snapshot, local_context, lock_holder);
        return;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate is not supported by storage {}", getName());
}

void StorageObjectStorageCluster::checkTableCanBeRenamed(const StorageID & new_name) const
{
    if (pure_storage)
        pure_storage->checkTableCanBeRenamed(new_name);
    IStorageCluster::checkTableCanBeRenamed(new_name);
}

void StorageObjectStorageCluster::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    if (pure_storage)
        pure_storage->rename(new_path_to_table_data, new_table_id);
    IStorageCluster::rename(new_path_to_table_data, new_table_id);
}

void StorageObjectStorageCluster::renameInMemory(const StorageID & new_table_id)
{
    if (pure_storage)
        pure_storage->renameInMemory(new_table_id);
    IStorageCluster::renameInMemory(new_table_id);
}

void StorageObjectStorageCluster::alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder)
{
    if (getClusterName(context).empty())
    {
        pure_storage->alter(params, context, alter_lock_holder);
        setInMemoryMetadata(pure_storage->getInMemoryMetadata());
        return;
    }
    IStorageCluster::alter(params, context, alter_lock_holder);
    pure_storage->setInMemoryMetadata(IStorageCluster::getInMemoryMetadata());
}

void StorageObjectStorageCluster::addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const
{
    configuration->addStructureAndFormatToArgsIfNeeded(args, "", configuration->getFormat(), context, /*with_structure=*/false);
}

StorageMetadataPtr StorageObjectStorageCluster::getInMemoryMetadataPtr(bool bypass_metadata_cache) const
{
    if (pure_storage)
        return pure_storage->getInMemoryMetadataPtr(bypass_metadata_cache);
    return IStorageCluster::getInMemoryMetadataPtr(bypass_metadata_cache);
}

IDataLakeMetadata * StorageObjectStorageCluster::getExternalMetadata(ContextPtr query_context)
{
    if (getClusterName(query_context).empty())
        return pure_storage->getExternalMetadata(query_context);

    configuration->update(
        object_storage,
        query_context);

    return configuration->getExternalMetadata();
}

void StorageObjectStorageCluster::checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const
{
    if (getClusterName(context).empty())
    {
        pure_storage->checkAlterIsPossible(commands, context);
        return;
    }
    IStorageCluster::checkAlterIsPossible(commands, context);
}

void StorageObjectStorageCluster::checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const
{
    if (pure_storage)
    {
        pure_storage->checkMutationIsPossible(commands, settings);
        return;
    }
    IStorageCluster::checkMutationIsPossible(commands, settings);
}

Pipe StorageObjectStorageCluster::alterPartition(
    const StorageMetadataPtr & metadata_snapshot,
    const PartitionCommands & commands,
    ContextPtr context)
{
    if (getClusterName(context).empty())
        return pure_storage->alterPartition(metadata_snapshot, commands, context);
    return IStorageCluster::alterPartition(metadata_snapshot, commands, context);
}

void StorageObjectStorageCluster::checkAlterPartitionIsPossible(
    const PartitionCommands & commands,
    const StorageMetadataPtr & metadata_snapshot,
    const Settings & settings,
    ContextPtr context) const
{
    if (getClusterName(context).empty())
    {
        pure_storage->checkAlterPartitionIsPossible(commands, metadata_snapshot, settings, context);
        return;
    }
    IStorageCluster::checkAlterPartitionIsPossible(commands, metadata_snapshot, settings, context);
}

bool StorageObjectStorageCluster::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr context)
{
    if (getClusterName(context).empty())
        return pure_storage->optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, cleanup, context);
    return IStorageCluster::optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, cleanup, context);
}

QueryPipeline StorageObjectStorageCluster::updateLightweight(const MutationCommands & commands, ContextPtr context)
{
    if (getClusterName(context).empty())
        return pure_storage->updateLightweight(commands, context);
    return IStorageCluster::updateLightweight(commands, context);
}

void StorageObjectStorageCluster::mutate(const MutationCommands & commands, ContextPtr context)
{
    if (getClusterName(context).empty())
    {
        pure_storage->mutate(commands, context);
        return;
    }
    IStorageCluster::mutate(commands, context);
}

CancellationCode StorageObjectStorageCluster::killMutation(const String & mutation_id)
{
    if (pure_storage)
        return pure_storage->killMutation(mutation_id);
    return IStorageCluster::killMutation(mutation_id);
}

void StorageObjectStorageCluster::waitForMutation(const String & mutation_id, bool wait_for_another_mutation)
{
    if (pure_storage)
    {
        pure_storage->waitForMutation(mutation_id, wait_for_another_mutation);
        return;
    }
    IStorageCluster::waitForMutation(mutation_id, wait_for_another_mutation);
}

void StorageObjectStorageCluster::setMutationCSN(const String & mutation_id, UInt64 csn)
{
    if (pure_storage)
    {
        pure_storage->setMutationCSN(mutation_id, csn);
        return;
    }
    IStorageCluster::setMutationCSN(mutation_id, csn);
}

CancellationCode StorageObjectStorageCluster::killPartMoveToShard(const UUID & task_uuid)
{
    if (pure_storage)
        return pure_storage->killPartMoveToShard(task_uuid);
    return IStorageCluster::killPartMoveToShard(task_uuid);
}

void StorageObjectStorageCluster::startup()
{
    if (pure_storage)
    {
        pure_storage->startup();
        return;
    }
    IStorageCluster::startup();
}

void StorageObjectStorageCluster::shutdown(bool is_drop)
{
    if (pure_storage)
    {
        pure_storage->shutdown(is_drop);
        return;
    }
    IStorageCluster::shutdown(is_drop);
}

void StorageObjectStorageCluster::flushAndPrepareForShutdown()
{
    if (pure_storage)
    {
        pure_storage->flushAndPrepareForShutdown();
        return;
    }
    IStorageCluster::flushAndPrepareForShutdown();
}

ActionLock StorageObjectStorageCluster::getActionLock(StorageActionBlockType action_type)
{
    if (pure_storage)
        return pure_storage->getActionLock(action_type);
    return IStorageCluster::getActionLock(action_type);
}

void StorageObjectStorageCluster::onActionLockRemove(StorageActionBlockType action_type)
{
    if (pure_storage)
    {
        pure_storage->onActionLockRemove(action_type);
        return;
    }
    IStorageCluster::onActionLockRemove(action_type);
}

bool StorageObjectStorageCluster::supportsDelete() const
{
    if (pure_storage)
        return pure_storage->supportsDelete();
    return IStorageCluster::supportsDelete();
}

bool StorageObjectStorageCluster::supportsParallelInsert() const
{
    if (pure_storage)
        return pure_storage->supportsParallelInsert();
    return IStorageCluster::supportsParallelInsert();
}

bool StorageObjectStorageCluster::prefersLargeBlocks() const
{
    if (pure_storage)
        return pure_storage->prefersLargeBlocks();
    return IStorageCluster::prefersLargeBlocks();
}

bool StorageObjectStorageCluster::supportsPartitionBy() const
{
    if (pure_storage)
        return pure_storage->supportsPartitionBy();
    return IStorageCluster::supportsPartitionBy();
}

bool StorageObjectStorageCluster::supportsSubcolumns() const
{
    if (pure_storage)
        return pure_storage->supportsSubcolumns();
    return IStorageCluster::supportsSubcolumns();
}

bool StorageObjectStorageCluster::supportsTrivialCountOptimization(const StorageSnapshotPtr & snapshot, ContextPtr context) const
{
    if (pure_storage)
        return pure_storage->supportsTrivialCountOptimization(snapshot, context);
    return IStorageCluster::supportsTrivialCountOptimization(snapshot, context);
}

bool StorageObjectStorageCluster::supportsPrewhere() const
{
    if (pure_storage)
        return pure_storage->supportsPrewhere();
    return IStorageCluster::supportsPrewhere();
}

bool StorageObjectStorageCluster::canMoveConditionsToPrewhere() const
{
    if (pure_storage)
        return pure_storage->canMoveConditionsToPrewhere();
    return IStorageCluster::canMoveConditionsToPrewhere();
}

std::optional<NameSet> StorageObjectStorageCluster::supportedPrewhereColumns() const
{
    if (pure_storage)
        return pure_storage->supportedPrewhereColumns();
    return IStorageCluster::supportedPrewhereColumns();
}

IStorageCluster::ColumnSizeByName StorageObjectStorageCluster::getColumnSizes() const
{
    if (pure_storage)
        return pure_storage->getColumnSizes();
    return IStorageCluster::getColumnSizes();
}

bool StorageObjectStorageCluster::parallelizeOutputAfterReading(ContextPtr context) const
{
    if (pure_storage)
        return pure_storage->parallelizeOutputAfterReading(context);
    return IStorageCluster::parallelizeOutputAfterReading(context);
}

Pipe StorageObjectStorageCluster::executeCommand(const String & command_name, const ASTPtr & args, ContextPtr context)
{
    if (pure_storage)
        return pure_storage->executeCommand(command_name, args, context);
    return IStorageCluster::executeCommand(command_name, args, context);
}

}
