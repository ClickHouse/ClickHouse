#include <Storages/ObjectStorage/DataLakes/Iceberg/StorageIceberg.h>

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromIcebergStep.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/StorageID.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Databases/DataLake/Common.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/AlterCommands.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_count_from_files;
    extern const SettingsUInt64 max_streams_for_files_processing_in_cluster_functions;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

StorageDataLake<IcebergMetadata>::StorageDataLake(
    ObjectStorageConnectionConfigurationPtr configuration_,
    StorageObjectStorageTableOptions table_options_,
    ObjectStoragePtr object_storage_,
    ContextPtr context,
    const StorageID & table_id_,
    const ColumnsDescription & columns_in_table_or_function_definition,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    LoadingStrictnessLevel mode,
    DataLakeStorageSettingsPtr datalake_settings_,
    std::shared_ptr<DataLake::ICatalog> catalog_,
    bool distributed_processing_,
    ASTPtr /*partition_by_*/,
    ASTPtr /*order_by_*/,
    bool is_table_function_,
    bool request_skipping_initialization)
    : IStorage(table_id_)
    , configuration(configuration_)
    , table_options(table_options_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , distributed_processing(distributed_processing_)
    , is_table_function(is_table_function_)
    , log(getLogger(
          fmt::format("Storage{}({})", String(IcebergMetadata::name) + configuration->getEngineName(), table_id_.getFullTableName())))
    , datalake_settings(std::move(datalake_settings_))
    , catalog(catalog_)
    , storage_id(table_id_)
{
    // Legacy code based on an incorrect assumption that all files in iceberg have the same format
    FormatFactory::instance().checkFormatName(table_options.format);
    bool format_supports_prewhere = FormatFactory::instance().checkIfFormatSupportsPrewhere(table_options.format, context, format_settings);
    supports_prewhere = format_supports_prewhere;
    supports_tuple_elements = format_supports_prewhere;


    /// Ensure trailing slash on the raw path for data lake storages.
    auto raw_path = configuration->getRawPath();
    if (!raw_path.path.ends_with('/'))
        configuration->setRawPath(ObjectStorageConnectionConfiguration::Path(raw_path.path + "/"));

    const bool need_resolve_columns = columns_in_table_or_function_definition.empty();
    const bool skip_initialization = request_skipping_initialization && !need_resolve_columns && !is_table_function;

    /// Initialize Iceberg metadata (reads manifest lists, resolves schema, etc.).
    /// Skip when lazy init is requested and columns are already known.
    if (!skip_initialization)
    {
        try
        {
            configuration->update(object_storage, context);
            ensureMetadataInitialized(context);
        }
        catch (...)
        {
            /// During CREATE or when we must infer the schema, initialization failure is fatal.
            if ((mode <= LoadingStrictnessLevel::CREATE) || need_resolve_columns)
                throw;

            /// Otherwise (e.g. ATTACH with known columns), log and continue —
            /// metadata will be re-initialized on the first query.
            tryLogCurrentException(log, /*start of message=*/"", LogsLevel::warning);
            return;
        }
    }

    ensureMetadataInitialized(context);
    StorageInMemoryMetadata storage_metadata = current_metadata->buildStorageMetadataFromState(context);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    validateSupportedColumns(storage_metadata.columns, configuration->getTypeName());
    setVirtuals(
        VirtualColumnUtils::getVirtualsForFileLikeStorage(
            storage_metadata.columns, context, format_settings, table_options.partition_strategy_type, ""));

    setInMemoryMetadata(storage_metadata);
}

void StorageDataLake<IcebergMetadata>::ensureMetadataInitialized(ContextPtr context) const
{
    if (current_metadata)
        return;
    configuration->update(object_storage, context);
    current_metadata = IcebergMetadata::create(object_storage, configuration, datalake_settings, context);
}

String StorageDataLake<IcebergMetadata>::getName() const
{
    return String(IcebergMetadata::name) + configuration->getEngineName();
}

bool StorageDataLake<IcebergMetadata>::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(table_options.format);
}

bool StorageDataLake<IcebergMetadata>::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(table_options.format, context);
}

bool StorageDataLake<IcebergMetadata>::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(table_options.format, context, format_settings);
}

bool StorageDataLake<IcebergMetadata>::supportsPrewhere() const
{
    return supports_prewhere;
}

bool StorageDataLake<IcebergMetadata>::canMoveConditionsToPrewhere() const
{
    return supports_prewhere;
}

std::optional<NameSet> StorageDataLake<IcebergMetadata>::supportedPrewhereColumns() const
{
    return getInMemoryMetadataPtr()->getColumnsWithoutDefaultExpressions(/*exclude=*/ {});
}

IStorage::ColumnSizeByName StorageDataLake<IcebergMetadata>::getColumnSizes() const
{
    return getInMemoryMetadataPtr()->getFakeColumnSizes();
}

IcebergMetadata * StorageDataLake<IcebergMetadata>::getIcebergMetadata(ContextPtr context)
{
    ensureMetadataInitialized(context);
    return current_metadata.get();
}

void StorageDataLake<IcebergMetadata>::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    ensureMetadataInitialized(query_context);
    setInMemoryMetadata(current_metadata->buildStorageMetadataFromState(query_context));
}


std::optional<UInt64> StorageDataLake<IcebergMetadata>::totalRows(ContextPtr query_context) const
{
    if (distributed_processing)
        return std::nullopt;

    ensureMetadataInitialized(query_context);
    return current_metadata->totalRows(query_context);
}

std::optional<UInt64> StorageDataLake<IcebergMetadata>::totalBytes(ContextPtr query_context) const
{
    if (distributed_processing)
        return std::nullopt;

    ensureMetadataInitialized(query_context);
    return current_metadata->totalBytes(query_context);
}

void StorageDataLake<IcebergMetadata>::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    auto * read_metadata = getIcebergMetadata(local_context);

    if (distributed_processing && local_context->getSettingsRef()[Setting::max_streams_for_files_processing_in_cluster_functions])
        num_streams = local_context->getSettingsRef()[Setting::max_streams_for_files_processing_in_cluster_functions];

    if (table_options.partition_strategy && table_options.partition_strategy_type != PartitionStrategyFactory::StrategyType::HIVE)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Reading from a partitioned {} storage is not implemented yet",
                        getName());
    }

    auto read_from_format_info = read_metadata->prepareReadingFromFormat(
        column_names,
        storage_snapshot,
        local_context,
        supportsSubsetOfColumns(local_context),
        supports_tuple_elements);


    if (query_info.prewhere_info || query_info.row_level_filter)
        read_from_format_info = updateFormatPrewhereInfo(read_from_format_info, query_info.row_level_filter, query_info.prewhere_info);

    const auto & settings = local_context->getSettingsRef();
    const bool need_only_count = (query_info.optimize_trivial_count
                                  || (read_from_format_info.requested_columns.empty()
                                      && !read_from_format_info.prewhere_info
                                      && !read_from_format_info.row_level_filter))
        && settings[Setting::optimize_count_from_files]
        && !VirtualColumnUtils::hasRowDependentVirtualColumns(read_from_format_info.requested_virtual_columns);

    auto modified_format_settings{format_settings};
    if (!modified_format_settings.has_value())
        modified_format_settings.emplace(getFormatSettings(local_context));

    read_metadata->modifyFormatSettings(modified_format_settings.value(), *local_context);

    auto read_step = std::make_unique<ReadFromIcebergStep>(
        object_storage,
        configuration,
        table_options,
        column_names,
        getVirtualsList(),
        query_info,
        storage_snapshot,
        modified_format_settings,
        read_from_format_info,
        need_only_count,
        local_context,
        max_block_size,
        num_streams,
        read_metadata,
        distributed_processing);

    query_plan.addStep(std::move(read_step));
}

SinkToStoragePtr StorageDataLake<IcebergMetadata>::write(
    const ASTPtr &,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    ensureMetadataInitialized(local_context);
    const auto sample_block = std::make_shared<const Block>(metadata_snapshot->getSampleBlock());
    return current_metadata->write(sample_block, storage_id, object_storage, configuration, format_settings, local_context, catalog);
}

bool StorageDataLake<IcebergMetadata>::optimize(
    const ASTPtr & /*query*/,
    [[maybe_unused]] const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & /*partition*/,
    bool /*final*/,
    bool /*deduplicate*/,
    const Names & /* deduplicate_by_columns */,
    bool /*cleanup*/,
    [[maybe_unused]] ContextPtr context)
{
    ensureMetadataInitialized(context);
    return current_metadata->optimize(metadata_snapshot, context, format_settings);
}

void StorageDataLake<IcebergMetadata>::truncate(
    const ASTPtr & /* query */,
    const StorageMetadataPtr & /* metadata_snapshot */,
    ContextPtr /* context */,
    TableExclusiveLockHolder & /* table_holder */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate is not supported for data lake engine");
}

void StorageDataLake<IcebergMetadata>::drop()
{
    if (catalog)
    {
        const auto [namespace_name, table_name] = DataLake::parseTableName(storage_id.getTableName());
        catalog->dropTable(namespace_name, table_name);
    }
    /// We cannot use query context here, because drop is executed in the background.
    if (current_metadata)
        current_metadata->drop(Context::getGlobalContextInstance());
}

void StorageDataLake<IcebergMetadata>::addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const
{
    configuration->addStructureAndFormatToArgsIfNeeded(args, "", table_options.format, context, /*with_structure=*/false);
}

void StorageDataLake<IcebergMetadata>::mutate([[maybe_unused]] const MutationCommands & commands, [[maybe_unused]] ContextPtr context_)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    current_metadata->mutate(commands, configuration, context_, storage, metadata_snapshot, catalog, format_settings);
}

void StorageDataLake<IcebergMetadata>::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    current_metadata->checkMutationIsPossible(commands);
}

Pipe StorageDataLake<IcebergMetadata>::executeCommand(const String & command_name, const ASTPtr & args, ContextPtr context)
{
    auto * metadata = getIcebergMetadata(context);
    return metadata->executeCommand(command_name, args, object_storage, configuration, catalog, context, storage_id);
}

void StorageDataLake<IcebergMetadata>::alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & /*alter_lock_holder*/)
{
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, context);

    current_metadata->alter(params, context);

    DatabaseCatalog::instance()
        .getDatabase(storage_id.database_name)
        ->alterTable(context, storage_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);
}

void StorageDataLake<IcebergMetadata>::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*context*/) const
{
    current_metadata->checkAlterIsPossible(commands);
}

}
