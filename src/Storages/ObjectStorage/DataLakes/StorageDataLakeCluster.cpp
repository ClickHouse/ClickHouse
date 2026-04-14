#include <Storages/ObjectStorage/DataLakes/StorageDataLakeCluster.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Storages/IPartitionStrategy.h>

#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/ClusterUtils.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/StorageIceberg.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StorageIcebergCluster.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>

namespace DB
{

template <typename DataLakeMetadata>
void StorageDataLakeCluster<DataLakeMetadata>::ensureMetadataInitialized(ContextPtr context) const
{
    if (current_metadata)
        return;
    current_metadata = DataLakeMetadata::create(object_storage, configuration, datalake_settings, context);
}

template <typename DataLakeMetadata>
void StorageDataLakeCluster<DataLakeMetadata>::updateMetadata(ContextPtr context) const
{
    if (current_metadata && current_metadata->supportsUpdate())
    {
        current_metadata->update(context);
        return;
    }
    current_metadata = DataLakeMetadata::create(object_storage, configuration, datalake_settings, context);
}

template <typename DataLakeMetadata>
StorageDataLakeCluster<DataLakeMetadata>::StorageDataLakeCluster(
    const String & cluster_name_,
    ObjectStorageConnectionConfigurationPtr configuration_,
    StorageObjectStorageTableOptions table_options_,
    ObjectStoragePtr object_storage_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_in_table_or_function_definition,
    const ConstraintsDescription & constraints_,
    const ASTPtr & partition_by,
    ContextPtr context_,
    DataLakeStorageSettingsPtr datalake_settings_)
    : IStorageCluster(
          cluster_name_,
          table_id_,
          getLogger(fmt::format("{}({})", String(DataLakeMetadata::name) + configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , table_options(std::move(table_options_))
    , object_storage(object_storage_)
    , datalake_settings(std::move(datalake_settings_))
{
    /// Ensure trailing slash on the raw path for data lake storages.
    auto path = configuration->getRawPath();
    if (!path.path.ends_with('/'))
        configuration->setRawPath(ObjectStorageConnectionConfiguration::Path(path.path + "/"));

    table_options.initPartitionStrategy(partition_by, columns_in_table_or_function_definition, context_, configuration->getRawPath());
    /// We allow exceptions to be thrown on update(),
    /// because Cluster engine can only be used as table function,
    /// so no lazy initialization is allowed.
    configuration->update(object_storage, context_);

    current_metadata = DataLakeMetadata::create(object_storage, configuration, datalake_settings, context_);

    ColumnsDescription columns{columns_in_table_or_function_definition};
    std::string sample_path;

    /// Try to resolve schema from data lake metadata first.
    if (columns.empty() && current_metadata)
    {
        auto schema = current_metadata->getTableSchema(context_);
        if (!schema.empty())
            columns = ColumnsDescription(std::move(schema));
    }

    if (table_options.format == "auto")
        table_options.format = "Parquet";

    if (columns.empty())
        resolveSchemaAndFormat(columns, table_options.format, table_options.compression_method, object_storage, configuration, {}, sample_path, context_);
    FormatFactory::instance().checkFormatName(table_options.format);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    if (auto state = current_metadata->getTableStateSnapshot(context_))
    {
        metadata.setDataLakeTableState(*state);
        if (current_metadata->shouldReloadSchemaForConsistency(context_))
        {
            if (auto metadata_snapshot = current_metadata->buildStorageMetadataFromState(*state, context_))
                metadata = *metadata_snapshot;
        }
    }

    metadata.setConstraints(constraints_);

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(
        metadata.columns,
        context_,
        /* format_settings */std::nullopt,
        table_options.partition_strategy_type,
        sample_path));

    setInMemoryMetadata(metadata);
}

template <typename DataLakeMetadata>
std::string StorageDataLakeCluster<DataLakeMetadata>::getName() const
{
    return String(DataLakeMetadata::name) + configuration->getEngineName();
}

template <typename DataLakeMetadata>
std::optional<UInt64> StorageDataLakeCluster<DataLakeMetadata>::totalRows(ContextPtr query_context) const
{
    ensureMetadataInitialized(query_context);
    return current_metadata->totalRows(query_context);
}

template <typename DataLakeMetadata>
std::optional<UInt64> StorageDataLakeCluster<DataLakeMetadata>::totalBytes(ContextPtr query_context) const
{
    ensureMetadataInitialized(query_context);
    return current_metadata->totalBytes(query_context);
}

template <typename DataLakeMetadata>
void StorageDataLakeCluster<DataLakeMetadata>::updateQueryToSendIfNeeded(
    ASTPtr & query,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    updateClusterQueryToSendIfNeeded(query, storage_snapshot, context, configuration, table_options, getName());
}

template <typename DataLakeMetadata>
void StorageDataLakeCluster<DataLakeMetadata>::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    /// Always force an update to pick up the latest snapshot version.
    updateMetadata(query_context);

    auto state = current_metadata->getTableStateSnapshot(query_context);
    if (!state)
        return;

    auto new_metadata = *getInMemoryMetadataPtr(query_context, /*bypass_metadata_cache=*/false);
    new_metadata.setDataLakeTableState(*state);

    if (current_metadata->shouldReloadSchemaForConsistency(query_context))
    {
        if (auto metadata_snapshot = current_metadata->buildStorageMetadataFromState(*state, query_context))
            new_metadata = *metadata_snapshot;
    }

    setInMemoryMetadata(new_metadata);
}

template <typename DataLakeMetadata>
RemoteQueryExecutor::Extension StorageDataLakeCluster<DataLakeMetadata>::getTaskIteratorExtension(
    const ActionsDAG::Node * /*predicate*/,
    const ActionsDAG * filter,
    const ContextPtr & local_context,
    ClusterPtr cluster,
    StorageMetadataPtr storage_metadata_snapshot) const
{
    ensureMetadataInitialized(local_context);

    auto iterator = current_metadata->iterate(
        filter,
        local_context->getFileProgressCallback(),
        configuration->getQuerySettings(local_context).list_object_keys_size,
        storage_metadata_snapshot,
        local_context);

    return buildClusterTaskIteratorExtension(std::move(iterator), table_options, object_storage, local_context, std::move(cluster));
}

#if USE_AVRO
template class StorageDataLakeCluster<PaimonMetadata>;
#endif

#if USE_PARQUET
template class StorageDataLakeCluster<DeltaLakeMetadata>;
#endif

#if USE_AWS_S3
template class StorageDataLakeCluster<HudiMetadata>;
#endif

}
