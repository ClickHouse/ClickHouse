#include "config.h"
#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/StorageIcebergCluster.h>

#include <Interpreters/Context.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/ClusterUtils.h>

namespace DB
{

void StorageDataLakeCluster<IcebergMetadata>::ensureMetadataInitialized(ContextPtr context) const
{
    if (current_metadata)
        return;
    current_metadata = IcebergMetadata::create(object_storage, configuration, datalake_settings, context, table_options.format);
}

StorageDataLakeCluster<IcebergMetadata>::StorageDataLakeCluster(
    const String & cluster_name_,
    ObjectStorageConnectionConfigurationPtr configuration_,
    StorageObjectStorageTableOptions table_options_,
    ObjectStoragePtr object_storage_,
    const StorageID & table_id_,
    const ColumnsDescription & /*columns_in_table_or_function_definition*/,
    const ConstraintsDescription & constraints_,
    const ASTPtr & /*partition_by*/,
    ContextPtr context_,
    DataLakeStorageSettingsPtr datalake_settings_)
    : IStorageCluster(
          cluster_name_,
          table_id_,
          getLogger(fmt::format("{}({})", String(IcebergMetadata::name) + configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , table_options(std::move(table_options_))
    , object_storage(object_storage_)
    , datalake_settings(std::move(datalake_settings_))
{
    /// Ensure trailing slash on the raw path for data lake storages.
    auto path = configuration->getRawPath();
    if (!path.path.ends_with('/'))
        configuration->setRawPath(ObjectStorageConnectionConfiguration::Path(path.path + "/"));

    /// We allow exceptions to be thrown on update(),
    /// because Cluster engine can only be used as table function,
    /// so no lazy initialization is allowed.
    configuration->update(object_storage, context_);

    current_metadata = IcebergMetadata::create(object_storage, configuration, datalake_settings, context_, table_options.format);

    StorageInMemoryMetadata metadata = current_metadata->buildStorageMetadataFromState(context_);
    metadata.setConstraints(constraints_);
    setInMemoryMetadata(metadata);
    setVirtuals(
        VirtualColumnUtils::getVirtualsForFileLikeStorage(
            metadata.columns,
            context_,
            /* format_settings */ std::nullopt,
            table_options.partition_strategy_type,
            ""));
}

std::string StorageDataLakeCluster<IcebergMetadata>::getName() const
{
    return String(IcebergMetadata::name) + configuration->getEngineName();
}

std::optional<UInt64> StorageDataLakeCluster<IcebergMetadata>::totalRows(ContextPtr query_context) const
{
    ensureMetadataInitialized(query_context);
    return current_metadata->totalRows(query_context);
}

std::optional<UInt64> StorageDataLakeCluster<IcebergMetadata>::totalBytes(ContextPtr query_context) const
{
    ensureMetadataInitialized(query_context);
    return current_metadata->totalBytes(query_context);
}

void StorageDataLakeCluster<IcebergMetadata>::updateQueryToSendIfNeeded(
    ASTPtr & query,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    updateClusterQueryToSendIfNeeded(query, storage_snapshot, context, configuration, table_options, getName());
}

void StorageDataLakeCluster<IcebergMetadata>::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    ensureMetadataInitialized(query_context);
    setInMemoryMetadata(current_metadata->buildStorageMetadataFromState(query_context));
}

RemoteQueryExecutor::Extension StorageDataLakeCluster<IcebergMetadata>::getTaskIteratorExtension(
    const ActionsDAG::Node * /*predicate*/,
    const ActionsDAG * filter,
    const ContextPtr & local_context,
    ClusterPtr cluster,
    StorageMetadataPtr storage_metadata_snapshot) const
{
    ensureMetadataInitialized(local_context);

    LOG_DEBUG(getLogger("StorageIcebergCluster"), "getTaskIteratorExtension: filter={}", filter != nullptr ? "non-null" : "null");

    auto iterator = current_metadata->iterate(
        filter,
        local_context->getFileProgressCallback(),
        configuration->getQuerySettings(local_context).list_object_keys_size,
        storage_metadata_snapshot,
        local_context);

    return buildClusterTaskIteratorExtension(std::move(iterator), table_options, object_storage, local_context, std::move(cluster));
}

}

#endif
