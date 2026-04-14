#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Storages/IPartitionStrategy.h>

#include <Storages/VirtualColumnUtils.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/ClusterUtils.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool use_hive_partitioning;
}

String StorageObjectStorageCluster::getPathSample(ContextPtr context)
{
    auto query_settings = configuration->getQuerySettings(context);
    /// We don't want to throw an exception if there are no files with specified path.
    query_settings.throw_on_zero_files_match = false;
    auto file_iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        table_options.getPathForRead(),
        {table_options.getPathForRead()},
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
        {} // file_progress_callback
    );

    if (auto file = file_iterator->next(0))
        return file->getPath();
    return "";
}

StorageObjectStorageCluster::StorageObjectStorageCluster(
    const String & cluster_name_,
    ObjectStorageConnectionConfigurationPtr configuration_,
    StorageObjectStorageTableOptions table_options_,
    ObjectStoragePtr object_storage_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_in_table_or_function_definition,
    const ConstraintsDescription & constraints_,
    const ASTPtr & partition_by,
    ContextPtr context_)
    : IStorageCluster(cluster_name_, table_id_, getLogger(fmt::format("{}({})", configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , table_options(std::move(table_options_))
    , object_storage(object_storage_)
{
    table_options.initPartitionStrategy(partition_by, columns_in_table_or_function_definition, context_, configuration->getRawPath());
    /// We allow exceptions to be thrown on update(),
    /// because Cluster engine can only be used as table function,
    /// so no lazy initialization is allowed.
    configuration->update(object_storage, context_);

    ColumnsDescription columns{columns_in_table_or_function_definition};
    std::string sample_path;
    resolveSchemaAndFormat(columns, table_options.format, table_options.compression_method, object_storage, configuration, {}, sample_path, context_);
    FormatFactory::instance().checkFormatName(table_options.format);

    if (sample_path.empty()
        && context_->getSettingsRef()[Setting::use_hive_partitioning]
        && !table_options.partition_strategy)
        sample_path = getPathSample(context_);

    /// Not grabbing the file_columns because it is not necessary to do it here.
    std::tie(hive_partition_columns_to_read_from_file_path, std::ignore) = HivePartitioningUtils::setupHivePartitioningForObjectStorage(
        columns,
        configuration,
        table_options,
        sample_path,
        columns_in_table_or_function_definition.empty(),
        std::nullopt,
        context_);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    metadata.setConstraints(constraints_);

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(
        metadata.columns,
        context_,
        /* format_settings */std::nullopt,
        table_options.partition_strategy_type,
        sample_path));

    setInMemoryMetadata(metadata);
}

std::string StorageObjectStorageCluster::getName() const
{
    return configuration->getEngineName();
}

std::optional<UInt64> StorageObjectStorageCluster::totalRows(ContextPtr) const
{
    return std::nullopt;
}

std::optional<UInt64> StorageObjectStorageCluster::totalBytes(ContextPtr) const
{
    return std::nullopt;
}

void StorageObjectStorageCluster::updateQueryToSendIfNeeded(
    ASTPtr & query,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    updateClusterQueryToSendIfNeeded(query, storage_snapshot, context, configuration, table_options, getName());
}

RemoteQueryExecutor::Extension StorageObjectStorageCluster::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate,
    const ActionsDAG * filter,
    const ContextPtr & local_context,
    ClusterPtr cluster,
    StorageMetadataPtr storage_metadata_snapshot) const
{
    auto iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        table_options.getPathForRead(),
        {table_options.getPathForRead()},
        configuration->getQuerySettings(local_context),
        object_storage,
        storage_metadata_snapshot,
        /* distributed_processing */ false,
        local_context,
        predicate,
        filter,
        getVirtualsPtr()->getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader).getNamesAndTypesList(),
        hive_partition_columns_to_read_from_file_path,
        nullptr,
        local_context->getFileProgressCallback(),
        /*ignore_archive_globs=*/false,
        /*skip_object_metadata=*/true);

    return buildClusterTaskIteratorExtension(std::move(iterator), table_options, object_storage, local_context, std::move(cluster));
}

}

