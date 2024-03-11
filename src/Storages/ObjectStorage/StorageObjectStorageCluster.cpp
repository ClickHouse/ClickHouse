#include "Storages/ObjectStorage/StorageObjectStorageCluster.h"

#include "config.h"
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/IStorage.h>
#include <Storages/StorageURL.h>
#include <Storages/StorageDictionary.h>
#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Common/Exception.h>
#include <Parsers/queryToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Definition, typename StorageSettings, typename Configuration>
StorageObjectStorageCluster<Definition, StorageSettings, Configuration>::StorageObjectStorageCluster(
    const String & cluster_name_,
    const Storage::ConfigurationPtr & configuration_,
    ObjectStoragePtr object_storage_,
    const String & engine_name_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorageCluster(cluster_name_,
                      table_id_,
                      getLogger(fmt::format("{}({})", engine_name_, table_id_.table_name)))
    , engine_name(engine_name_)
    , configuration{configuration_}
    , object_storage(object_storage_)
{
    configuration->check(context_);
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        ColumnsDescription columns = Storage::getTableStructureFromData(object_storage, configuration, /*format_settings=*/std::nullopt, context_);
        storage_metadata.setColumns(columns);
    }
    else
    {
        if (configuration->format == "auto")
            StorageS3::setFormatFromData(object_storage, configuration, /*format_settings=*/std::nullopt, context_);

        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.getColumns()));
}

template <typename Definition, typename StorageSettings, typename Configuration>
void StorageObjectStorageCluster<Definition, StorageSettings, Configuration>::updateQueryToSendIfNeeded(
    ASTPtr & query,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Expected SELECT query from table function {}, got '{}'",
                        engine_name, queryToString(query));
    }

    TableFunction::updateStructureAndFormatArgumentsIfNeeded(
        expression_list->children,
        storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription(),
        configuration->format,
        context);
}

template <typename Definition, typename StorageSettings, typename Configuration>
RemoteQueryExecutor::Extension
StorageObjectStorageCluster<Definition, StorageSettings, Configuration>::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate, const ContextPtr & local_context) const
{
    const auto settings = StorageSettings::create(local_context->getSettingsRef());
    auto iterator = std::make_shared<StorageObjectStorageSource::GlobIterator>(
        object_storage, configuration, predicate, virtual_columns, local_context,
        nullptr, settings.list_object_keys_size, local_context->getFileProgressCallback());

    auto callback = std::make_shared<std::function<String()>>([iterator]() mutable -> String
    {
        auto object_info = iterator->next(0);
        if (object_info)
            return object_info->relative_path;
        else
            return "";
    });
    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}


#if USE_AWS_S3
template class StorageObjectStorageCluster<S3ClusterDefinition, S3StorageSettings, StorageS3Configuration>;
#endif

#if USE_AZURE_BLOB_STORAGE
template class StorageObjectStorageCluster<AzureClusterDefinition, AzureStorageSettings, StorageAzureBlobConfiguration>;
#endif

#if USE_HDFS
template class StorageObjectStorageCluster<HDFSClusterDefinition, HDFSStorageSettings, StorageHDFSConfiguration>;
#endif

}
