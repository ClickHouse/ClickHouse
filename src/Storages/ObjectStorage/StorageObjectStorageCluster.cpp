#include "Storages/ObjectStorage/StorageObjectStorageCluster.h"

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/extractTableFunctionFromSelectQuery.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool use_hive_partitioning;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String StorageObjectStorageCluster::getPathSample(StorageInMemoryMetadata metadata, ContextPtr context)
{
    auto query_settings = configuration->getQuerySettings(context);
    /// We don't want to throw an exception if there are no files with specified path.
    query_settings.throw_on_zero_files_match = false;
    auto file_iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        query_settings,
        object_storage,
        false, // distributed_processing
        context,
        {}, // predicate
        metadata.getColumns().getAll(), // virtual_columns
        nullptr, // read_keys
        {} // file_progress_callback
    );

    if (auto file = file_iterator->next(0))
        return file->getPath();
    return "";
}

StorageObjectStorageCluster::StorageObjectStorageCluster(
    const String & cluster_name_,
    ConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorageCluster(
        cluster_name_, table_id_, getLogger(fmt::format("{}({})", configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , object_storage(object_storage_)
{
    ColumnsDescription columns{columns_};
    std::string sample_path;
    resolveSchemaAndFormat(columns, configuration->format, object_storage, configuration, {}, sample_path, context_);
    configuration->check(context_);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    metadata.setConstraints(constraints_);

    if (sample_path.empty() && context_->getSettingsRef()[Setting::use_hive_partitioning])
        sample_path = getPathSample(metadata, context_);

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(metadata.columns, context_, sample_path));
    setInMemoryMetadata(metadata);
}

std::string StorageObjectStorageCluster::getName() const
{
    return configuration->getEngineName();
}

void StorageObjectStorageCluster::updateQueryToSendIfNeeded(
    ASTPtr & query,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    auto * table_function = extractTableFunctionFromSelectQuery(query);
    if (!table_function)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected SELECT query from table function {}, got '{}'",
            configuration->getEngineName(), queryToString(query));
    }

    auto * expression_list = table_function->arguments->as<ASTExpressionList>();
    if (!expression_list)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected SELECT query from table function {}, got '{}'",
            configuration->getEngineName(), queryToString(query));
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

    if (table_function->name == configuration->getTypeName())
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->format, context);
    else if (table_function->name == fmt::format("{}Cluster", configuration->getTypeName()))
    {
        ASTPtr cluster_name_arg = args.front();
        args.erase(args.begin());
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->format, context);
        args.insert(args.begin(), cluster_name_arg);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected table function name: {}", table_function->name);

}

RemoteQueryExecutor::Extension StorageObjectStorageCluster::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate, const ContextPtr & local_context) const
{
    auto iterator = StorageObjectStorageSource::createFileIterator(
        configuration, configuration->getQuerySettings(local_context), object_storage, /* distributed_processing */false,
        local_context, predicate, virtual_columns, nullptr, local_context->getFileProgressCallback());

    auto callback = std::make_shared<std::function<String()>>([iterator]() mutable -> String
    {
        auto object_info = iterator->next(0);
        if (object_info)
            return object_info->getPath();
        return "";
    });
    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

}
