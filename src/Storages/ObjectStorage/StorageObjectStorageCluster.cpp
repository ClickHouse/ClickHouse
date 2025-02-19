#include "Storages/ObjectStorage/StorageObjectStorageCluster.h"

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>

#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool use_hive_partitioning;
    extern const SettingsString object_storage_cluster;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FUNCTION;
}


String StorageObjectStorageCluster::getPathSample(StorageInMemoryMetadata metadata, ContextPtr context)
{
    auto query_settings = configuration->getQuerySettings(context);
    /// We don't want to throw an exception if there are no files with specified path.
    query_settings.throw_on_zero_files_match = false;

    if (!configuration->isArchive() && !configuration->isPathWithGlobs())
        return configuration->getPath();

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
    ContextPtr context_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment_,
    std::optional<FormatSettings> format_settings_,
    LoadingStrictnessLevel mode_,
    ASTPtr partition_by_
)
    : IStorageCluster(
        cluster_name_, table_id_, getLogger(fmt::format("{}({})", configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , object_storage(object_storage_)
    , cluster_name_in_settings(false)
    , comment(comment_)
    , format_settings(format_settings_)
    , mode(mode_)
    , partition_by(partition_by_)
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
            configuration->getEngineName(), queryToString(query));

    auto * table_expression = tables->children[0]->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>();

    if (!table_expression)
        return;

    if (!table_expression->database_and_table_name)
        return;

    auto & table_identifier_typed = table_expression->database_and_table_name->as<ASTTableIdentifier &>();

    auto table_alias = table_identifier_typed.tryGetAlias();

    static std::unordered_map<std::string, std::string> engine_to_function = {
        {"S3", "s3"},
        {"Azure", "azureBlobStorage"},
        {"HDFS", "hdfs"},
        {"Iceberg", "icebergS3"},
        {"IcebergS3", "icebergS3"},
        {"IcebergAzure", "icebergAzure"},
        {"IcebergHDFS", "icebergHDFS"},
        {"DeltaLake", "deltaLake"},
        {"Hudi", "hudi"}
    };

    auto p = engine_to_function.find(configuration->getEngineName());
    if (p == engine_to_function.end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't find table function for engine {}",
            configuration->getEngineName()
        );
    }

    std::string table_function_name = p->second;

    auto function_ast = std::make_shared<ASTFunction>();
    function_ast->name = table_function_name;

    auto cluster_name = getClusterName(context);

    if (cluster_name.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't be here without cluster name, no cluster name in query {}",
            queryToString(query));
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
        auto settings_ast_ptr = std::make_shared<ASTSetQuery>();
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

    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);

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

    if (cluster_name_in_settings)
    {
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->format, context, /*with_structure=*/true);
    }
    else
    {
        ASTPtr cluster_name_arg = args.front();
        args.erase(args.begin());
        configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->format, context, /*with_structure=*/true);
        args.insert(args.begin(), cluster_name_arg);
    }
}

RemoteQueryExecutor::Extension StorageObjectStorageCluster::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate, const ContextPtr & local_context) const
{
    auto iterator = StorageObjectStorageSource::createFileIterator(
        configuration, configuration->getQuerySettings(local_context), object_storage, /* distributed_processing */false,
        local_context, predicate, getVirtualsList(), nullptr, local_context->getFileProgressCallback());

    auto callback = std::make_shared<std::function<String()>>([iterator]() mutable -> String
    {
        auto object_info = iterator->next(0);
        if (object_info)
            return object_info->getPath();
        return "";
    });
    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

std::shared_ptr<StorageObjectStorage> StorageObjectStorageCluster::getPureStorage(ContextPtr context)
{
    std::lock_guard lock(mutex);
    if (!pure_storage)
    {
        pure_storage = std::make_shared<StorageObjectStorage>(
            configuration,
            object_storage,
            context,
            getStorageID(),
            getInMemoryMetadata().getColumns(),
            getInMemoryMetadata().getConstraints(),
            comment,
            format_settings,
            mode,
            /* distributed_processing */false,
            partition_by);

        auto virtuals_ = getVirtualsPtr();
        if (virtuals_)
            pure_storage->setVirtuals(*virtuals_);

        pure_storage->setInMemoryMetadata(getInMemoryMetadata());
    }

    return pure_storage;
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
    getPureStorage(context)->read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr StorageObjectStorageCluster::writeFallBackToPure(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    bool async_insert)
{
    return getPureStorage(context)->write(query, metadata_snapshot, context, async_insert);
}

String StorageObjectStorageCluster::getClusterName(ContextPtr context) const
{
    auto cluster_name_ = context->getSettingsRef()[Setting::object_storage_cluster].value;
    if (cluster_name_.empty())
        cluster_name_ = getOriginalClusterName();
    return cluster_name_;
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

}
