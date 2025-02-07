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
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorageCluster(
        cluster_name_, table_id_, getLogger(fmt::format("{}({})", configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , object_storage(object_storage_)
    , cluster_name_in_settings(false)
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

void StorageObjectStorageCluster::updateQueryForDistributedEngineIfNeeded(ASTPtr & query)
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
    auto * table_expression = tables->children[0]->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>();
    if (!table_expression->database_and_table_name)
        return;

    auto & table_identifier_typed = table_expression->database_and_table_name->as<ASTTableIdentifier &>();

    auto table_alias = table_identifier_typed.tryGetAlias();

    std::unordered_map<std::string, std::string> engine_to_function = {
        {"S3", "s3"},
        {"Azure", "azureBlobStorage"},
        {"HDFS", "hdfs"},
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
    auto arguments = std::make_shared<ASTExpressionList>();

    auto cluster_name = getClusterName();

    if (cluster_name.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't be here without cluster name, no cluster name in query {}",
            queryToString(query));
    }

    configuration->setFunctionArgs(arguments->children);

    function_ast->arguments = arguments;
    function_ast->children.push_back(arguments);
    function_ast->setAlias(table_alias);

    ASTPtr function_ast_ptr(function_ast);

    table_expression->database_and_table_name = nullptr;
    table_expression->table_function = function_ast_ptr;
    table_expression->children[0].swap(function_ast_ptr);

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
    updateQueryForDistributedEngineIfNeeded(query);

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

}
