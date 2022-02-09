#include <Storages/Hive/StorageHiveCluster.h>
#if USE_HIVE
#include <algorithm>
#include <base/logger_useful.h>
#include <Client/IConnections.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/Hive/HiveSettings.h>
#include <Storages/Hive/IHiveTaskPolicy.h>
#include <Storages/Hive/HiveTaskPolicyFactory.h>
#include <Storages/Hive/StorageHive.h>
#include <Storages/StorageFactory.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StorageHiveCluster::StorageHiveCluster(
    const String & cluster_name_,
    const String & hive_metastore_url_,
    const String & hive_database_,
    const String & hive_table_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment_,
    const ASTPtr & partition_by_ast_,
    std::unique_ptr<HiveSettings> storage_settings_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_)
    , cluster_name(cluster_name_)
    , hive_metastore_url(hive_metastore_url_)
    , hive_database(hive_database_)
    , hive_table(hive_table_)
    , partition_by_ast(partition_by_ast_)
    , storage_settings(std::move(storage_settings_))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageHiveCluster::read(
    const Names & column_names_,
    const StorageMetadataPtr & metadata_snapshot_,
    SelectQueryInfo & query_info_,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage_,
    size_t max_block_size_,
    unsigned num_streams_)
{
    auto query_kind = context_->getClientInfo().query_kind;
    /*
    LOG_TRACE(
        logger,
        "query kinkd: {}, processed stage: {}, query: {}"
        "task iterate policy:{}",
        query_kind,
        processed_stage_,
        queryToString(query_info_.query),
        context_->getSettings().getString("hive_cluster_task_iterate_policy"));
    */
    auto policy_name = context_->getSettings().getString("hive_cluster_task_iterate_policy");
    // first stage. create remote executors pipeline
    if (query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        auto iterate_callback_builder = HiveTaskPolicyFactory::instance().getIterateCallback(policy_name);
        if (!iterate_callback_builder)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown hive task policy : {}", policy_name);

        IHiveTaskIterateCallback::Arguments args
            = {.cluster_name = cluster_name,
               .storage_settings = storage_settings,
               .columns = getInMemoryMetadata().getColumns(),
               .context = context_,
               .query_info = &query_info_,
               .hive_metastore_url = hive_metastore_url,
               .hive_database = hive_database,
               .hive_table = hive_table,
               .partition_by_ast = partition_by_ast,
               .num_streams = num_streams_

            };
        iterate_callback_builder->setupArgs(args);

        auto cluster = context_->getCluster(cluster_name)->getClusterWithReplicasAsShards(context_->getSettings());

        Block header = InterpreterSelectQuery(query_info_.query, context_, SelectQueryOptions(processed_stage_).analyze()).getSampleBlock();
        const Scalars & scalars = context_->hasQueryContext() ? context_->getQueryContext()->getScalars() : Scalars{};
        Pipes pipes;
        const bool add_agg_info = processed_stage_ == QueryProcessingStage::WithMergeableState;

        for (const auto & replicas : cluster->getShardsAddresses())
        {
            for (const auto & node : replicas)
            {
                auto connection = std::make_shared<Connection>(
                    node.host_name,
                    node.port,
                    context_->getGlobalContext()->getCurrentDatabase(),
                    node.user,
                    node.password,
                    node.cluster,
                    node.cluster_secret,
                    "HiveCluster",
                    node.compression,
                    node.secure);

                auto task_iter_callback = std::make_shared<TaskIterator>([node]() { return node.host_name; });
                auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                    connection,
                    queryToString(query_info_.query),
                    header,
                    context_,
                    nullptr,
                    scalars,
                    Tables(),
                    processed_stage_,
                    RemoteQueryExecutor::Extension{.task_iterator = iterate_callback_builder->buildCallback(node)});
                pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, add_agg_info, false));
            }
        }
        metadata_snapshot_->check(column_names_, getVirtuals(), getStorageID());
        return Pipe::unitePipes(std::move(pipes));
    }

    auto files_collector = HiveTaskPolicyFactory::instance().getFilesCollector(policy_name);
    if (!files_collector)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown hive task policy : {}", policy_name);
    String task_resp = context_->getReadTaskCallback()();
    HiveTaskPackage task_package;
    stringToPackage(task_resp, task_package);
    files_collector->setupCallbackData(task_package.data);
    IHiveTaskFilesCollector::Arguments args
        = {.context = context_,
           .query_info = &query_info_,
           .hive_metastore_url = hive_metastore_url,
           .hive_database = hive_database,
           .hive_table = hive_table,
           .storage_settings = storage_settings,
           .columns = getInMemoryMetadata().getColumns(),
           .num_streams = num_streams_,
           .partition_by_ast = partition_by_ast};
    files_collector->setupArgs(args);
    auto files_collector_builder = [&files_collector]() { return files_collector; };


    //const auto & client_info = context_->getClientInfo();
    //LOG_TRACE(
    //    logger,
    //    "replica info. count_participating_replicas:{}, number_of_current_replica:{}",
    //    client_info.count_participating_replicas,
    //    client_info.number_of_current_replica);


    // second stage, create local hive storage reading pipeline
    auto local_storage_settings = std::make_unique<HiveSettings>();
    local_storage_settings->applyChanges(*storage_settings);
    auto metadata = getInMemoryMetadataPtr();
    auto storage_hive = StorageHive::create(
        hive_metastore_url,
        hive_database,
        hive_table,
        StorageID("StorageHiveCluster", hive_database + "_" + hive_table),
        metadata->columns,
        metadata->constraints,
        metadata->comment,
        partition_by_ast,
        std::move(local_storage_settings),
        context_,
        std::make_shared<HiveTaskFilesCollectorBuilder>(files_collector_builder));

    return storage_hive->read(column_names_, metadata_snapshot_, query_info_, context_, processed_stage_, max_block_size_, num_streams_);
}

QueryProcessingStage::Enum StorageHiveCluster::getQueryProcessingStage(
    ContextPtr context_, QueryProcessingStage::Enum to_stage_, const StorageMetadataPtr &, SelectQueryInfo &) const
{
    if (context_->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        if (to_stage_ >= QueryProcessingStage::Enum::WithMergeableState)
            return QueryProcessingStage::Enum::WithMergeableState;
    return QueryProcessingStage::Enum::FetchColumns;
}

void registerStorageHiveCluster(StorageFactory & factory_)
{
    factory_.registerStorage(
        "HiveCluster",
        [](const StorageFactory::Arguments & args) {
            bool have_settings = args.storage_def->settings;
            std::unique_ptr<HiveSettings> hive_settings = std::make_unique<HiveSettings>();
            if (have_settings)
                hive_settings->loadFromQuery(*args.storage_def);

            ASTs engine_args = args.engine_args;
            //for (const auto & ast : engine_args)
            //{
            //    LOG_TRACE(&Poco::Logger::get("StorageHiveCluster"), "engin arg: {}", ast->dumpTree());
            //}
            if (engine_args.size() != 4)
                throw Exception(
                    "StorageHiveCluster requires 3 parameters: cluster, hive metadata server url, hive database and hive table",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            auto * partition_by = args.storage_def->partition_by;
            if (!partition_by)
                throw Exception("StorageHiveCluster requires partition by clause", ErrorCodes::BAD_ARGUMENTS);

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

            const String & cluster_name = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
            const String & hive_metastore_url = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            const String & hive_database = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
            const String & hive_table = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
            /*
            LOG_TRACE(
                &Poco::Logger::get("StorageHiveCluster"),
                "settings: {}. \n"
                "cluster:{}, hive url:{}, database: {}, table: {}\n"
                "database:{}, table:{}\n"
                "columns: {}\n"
                "partition by ast: {}",
                hive_settings->toString(),
                cluster_name,
                hive_metastore_url,
                hive_database,
                hive_table,
                args.table_id.getDatabaseName(),
                args.table_id.getTableName(),
                args.columns.toString(),
                partition_by->dumpTree());
            */

            return StorageHiveCluster::create(
                cluster_name,
                hive_metastore_url,
                hive_database,
                hive_table,
                args.table_id,
                args.columns,
                args.constraints,
                args.comment,
                partition_by->ptr(),
                std::move(hive_settings),
                args.getContext());
        },
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .supports_sort_order = true,
        });
}
} // namespace  DB

#endif
