#include <Interpreters/InterpreterCreateClusterCatalogQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterMetadataManager.h>
#include <Common/Clusters/PropertyValidation.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateClusterCatalogQuery.h>

namespace DB
{

using namespace SQLClusterCatalog;

namespace Setting
{
    extern const SettingsInt64 distributed_ddl_task_timeout;
}

BlockIO InterpreterCreateClusterCatalogQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTCreateClusterCatalogQuery &>();

    const bool is_cluster = query.kind == ASTCreateClusterCatalogQuery::Kind::Cluster;

    current_context->checkAccess(is_cluster ? AccessType::CREATE_CLUSTER : AccessType::CREATE_SHARD);

    String cluster_secret;
    bool allow_distributed_ddl_queries = true;
    UInt32 shard_weight = 1;
    bool shard_internal_replication = false;
    if (is_cluster)
        PropertyValidation::Cluster::validateAndExtract(query.properties, cluster_secret, allow_distributed_ddl_queries);
    else
        PropertyValidation::Shard::validateAndExtract(query.properties, shard_weight, shard_internal_replication);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        ContextPtr ddl_context = current_context;
        ContextMutablePtr ddl_context_override;
        if (query.sync && current_context->getSettingsRef()[Setting::distributed_ddl_task_timeout] == 0)
        {
            ddl_context_override = Context::createCopy(current_context);
            /// Match default `distributed_ddl_task_timeout` so `SYNC` waits for hosts instead of fire-and-forget.
            ddl_context_override->setSetting("distributed_ddl_task_timeout", Field{Int64{180}});
            ddl_context = ddl_context_override;
        }
        return executeDDLQueryOnCluster(updated_query, ddl_context, params);
    }

    /// Existence is enforced inside `ClusterMetadataManager::create{Cluster,Shard}` via the metadata DDL worker.
    if (is_cluster)
    {
        ClusterMetadataManager::instance().createCluster(
            query.name,
            query.members,
            cluster_secret,
            allow_distributed_ddl_queries,
            query.if_not_exists);
    }
    else
    {
        ClusterMetadataManager::instance().createShard(
            query.name,
            query.members,
            shard_weight,
            shard_internal_replication,
            query.if_not_exists);
    }
    return {};
}

void registerInterpreterCreateClusterCatalogQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterCreateClusterCatalogQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterCreateClusterCatalogQuery>(args.query, args.context); });
}

}
