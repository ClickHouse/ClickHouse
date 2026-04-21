#include <Interpreters/InterpreterCreateClusterCatalogQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateClusterCatalogQuery.h>

namespace DB
{

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
        validateAndExtractClusterLevelProperties(query.properties, cluster_secret, allow_distributed_ddl_queries);
    else
        validateAndExtractShardLevelProperties(query.properties, shard_weight, shard_internal_replication);

    if (is_cluster)
    {
        /// Members are either existing SQL `SHARD` definitions or whole-shard named collections (`replicas` key).
        /// `ClusterFactory::createCluster` loads the latter from `NamedCollectionFactory`; `CREATE_CLUSTER` alone
        /// must not let the caller reference collections they are not allowed to use.
        ///
        /// `hasShard` uses an unlocked snapshot, so member classification here is best-effort: between this check
        /// and the factory's critical section another session could `DROP SHARD` / `CREATE NAMED COLLECTION` with
        /// the same name. Existence and name-ambiguity are re-validated inside `ClusterFactory::createCluster`
        /// under its lock (`checkSQLClusterMemberNameLocked` + `namedCollectionExists`), which is what ultimately
        /// guards the catalog — this pre-check simply rejects unauthorised NC references early.
        for (const auto & member : query.members)
        {
            if (!ClusterFactory::instance().hasShard(member))
                current_context->checkAccess(AccessType::NAMED_COLLECTION, member);
        }
    }
    else
    {
        /// Each replica names an existing `TYPE REPLICA` collection resolved in `ClusterFactory::createShard`.
        /// `CREATE_SHARD` alone must not let the caller reference collections they are not allowed to use.
        for (const auto & replica_name : query.members)
            current_context->checkAccess(AccessType::NAMED_COLLECTION, replica_name);
    }

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

    /// Existence (`remote_servers`, discovery, SQL catalog row, materialized registry) is enforced inside
    /// `ClusterFactory::create{Cluster,Shard}` under one critical section — avoids `IF NOT EXISTS` races
    /// with concurrent DDL.
    if (is_cluster)
    {
        ClusterFactory::instance().createCluster(
            query.name,
            query.members,
            cluster_secret,
            allow_distributed_ddl_queries,
            query.if_not_exists);
    }
    else
    {
        ClusterFactory::instance().createShard(
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
