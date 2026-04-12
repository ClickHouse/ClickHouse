#include <Interpreters/InterpreterCreateShardQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateShardQuery.h>

namespace DB
{

namespace Setting
{
    extern const SettingsInt64 distributed_ddl_task_timeout;
}

BlockIO InterpreterCreateShardQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTCreateShardQuery &>();

    current_context->checkAccess(AccessType::CREATE_SHARD);

    UInt32 weight = 1;
    bool internal_replication = false;
    validateAndExtractShardLevelProperties(query.shard_properties, weight, internal_replication);

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

    if (query.if_not_exists && ClusterFactory::instance().hasShard(query.shard_name))
        return {};

    ClusterFactory::instance().createShard(query.shard_name, query.replicas, weight, internal_replication);
    return {};
}

void registerInterpreterCreateShardQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterCreateShardQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterCreateShardQuery>(args.query, args.context); });
}

}
