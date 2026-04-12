#include <Interpreters/InterpreterAlterShardQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTAlterShardQuery.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

BlockIO InterpreterAlterShardQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTAlterShardQuery &>();

    current_context->checkAccess(AccessType::CREATE_SHARD);

    switch (query.command)
    {
        case AlterShardCommand::ModifyShardProperties:
            validateShardLevelPropertyPatchAssignments(query.shard_definition_properties);
            break;
        case AlterShardCommand::AddReplica:
            if (!query.replica_properties.empty())
                validateReplicaLevelPropertyKeys(query.replica_properties);
            break;
        case AlterShardCommand::DropReplica:
            break;
        case AlterShardCommand::ReplaceReplicas:
            if (!query.shard_definition_properties.empty())
                validateShardLevelPropertyPatchAssignments(query.shard_definition_properties);
            break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER SHARD: this variant is not implemented yet");
    }

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    switch (query.command)
    {
        case AlterShardCommand::ModifyShardProperties:
            if (!ClusterFactory::instance().updateShardPropertiesFromSQL(query))
                return {};
            break;
        case AlterShardCommand::AddReplica:
            if (!query.replica_properties.empty())
            {
                current_context->checkAccess(AccessType::ALTER_NAMED_COLLECTION, query.replica_name);
                auto alter_nc = make_intrusive<ASTAlterNamedCollectionQuery>();
                alter_nc->collection_name = query.replica_name;
                alter_nc->changes = query.replica_properties;
                NamedCollectionFactory::instance().updateFromSQL(*alter_nc);
            }
            if (!ClusterFactory::instance().addReplicaToShardFromSQL(query))
                return {};
            break;
        case AlterShardCommand::DropReplica:
            if (!ClusterFactory::instance().dropReplicaFromShardFromSQL(query))
                return {};
            break;
        case AlterShardCommand::ReplaceReplicas:
            if (!ClusterFactory::instance().replaceShardReplicasFromSQL(query))
                return {};
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ALTER SHARD: unsupported command after validation");
    }

    auto global_context = current_context->getGlobalContext();
    for (const auto & cluster_name : ClusterFactory::instance().listSqlClustersContainingMember(query.shard_name))
    {
        if (auto cluster = ClusterFactory::instance().tryMaterializeCluster(cluster_name, global_context))
            global_context->setCluster(cluster_name, cluster);
    }

    return {};
}

void registerInterpreterAlterShardQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterAlterShardQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterAlterShardQuery>(args.query, args.context); });
}

}
