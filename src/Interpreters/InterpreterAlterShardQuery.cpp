#include <Interpreters/InterpreterAlterShardQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterMetadataManager.h>
#include <Common/Clusters/PropertyValidation.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAlterShardQuery.h>

#include <Common/Exception.h>


namespace DB
{

using namespace SQLClusterCatalog;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
            PropertyValidation::Shard::validatePatchAssignments(query.shard_definition_properties);
            break;
        case AlterShardCommand::AddReplica:
            if (!ClusterMetadataManager::instance().tryGetEndpoint(query.replica_name))
                current_context->checkAccess(AccessType::CREATE_ENDPOINT);
            break;
        case AlterShardCommand::DropReplica:
            break;
        case AlterShardCommand::ReplaceReplicas:
            if (!query.shard_definition_properties.empty())
                PropertyValidation::Shard::validatePatchAssignments(query.shard_definition_properties);
            for (const auto & clause : query.replica_replace_clauses)
                for (const auto & to_name : clause.to_collections)
                    if (!ClusterMetadataManager::instance().tryGetEndpoint(to_name))
                        current_context->checkAccess(AccessType::CREATE_ENDPOINT);
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
            ClusterMetadataManager::instance().updateShardPropertiesFromSQL(query);
            break;
        case AlterShardCommand::AddReplica:
            ClusterMetadataManager::instance().addReplicaToShardFromSQL(query);
            break;
        case AlterShardCommand::DropReplica:
            ClusterMetadataManager::instance().dropReplicaFromShardFromSQL(query);
            break;
        case AlterShardCommand::ReplaceReplicas:
            ClusterMetadataManager::instance().replaceShardReplicasFromSQL(query);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ALTER SHARD: unsupported command after validation");
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
