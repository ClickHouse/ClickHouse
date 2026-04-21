#include <Interpreters/InterpreterAlterShardQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
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
            /// `ADD REPLICA` only attaches an existing replica named collection (created via `CREATE REPLICA`) —
            /// no property patch is accepted here (the parser rejects `PROPERTIES`). Use `ALTER REPLICA` to
            /// mutate replica properties.
            /// `CREATE_SHARD` alone must not let the caller bring in a named collection they are not entitled
            /// to use, so check `NAMED_COLLECTION` access up-front — before the `ON CLUSTER` dispatch — to
            /// match the contract of `CREATE SHARD`, `CREATE CLUSTER`, and `ALTER CLUSTER`.
            current_context->checkAccess(AccessType::NAMED_COLLECTION, query.replica_name);
            break;
        case AlterShardCommand::DropReplica:
            break;
        case AlterShardCommand::ReplaceReplicas:
            if (!query.shard_definition_properties.empty())
                PropertyValidation::Shard::validatePatchAssignments(query.shard_definition_properties);
            /// `REPLACE ... TO <new>` introduces named collections `<new>` as shard replicas — the factory
            /// resolves them against `NamedCollectionFactory`. Check `NAMED_COLLECTION` per target name
            /// up-front (same contract as `CREATE SHARD` and `ALTER SHARD ADD REPLICA`). `from_collections`
            /// are existing attachments of this shard and are looked up in the shard's own state, not
            /// re-read from the named collection factory, so they do not need an additional check here.
            for (const auto & clause : query.replica_replace_clauses)
                for (const auto & to_name : clause.to_collections)
                    current_context->checkAccess(AccessType::NAMED_COLLECTION, to_name);
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
            ClusterFactory::instance().updateShardPropertiesFromSQL(query);
            break;
        case AlterShardCommand::AddReplica:
            ClusterFactory::instance().addReplicaToShardFromSQL(query);
            break;
        case AlterShardCommand::DropReplica:
            ClusterFactory::instance().dropReplicaFromShardFromSQL(query);
            break;
        case AlterShardCommand::ReplaceReplicas:
            ClusterFactory::instance().replaceShardReplicasFromSQL(query);
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
