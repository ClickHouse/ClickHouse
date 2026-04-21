#include <Interpreters/InterpreterCreateReplicaQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Common/NamedCollections/NamedCollectionReservedKeys.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTCreateReplicaQuery.h>


namespace CurrentMetrics
{
    extern const Metric NamedCollection;
}

namespace DB
{

using namespace SQLClusterCatalog;

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_named_collection_num_to_throw;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_NAMED_COLLECTIONS;
}

BlockIO InterpreterCreateReplicaQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & replica_query = updated_query->as<const ASTCreateReplicaQuery &>();

    current_context->checkAccess(AccessType::CREATE_NAMED_COLLECTION, replica_query.replica_name);

    PropertyValidation::Replica::validateForSQLReplica(replica_query.properties);

    if (!replica_query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    /// Tag the backing named collection with `__type__='replica'` so `system.replicas_collection` and
    /// the replica-specific DDL interpreters (`ALTER REPLICA`, `DROP REPLICA`) can discriminate replicas
    /// from ordinary named collections. The reserved `__` prefix makes this key un-forgeable from user
    /// DDL (see `NamedCollectionReservedKeys.h` and the `assertNoReservedKeys` call in the plain
    /// `CREATE NAMED COLLECTION` interpreter).
    auto named_collection_ast = make_intrusive<ASTCreateNamedCollectionQuery>();
    named_collection_ast->collection_name = replica_query.replica_name;
    named_collection_ast->changes = replica_query.properties;
    named_collection_ast->changes.emplace_back(NAMED_COLLECTION_KIND_KEY, Field{String{NAMED_COLLECTION_KIND_REPLICA}});
    named_collection_ast->if_not_exists = replica_query.if_not_exists;

    /// We deliberately DO NOT call `InterpreterCreateNamedCollectionQuery` here: that interpreter rejects
    /// any reserved key via `assertNoReservedKeys`, which is exactly the right behavior for user DDL but
    /// would block our internal tag injection. The `max_named_collection_num_to_throw` limit check is
    /// therefore duplicated here to preserve the global cap on collection count.
    UInt64 limit = current_context->getGlobalContext()->getServerSettings()[ServerSetting::max_named_collection_num_to_throw];
    UInt64 count = CurrentMetrics::get(CurrentMetrics::NamedCollection);
    if (limit > 0 && count >= limit)
        throw Exception(
            ErrorCodes::TOO_MANY_NAMED_COLLECTIONS,
            "Too many named collections. The limit (server configuration parameter `max_named_collection_num_to_throw`) "
            "is set to {}, the current number is {}", limit, count);

    NamedCollectionFactory::instance().createFromSQL(*named_collection_ast);
    return {};
}

void registerInterpreterCreateReplicaQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterCreateReplicaQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterCreateReplicaQuery>(args.query, args.context); });
}

}
