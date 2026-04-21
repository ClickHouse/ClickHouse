#include <Interpreters/InterpreterCreateNamedCollectionQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Common/NamedCollections/NamedCollectionReservedKeys.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Core/ServerSettings.h>


namespace CurrentMetrics
{
    extern const Metric NamedCollection;
}

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_named_collection_num_to_throw;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_NAMED_COLLECTIONS;
}

BlockIO InterpreterCreateNamedCollectionQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTCreateNamedCollectionQuery &>();

    current_context->checkAccess(AccessType::CREATE_NAMED_COLLECTION, query.collection_name);

    /// User DDL is never allowed to populate internal reserved keys (see `NamedCollectionReservedKeys.h`).
    /// Interpreters like `InterpreterCreateReplicaQuery` that legitimately need to set them bypass this
    /// interpreter and talk to `NamedCollectionFactory` directly.
    assertNoReservedKeys(query.changes);

    UInt64 limit = getContext()->getGlobalContext()->getServerSettings()[ServerSetting::max_named_collection_num_to_throw];
    UInt64 count = CurrentMetrics::get(CurrentMetrics::NamedCollection);
    if (limit > 0 && count >= limit)
        throw Exception(ErrorCodes::TOO_MANY_NAMED_COLLECTIONS,
                        "Too many named collections. The limit (server configuration parameter `max_named_collection_num_to_throw`) "
                        "is set to {}, the current number is {}", limit, count);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    NamedCollectionFactory::instance().createFromSQL(query);

    return {};
}

void registerInterpreterCreateNamedCollectionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateNamedCollectionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateNamedCollectionQuery", create_fn);
}

}
