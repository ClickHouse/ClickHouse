#include <Interpreters/SystemCommands/SystemCommandHelpers.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

/// The commands that the parser accepts but this server does not implement.
void registerSystemCommandsUnsupported(SystemCommandFactory & factory)
{
    using Type = ASTSystemQuery::Type;

    auto register_unknown = [&](Type type, SystemCommandFactory::AccessOnClusterFn access_on_cluster)
    {
        factory.registerCommand(type, std::move(access_on_cluster), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
        {
            throwUnknownSystemQueryType();
        });
    };

    register_unknown(Type::DROP_CATALOG_REPLICA, globalAccessOnCluster(AccessType::SYSTEM_DROP_REPLICA));
    register_unknown(Type::STOP_REPLICATED_DDL_QUERIES, globalAccessOnCluster(AccessType::SYSTEM_REPLICATION_QUEUES));
    register_unknown(Type::START_REPLICATED_DDL_QUERIES, globalAccessOnCluster(AccessType::SYSTEM_REPLICATION_QUEUES));
    register_unknown(Type::STOP_VIRTUAL_PARTS_UPDATE, tableOrGlobalAccessOnCluster(AccessType::SYSTEM_VIRTUAL_PARTS_UPDATE));
    register_unknown(Type::START_VIRTUAL_PARTS_UPDATE, tableOrGlobalAccessOnCluster(AccessType::SYSTEM_VIRTUAL_PARTS_UPDATE));
    register_unknown(Type::STOP_REDUCE_BLOCKING_PARTS, tableOrGlobalAccessOnCluster(AccessType::SYSTEM_REDUCE_BLOCKING_PARTS));
    register_unknown(Type::START_REDUCE_BLOCKING_PARTS, tableOrGlobalAccessOnCluster(AccessType::SYSTEM_REDUCE_BLOCKING_PARTS));
    register_unknown(Type::UNLOCK_SNAPSHOT, globalAccessOnCluster(AccessType::BACKUP));

    for (const auto type : {Type::REPLICA_READY, Type::REPLICA_UNREADY})
    {
        factory.registerCommand(
            type,
            [](const ASTSystemQuery &) -> AccessRightsElements
            {
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Not implemented");
            },
            [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
            {
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Not implemented");
            });
    }
}

}
