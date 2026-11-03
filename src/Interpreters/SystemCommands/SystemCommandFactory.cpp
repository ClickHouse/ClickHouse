#include <Interpreters/SystemCommands/SystemCommandFactory.h>
#include <Interpreters/SystemCommands/SystemCommandHelpers.h>

#include <Interpreters/Context.h>
#include <base/EnumReflection.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

class FunctionSystemCommand : public ISystemCommand
{
public:
    FunctionSystemCommand(SystemCommandFactory::AccessOnClusterFn access_on_cluster_, SystemCommandFactory::ExecuteFn execute_)
        : access_on_cluster(std::move(access_on_cluster_)), execute_fn(std::move(execute_))
    {
    }

    AccessRightsElements getRequiredAccessOnCluster(const ASTSystemQuery & query) const override { return access_on_cluster(query); }

    BlockIO execute(ASTSystemQuery & query, SystemCommandContext & context) const override { return execute_fn(query, context); }

private:
    SystemCommandFactory::AccessOnClusterFn access_on_cluster;
    SystemCommandFactory::ExecuteFn execute_fn;
};

}

void throwUnknownSystemQueryType()
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of SYSTEM query");
}

SystemCommandFactory::AccessOnClusterFn noAccessOnCluster()
{
    return [](const ASTSystemQuery &) { return AccessRightsElements{}; };
}

SystemCommandFactory::AccessOnClusterFn globalAccessOnCluster(AccessType access_type)
{
    return [access_type](const ASTSystemQuery &)
    {
        AccessRightsElements required_access;
        required_access.emplace_back(access_type);
        return required_access;
    };
}

SystemCommandFactory::AccessOnClusterFn tableOrGlobalAccessOnCluster(AccessType access_type)
{
    return [access_type](const ASTSystemQuery & query)
    {
        AccessRightsElements required_access;
        if (!query.table)
            required_access.emplace_back(access_type);
        else
            required_access.emplace_back(access_type, query.getDatabase(), query.getTable());
        return required_access;
    };
}

SystemCommandFactory::AccessOnClusterFn tableAccessOnCluster(AccessType access_type)
{
    return [access_type](const ASTSystemQuery & query)
    {
        AccessRightsElements required_access;
        required_access.emplace_back(access_type, query.getDatabase(), query.getTable());
        return required_access;
    };
}

SystemCommandFactory::AccessOnClusterFn databaseAccessOnCluster(AccessType access_type)
{
    return [access_type](const ASTSystemQuery & query)
    {
        AccessRightsElements required_access;
        required_access.emplace_back(access_type, query.getDatabase());
        return required_access;
    };
}

SystemCommandFactory::SystemCommandFactory()
{
    registerSystemCommandsActionLocks(*this);
    registerSystemCommandsCaches(*this);
    registerSystemCommandsDropReplica(*this);
    registerSystemCommandsReload(*this);
    registerSystemCommandsReplication(*this);
    registerSystemCommandsServer(*this);
    registerSystemCommandsStorage(*this);
    registerSystemCommandsUnsupported(*this);
    registerSystemCommandsViews(*this);

    for (const auto type : magic_enum::enum_values<Type>())
    {
        if (type == Type::UNKNOWN || type == Type::END)
            continue;
        if (!commands.contains(type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SYSTEM command {} is not registered", magic_enum::enum_name(type));
    }
}

SystemCommandFactory & SystemCommandFactory::instance()
{
    static SystemCommandFactory factory;
    return factory;
}

void SystemCommandFactory::registerCommand(Type type, SystemCommandPtr command)
{
    if (type == Type::UNKNOWN || type == Type::END)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot register SYSTEM command {}", magic_enum::enum_name(type));
    if (!commands.emplace(type, std::move(command)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SYSTEM command {} is registered twice", magic_enum::enum_name(type));
}

void SystemCommandFactory::registerCommand(Type type, AccessOnClusterFn access_on_cluster, ExecuteFn execute)
{
    registerCommand(type, std::make_unique<FunctionSystemCommand>(std::move(access_on_cluster), std::move(execute)));
}

void SystemCommandFactory::registerSimpleCommand(Type type, AccessType access_type, ActionFn action)
{
    registerCommand(
        type,
        globalAccessOnCluster(access_type),
        [access_type, action_fn = std::move(action)](ASTSystemQuery & query, SystemCommandContext & context)
        {
            context.getContext()->checkAccess(access_type);
            action_fn(query, context);
            return BlockIO{};
        });
}

const ISystemCommand & SystemCommandFactory::get(Type type) const
{
    auto it = commands.find(type);
    if (it == commands.end())
        throwUnknownSystemQueryType();
    return *it->second;
}

}
