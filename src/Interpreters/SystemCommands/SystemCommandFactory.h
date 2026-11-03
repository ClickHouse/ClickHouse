#pragma once

#include <Access/Common/AccessType.h>
#include <Interpreters/SystemCommands/ISystemCommand.h>
#include <Parsers/ASTSystemQuery.h>

#include <boost/noncopyable.hpp>

#include <unordered_map>


namespace DB
{

/// The registry of all `SYSTEM` commands, one per `ASTSystemQuery::Type`.
/// Every type except `UNKNOWN` and `END` must be registered: the constructor checks this.
class SystemCommandFactory : private boost::noncopyable
{
public:
    using Type = ASTSystemQuery::Type;
    /// Returns the privileges required to run the query `ON CLUSTER`.
    using AccessOnClusterFn = std::function<AccessRightsElements(const ASTSystemQuery &)>;
    using ExecuteFn = std::function<BlockIO(ASTSystemQuery &, SystemCommandContext &)>;
    using ActionFn = std::function<void(ASTSystemQuery &, SystemCommandContext &)>;

    static SystemCommandFactory & instance();

    void registerCommand(Type type, SystemCommandPtr command);

    /// A command that checks its privileges locally by itself.
    void registerCommand(Type type, AccessOnClusterFn access_on_cluster, ExecuteFn execute);

    /// A command that requires one global privilege, both locally and `ON CLUSTER`, and returns no result.
    void registerSimpleCommand(Type type, AccessType access_type, ActionFn action);

    /// Throws `BAD_ARGUMENTS` for `UNKNOWN` and `END`.
    const ISystemCommand & get(Type type) const;

private:
    SystemCommandFactory();

    std::unordered_map<Type, SystemCommandPtr> commands;
};

}
