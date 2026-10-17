#pragma once

#include <Access/Common/AccessRightsElement.h>
#include <Disks/IVolume.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <QueryPipeline/BlockIO.h>
#include <Common/Logger.h>

#include <functional>
#include <memory>


namespace DB
{

class ASTSystemQuery;

/// The state of one execution of a `SYSTEM` query on the local server, shared by all commands.
class SystemCommandContext : public WithMutableContext
{
public:
    SystemCommandContext(
        ContextMutablePtr context_, ContextMutablePtr system_context_, StorageID table_id_, VolumePtr volume_ptr_, LoggerPtr log_)
        : WithMutableContext(context_)
        , system_context(std::move(system_context_))
        , table_id(std::move(table_id_))
        , volume_ptr(std::move(volume_ptr_))
        , log(std::move(log_))
    {
    }

    /// The global context with the system profile applied.
    ContextMutablePtr system_context;
    /// Set up if the query contains a table name.
    StorageID table_id;
    /// Set up if the query contains a storage policy and a volume.
    VolumePtr volume_ptr;
    LoggerPtr log;
};

/// One `SYSTEM` command, such as `SYSTEM DROP MARK CACHE` or `SYSTEM SYNC REPLICA`.
/// Commands are registered in `SystemCommandFactory` and executed by `InterpreterSystemQuery`.
class ISystemCommand
{
public:
    virtual ~ISystemCommand() = default;

    /// The privileges that the initiator checks before sending the query `ON CLUSTER`.
    virtual AccessRightsElements getRequiredAccessOnCluster(const ASTSystemQuery & query) const = 0;

    /// Executes the command on the local server. The command checks the privileges by itself,
    /// because some commands check them per table or per database instead of requiring a global privilege.
    virtual BlockIO execute(ASTSystemQuery & query, SystemCommandContext & context) const = 0;
};

using SystemCommandPtr = std::unique_ptr<ISystemCommand>;

}
