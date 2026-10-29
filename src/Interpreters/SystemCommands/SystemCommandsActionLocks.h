#pragma once

#include <Interpreters/SystemCommands/ISystemCommand.h>
#include <Common/ActionLock.h>


namespace DB
{

class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

/// Implements `SYSTEM [START|STOP] <action from ActionLocks>`: for the volume of the query, for the table of the query,
/// or, if the query has neither, for all tables that the user has access to.
void startStopAction(SystemCommandContext & context, StorageActionBlockType action_type, bool start);

/// Starts or stops the action for all tables of the database that the user has access to.
void startStopActionInDatabase(
    StorageActionBlockType action_type,
    bool start,
    const String & database_name,
    const DatabasePtr & database,
    const ContextPtr & local_context,
    LoggerPtr log);

}
