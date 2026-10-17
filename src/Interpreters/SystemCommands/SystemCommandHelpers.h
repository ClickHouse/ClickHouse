#pragma once

#include <Interpreters/SystemCommands/SystemCommandFactory.h>

#include <string_view>


namespace DB
{

constexpr std::string_view table_is_not_replicated = "Table {} is not replicated";

/// Throws the exception for a type of `SYSTEM` query that has no implementation on this server.
[[noreturn]] void throwUnknownSystemQueryType();

/// Functions that build the `ON CLUSTER` privileges of a command.

/// No privileges.
SystemCommandFactory::AccessOnClusterFn noAccessOnCluster();

/// A global privilege.
SystemCommandFactory::AccessOnClusterFn globalAccessOnCluster(AccessType access_type);

/// A global privilege if the query has no table, otherwise the privilege on the table.
SystemCommandFactory::AccessOnClusterFn tableOrGlobalAccessOnCluster(AccessType access_type);

/// The privilege on the database and the table of the query (either may be empty).
SystemCommandFactory::AccessOnClusterFn tableAccessOnCluster(AccessType access_type);

/// The privilege on the database of the query.
SystemCommandFactory::AccessOnClusterFn databaseAccessOnCluster(AccessType access_type);

/// Each function below registers one group of commands. They are called from the constructor of `SystemCommandFactory`.
void registerSystemCommandsActionLocks(SystemCommandFactory & factory);
void registerSystemCommandsCaches(SystemCommandFactory & factory);
void registerSystemCommandsDropReplica(SystemCommandFactory & factory);
void registerSystemCommandsReload(SystemCommandFactory & factory);
void registerSystemCommandsReplication(SystemCommandFactory & factory);
void registerSystemCommandsServer(SystemCommandFactory & factory);
void registerSystemCommandsStorage(SystemCommandFactory & factory);
void registerSystemCommandsUnsupported(SystemCommandFactory & factory);
void registerSystemCommandsViews(SystemCommandFactory & factory);

}
