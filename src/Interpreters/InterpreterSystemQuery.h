#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/SyncReplicaMode.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Common/ActionLock.h>
#include <Disks/IVolume.h>


namespace Poco { class Logger; }

namespace DB
{

class Context;
class AccessRightsElements;
class ASTSystemQuery;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;
class RefreshTask;
using RefreshTaskPtr = std::shared_ptr<RefreshTask>;
using RefreshTaskList = std::list<RefreshTaskPtr>;


/** Implement various SYSTEM queries.
  * Examples: SYSTEM SHUTDOWN, SYSTEM DROP MARK CACHE.
  *
  * Some commands are intended to stop/start background actions for tables and comes with two variants:
  *
  * 1. SYSTEM STOP MERGES table, SYSTEM START MERGES table
  * - start/stop actions for specific table.
  *
  * 2. SYSTEM STOP MERGES, SYSTEM START MERGES
  * - start/stop actions for all existing tables.
  * Note that the actions for tables that will be created after this query will not be affected.
  */
class InterpreterSystemQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterSystemQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    static void startStopActionInDatabase(StorageActionBlockType action_type, bool start,
                                          const String & database_name, const DatabasePtr & database,
                                          const ContextPtr & local_context, LoggerPtr log);

    static bool trySyncReplica(StoragePtr table, SyncReplicaMode sync_replica_mode, const std::unordered_set<String> & src_replicas, ContextPtr context_);

private:
    ASTPtr query_ptr;
    LoggerPtr log = nullptr;
    StorageID table_id = StorageID::createEmpty();      /// Will be set up if query contains table name
    VolumePtr volume_ptr;

    /// Tries to get a replicated table and restart it
    /// Returns pointer to a newly created table if the restart was successful
    StoragePtr tryRestartReplica(const StorageID & replica, ContextMutablePtr context);

    void restartReplica(const StorageID & replica, ContextMutablePtr system_context);
    void restartReplicas(ContextMutablePtr system_context);
    void syncReplica(ASTSystemQuery & query);
    void setReplicaReadiness(bool ready);
    void waitLoadingParts();
    void unloadPrimaryKeys();

    void syncReplicatedDatabase(ASTSystemQuery & query);

    void syncTransactionLog();

    void restoreReplica();

    void dropReplica(ASTSystemQuery & query);
    bool dropReplicaImpl(ASTSystemQuery & query, const StoragePtr & table);
    void dropDatabaseReplica(ASTSystemQuery & query);
    void flushDistributed(ASTSystemQuery & query);
    [[noreturn]] void restartDisk(String & name);

    RefreshTaskList getRefreshTasks();

    AccessRightsElements getRequiredAccessForDDLOnCluster() const;
    void startStopAction(StorageActionBlockType action_type, bool start);
    void prewarmMarkCache();

    void stopReplicatedDDLQueries();
    void startReplicatedDDLQueries();
};


}
