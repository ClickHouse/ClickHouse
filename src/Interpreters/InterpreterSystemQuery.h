#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
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

private:
    ASTPtr query_ptr;
    Poco::Logger * log = nullptr;
    StorageID table_id = StorageID::createEmpty();      /// Will be set up if query contains table name
    VolumePtr volume_ptr;

    /// Tries to get a replicated table and restart it
    /// Returns pointer to a newly created table if the restart was successful
    StoragePtr tryRestartReplica(const StorageID & replica, ContextMutablePtr context, bool need_ddl_guard = true);

    void restartReplica(const StorageID & replica, ContextMutablePtr system_context);
    void restartReplicas(ContextMutablePtr system_context);
    void syncReplica(ASTSystemQuery & query);

    void syncReplicatedDatabase(ASTSystemQuery & query);

    void restoreReplica();

    void dropReplica(ASTSystemQuery & query);
    bool dropReplicaImpl(ASTSystemQuery & query, const StoragePtr & table);
    void flushDistributed(ASTSystemQuery & query);
    void restartDisk(String & name);

    AccessRightsElements getRequiredAccessForDDLOnCluster() const;
    void startStopAction(StorageActionBlockType action_type, bool start);

    void extendQueryLogElemImpl(QueryLogElement &, const ASTPtr &, ContextPtr) const override;
};


}
