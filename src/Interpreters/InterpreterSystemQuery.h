#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Common/ActionLock.h>


namespace Poco { class Logger; }

namespace DB
{

class Context;
class AccessRightsElements;
class ASTSystemQuery;

class InterpreterSystemQuery : public IInterpreter
{
public:
    InterpreterSystemQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;
    Context & context;
    Poco::Logger * log = nullptr;
    StorageID table_id = StorageID::createEmpty();      /// Will be set up if query contains table name

    /// Tries to get a replicated table and restart it
    /// Returns pointer to a newly created table if the restart was successful
    StoragePtr tryRestartReplica(const StorageID & replica, Context & context, bool need_ddl_guard = true);

    void restartReplicas(Context & system_context);
    void syncReplica(ASTSystemQuery & query);
    void flushDistributed(ASTSystemQuery & query);

    AccessRightsElements getRequiredAccessForDDLOnCluster() const;
    void startStopAction(StorageActionBlockType action_type, bool start);
};


}
