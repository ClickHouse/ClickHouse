#pragma once
#include <Interpreters/DDLWorker.h>

namespace DB
{

class DatabaseReplicated;

class DatabaseReplicatedDDLWorker : public DDLWorker
{
public:
    DatabaseReplicatedDDLWorker(DatabaseReplicated * db, const Context & context_);

    String enqueueQuery(DDLLogEntry & entry) override;

    String tryEnqueueAndExecuteEntry(DDLLogEntry & entry, const Context & query_context);

private:
    void initializeMainThread() override;
    void initializeReplication();

    DDLTaskPtr initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper) override;
    bool canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & stat) override;

    DatabaseReplicated * const database;
    mutable std::mutex mutex;
    std::condition_variable wait_current_task_change;
    String current_task;
    UInt32 logs_to_keep = std::numeric_limits<UInt32>::max();
};

}
