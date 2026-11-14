#pragma once

#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DistributedQueryStatusSource.h>
#include <Parsers/IAST_fwd.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>

namespace DB
{
class DDLOnClusterQueryStatusSource final : public DistributedQueryStatusSource
{
public:
    DDLOnClusterQueryStatusSource(
        const String & zk_node_path,
        const String & zk_replicas_path,
        ContextPtr context_,
        const Strings & hosts_to_wait,
        const ASTPtr & query);

    String getName() const override { return "DDLOnClusterQueryStatus"; }

protected:
    ExecutionStatus checkStatus(const String & host_id) override;
    Chunk generateChunkWithUnfinishedHosts() const override;
    Strings getNodesToWait() override;
    Chunk handleTimeoutExceeded() override;
    Chunk stopWaitingOfflineHosts() override;
    void handleNonZeroStatusCode(const ExecutionStatus & status, const String & host_id) override;
    void fillHostStatus(const String & host_id, const ExecutionStatus & status, MutableColumns & columns) override;

private:
    DistributedDDLOutputMode output_mode;
    /// `CREATE` / `ALTER CLUSTER` (and any other DDL whose AST has secret parts) carry `secret` and
    /// `password` in the text a worker quotes back. The status code stays; the text must not.
    bool hide_worker_error_text = false;

    String publishedWorkerError(const ExecutionStatus & status) const;
    static bool nullableErrorAndStatusFields(DistributedDDLOutputMode output_mode);
    static Block getSampleBlock(DistributedDDLOutputMode output_mode);
};
}
