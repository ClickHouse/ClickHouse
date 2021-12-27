#pragma once

#include <DataStreams/BlockIO.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace zkutil
{
    class ZooKeeper;
}

namespace DB
{

class AccessRightsElements;
struct DDLLogEntry;


/// Returns true if provided ALTER type can be executed ON CLUSTER
bool isSupportedAlterType(int type);

/// Pushes distributed DDL query to the queue.
/// Returns DDLQueryStatusInputStream, which reads results of query execution on each host in the cluster.
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context);
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context, const AccessRightsElements & query_requires_access);
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context, AccessRightsElements && query_requires_access);

BlockIO getDistributedDDLStatus(const String & node_path, const DDLLogEntry & entry, ContextPtr context, const std::optional<Strings> & hosts_to_wait = {});

class DDLQueryStatusInputStream final : public IBlockInputStream
{
public:
    DDLQueryStatusInputStream(const String & zk_node_path, const DDLLogEntry & entry, ContextPtr context_, const std::optional<Strings> & hosts_to_wait = {});

    String getName() const override { return "DDLQueryStatusInputStream"; }

    Block getHeader() const override { return sample; }

    Block getSampleBlock() const { return sample.cloneEmpty(); }

    Block readImpl() override;

private:

    static Strings getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path);

    Strings getNewAndUpdate(const Strings & current_list_of_finished_hosts);

    std::pair<String, UInt16> parseHostAndPort(const String & host_id) const;

    String node_path;
    ContextPtr context;
    Stopwatch watch;
    Poco::Logger * log;

    Block sample;

    NameSet waiting_hosts;  /// hosts from task host list
    NameSet finished_hosts; /// finished hosts from host list
    NameSet ignoring_hosts; /// appeared hosts that are not in hosts list
    Strings current_active_hosts; /// Hosts that were in active state at the last check
    size_t num_hosts_finished = 0;

    /// Save the first detected error and throw it at the end of execution
    std::unique_ptr<Exception> first_exception;

    Int64 timeout_seconds = 120;
    bool by_hostname = true;
    bool throw_on_timeout = true;
    bool timeout_exceeded = false;
};

}
