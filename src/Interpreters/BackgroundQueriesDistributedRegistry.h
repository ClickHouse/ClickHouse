#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>

#include <functional>
#include <memory>

namespace Coordination
{
    struct Stat;
}

namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class BackgroundQueryHandle;
using BackgroundQueryHandlePtr = std::shared_ptr<BackgroundQueryHandle>;

/// Distributed registry of queries started with `run_query_in_background`.
/// Powers the `system.background_queries` table.
///
/// Each query owns a TTL node under `<zookeeper_path>/entries`.
/// The node is created just before query starts and is updated whether the query
/// succeeds or fails.
/// If it fails, the error code and message is recorded.
/// Other than that, no execution artifacts (e.g. Settings or ProfileEvents) are recorded in the registry.
///
/// If a server crashes or loses the keeper connection for a long time before restarting,
/// we need to avoid reporting the status as `Running` until the node is removed on TTL.
/// To achieve that, we keep an ephemeral node for each server's running process (i.e. incarnation) under `<zookeeper_path>/incarnations`,
/// and each running process is keeping its incarnation node alive (in case the keeper session expires) in a worker thread that periodically
/// checks that the node exists and if not recreates it.
/// So, `Running` entry whose incarnation node is gone is interpreted as `Unknown`.
class BackgroundQueriesDistributedRegistry : public std::enable_shared_from_this<BackgroundQueriesDistributedRegistry>
{
public:
    /// Serialized into the keeper, keep backwards compatible.
    enum class Status : int8_t
    {
        Running = 1,
        Finished = 2,
        Failed = 3,
        Unknown = 4,
        InternalRegistryError = 5,
    };

    struct Entry
    {
        String query_id;
        String host;
        String user;
        String incarnation_id;
        String query;
        Status status;
        Int32 exception_code;
        String exception;
        time_t submit_time = 0;
        time_t finish_time = 0;

        String toString() const;
        static Entry parse(const String & data);
    };

    explicit BackgroundQueriesDistributedRegistry(ContextPtr global_context_);
    ~BackgroundQueriesDistributedRegistry();

    BackgroundQueryHandlePtr registerQuery(const String & query_id, const String & user, const String & query);

    void forEach(const std::function<void(Entry)> & callback);

    void truncate();

    void shutdown();

private:
    friend class BackgroundQueryHandle;

    void finalizeQuery(BackgroundQueryHandle & handle, Status status, Int32 exception_code, const String & exception);

    zkutil::ZooKeeperPtr getZooKeeper() const;

    void ensureIncarnationNode(const zkutil::ZooKeeperPtr & zookeeper);
    zkutil::ZooKeeperPtr incarnation_node_session;

    void threadFunction();

    ContextPtr global_context;
    LoggerPtr log;

    const String host;
    const String incarnation_id;

    const String zookeeper_path;
    const String entries_path;
    const String entry_path_prefix;
    const String incarnations_path;
    const String incarnation_path;
    const UInt64 entry_ttl_ms;

    /// We want to set the status even during keeper blips/outages, hence the queue.
    struct EntryUpdate
    {
        String entry_path;
        Entry entry;
    };
    ConcurrentBoundedQueue<EntryUpdate> entry_asynchronous_update_queue;

    ThreadFromGlobalPool thread;
};

class BackgroundQueryHandle
{
public:
    void onFinish();
    void onException(int code, const String & message);

private:
    friend class BackgroundQueriesDistributedRegistry;

    BackgroundQueryHandle(
        std::weak_ptr<BackgroundQueriesDistributedRegistry> registry_,
        String entry_path_,
        BackgroundQueriesDistributedRegistry::Entry entry_)
        : registry(std::move(registry_)), entry_path(std::move(entry_path_)), entry(std::move(entry_))
    {
    }

    std::weak_ptr<BackgroundQueriesDistributedRegistry> registry;
    String entry_path;
    BackgroundQueriesDistributedRegistry::Entry entry;
};

}
