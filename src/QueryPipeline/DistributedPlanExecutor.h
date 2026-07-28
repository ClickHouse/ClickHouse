#pragma once

#include <Processors/Chunk.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <IO/Progress.h>

#include <Common/DequeWithMemoryTracking.h>
#include <Common/SettingsChanges.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/ProtocolDefines.h>

namespace DB
{

/// Network endpoint of a worker, resolved on the initiator from the cluster config (and
/// server-level defaults). Both ports may differ per node so several workers can share a host.
struct WorkerAddress
{
    String host;
    UInt16 stateless_worker_port = 0;    /// interserver HTTP port the initiator dispatches tasks to
    UInt16 streaming_exchange_port = 0;  /// port this node accepts streaming-exchange peer connections on
};

/// Producer endpoint of an exchange stream, shipped to consumers so they dial the producer's
/// actual streaming-exchange port.
struct StreamSourceAddress
{
    String host;
    UInt16 port = 0;
};

class TaskToHostMap : public boost::noncopyable
{
public:
    TaskToHostMap(const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_);

    const VectorWithMemoryTracking<WorkerAddress> & getWorkerAddresses() const { return worker_addresses; }
    const UnorderedMapWithMemoryTracking<String, WorkerAddress> & getTaskHosts() const { return task_hosts; }
    const UnorderedMapWithMemoryTracking<String, StreamSourceAddress> & getExchangeStreamSourceHosts() const { return exchange_stream_source_hosts; }

private:
    void fillWorkerAddresses(ContextPtr context);
    void assignHostsForTasks(const DistributedQueryPlan & distributed_query_plan);

    VectorWithMemoryTracking<WorkerAddress> worker_addresses;
    UnorderedMapWithMemoryTracking<String, WorkerAddress> task_hosts;
    UnorderedMapWithMemoryTracking<String, StreamSourceAddress> exchange_stream_source_hosts;
};

using TaskToHostMapPtr = std::shared_ptr<const TaskToHostMap>;

struct DistributedQueryPlan;

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

/// Implements distributed query plan execution logic by executing stages according to dependencies between them.
class DistributedQueryPlanExecutor
{
public:
    virtual ~DistributedQueryPlanExecutor() = default;

    void start();
    bool execute(); /// Returns true if the execution is finished, false if it is still in progress and should be called again later.

    virtual void cleanup() = 0;

private:
    void startStageWithDependencies(const String & stage_name, UnorderedSetWithMemoryTracking<String> & executed_stages);

protected:
    DistributedQueryPlanExecutor(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_, std::shared_ptr<std::atomic<bool>> is_cancelled_);

    virtual void startStage(const String & stage_name, const DistributedQueryStage & stage) = 0;
    virtual bool waitForStage(const String & stage_name, std::optional<UInt64> timeout_ms) = 0;

    void checkCancelled() const;

    const UUID unique_query_id;
    const DistributedQueryPlan & distributed_query_plan;
    ContextPtr context;
    QueryStatusPtr query_status;
    std::shared_ptr<std::atomic<bool>> is_cancelled;
    DequeWithMemoryTracking<String> running_stages;
    LoggerPtr logger;
};

std::unique_ptr<DistributedQueryPlanExecutor> createDistributedQueryExecutor(
    const UUID & unique_query_id,
    const DistributedQueryPlan & distributed_query_plan,
    TaskToHostMapPtr task_to_host_map,
    ContextPtr context,
    std::shared_ptr<std::atomic<bool>> is_cancelled);

/// Wake every in-memory exchange waiter of the query. Idempotent and lock-free with respect to the
/// executor lifecycle, so cancellation paths can call it without waiting for the executor mutex.
void cancelDistributedQueryInMemoryExchanges(const UUID & unique_query_id);

/// Contains info about hosts assigned to exchange buckets
struct ExchangeStreamSources
{
    /// Exchange stream id -> producer endpoint (host + that producer's streaming-exchange port)
    UnorderedMapWithMemoryTracking<String, StreamSourceAddress> stream_hosts;
};

/// Minimal serialization version: v1 if every producer uses the server-level exchange port (a v1 worker
/// derives it locally), else v2.
UInt64 chooseTaskSerializationVersion(const ExchangeStreamSources & exchange_stream_sources, UInt64 server_exchange_port);

/// Contains all info to send a task to remote worker
struct DistributedQueryTaskDescription
{
    String initial_query_id;
    DistributedQueryTask task;
    String serialized_query_plan;
    ExchangeDescriptions exchanges;
    ExchangeStreamSources exchange_stream_sources;
    /// The initiator's changed settings, applied on the worker so query limits and execution-affecting
    /// settings (e.g. max_memory_usage) are honored remotely.
    SettingsChanges settings_changes;
    /// Wire-format version to emit, lowered to v1 for legacy-port-only tasks (rolling-upgrade safe).
    UInt64 serialization_version = DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION;
};

/// Executes a task locally. `distributed_query_id` is the node-independent identifier of the whole
/// distributed query (the same value on every node); it keys the in-memory and streaming exchanges,
/// while `object_storage_path` locates this node's persisted temporary files.
void doExecuteTask(const DistributedQueryTaskDescription & task, ObjectStoragePtr object_storage,
    const String & object_storage_path, const String & distributed_query_id, ContextMutablePtr context,
    std::function<bool()> is_cancelled = nullptr, ProgressCallback progress_callback = nullptr);

/// Returns object storage and path for temporary files
std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context);

struct ITemporaryFileLookup;
using TemporaryFileLookupPtr = std::shared_ptr<ITemporaryFileLookup>;

/// ITemporaryFileLookup that is used in buildQueryPipeline() to create readers and writers for temporary files by temporary file logical names
TemporaryFileLookupPtr createTemporaryFilesLookup(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & input_temporary_files_, const Strings & output_temporary_files_);

struct IExchangeLookup;
using ExchangeLookupPtr = std::shared_ptr<IExchangeLookup>;

struct ExchangeDescription;

ExchangeLookupPtr createExchangeLookup(
    const String & query_id,
    const ExchangeDescriptions & exchanges_,
    const ExchangeStreamSources & exchange_stream_sources,
    TemporaryFileLookupPtr temporary_files_,
    ContextPtr context);

class ICustomResourceHolder;

/// Helper to clean temporary files after query execution
std::shared_ptr<ICustomResourceHolder> makeTemporaryFilesCleaner(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & temporary_files_);

/// Helper to drop the query's in-memory exchanges once the query pipeline is destroyed.
std::shared_ptr<ICustomResourceHolder> makeInMemoryExchangesCleaner(const String & query_id);

}
