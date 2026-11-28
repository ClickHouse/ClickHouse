#pragma once

#include "config.h"

#include <atomic>
#include <exception>
#include <mutex>

#include <Processors/Chunk.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <IO/Progress.h>
#if CLICKHOUSE_CLOUD
#include <Server/StatelessWorker/StatelessWorkerAllocation_fwd.h>
#endif

#include <Common/DequeWithMemoryTracking.h>
#include <Common/SettingsChanges.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/WakeupFd.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/ProtocolDefines.h>

namespace DB
{

/// Node count Cascades should plan for, matching the executor's worker source:
/// `distributed_plan_workers_num` for local/Cloud-discovery execution, else the static worker
/// cluster size. Returns 0 when no source is available, so the caller can reject distributed planning.
size_t getCascadesPlanningNodeCount(ContextPtr context);

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
    /// Out-of-line so the `worker_allocation` deleter is instantiated where `StatelessWorkerAllocation` is complete.
    ~TaskToHostMap();

    const VectorWithMemoryTracking<WorkerAddress> & getWorkerAddresses() const { return worker_addresses; }
    const UnorderedMapWithMemoryTracking<String, WorkerAddress> & getTaskHosts() const { return task_hosts; }
    const UnorderedMapWithMemoryTracking<String, StreamSourceAddress> & getExchangeStreamSourceHosts() const { return exchange_stream_source_hosts; }

private:
    void fillWorkerAddresses(ContextPtr context);
    void assignHostsForTasks(const DistributedQueryPlan & distributed_query_plan);

    VectorWithMemoryTracking<WorkerAddress> worker_addresses;
    UnorderedMapWithMemoryTracking<String, WorkerAddress> task_hosts;
    UnorderedMapWithMemoryTracking<String, StreamSourceAddress> exchange_stream_source_hosts;
#if CLICKHOUSE_CLOUD
    StatelessWorkerAllocationPtr worker_allocation;  /// Keeps leased workers alive for the query lifetime
#endif
};

using TaskToHostMapPtr = std::shared_ptr<const TaskToHostMap>;

struct DistributedQueryPlan;

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

/// Notified when a task reaches a terminal state or the cancellation state changes, so the source
/// parked in the async queue wakes at once instead of at the end of its poll interval.
using StageWakeupPtr = std::shared_ptr<WakeupFd>;

/// Never throws: a lost wake-up only costs the waiter its poll interval, while the callers are task
/// threads and a `noexcept` cancellation path, where an escaping exception would end the process.
void notifyStageWakeup(const StageWakeupPtr & stage_wakeup) noexcept;

/// Cancellation state shared by everything that learns of a failure in a distributed query, with the
/// query's failure alongside the flag. One failing task makes its neighbours fail too (their exchange
/// sockets close), so failures are ranked and the record keeps the one that explains the most.
class DistributedQueryCancellation
{
public:
    DistributedQueryCancellation();

    /// Least first. A consequence is caused by another failure: a cancellation, or an exchange peer
    /// that went away. An unclassified failure has no code of its own: a task the worker does not
    /// know, or an HTTP error from a worker endpoint. A root cause is any other error.
    enum class FailureRank
    {
        Consequence,
        Unclassified,
        RootCause,
    };
    static FailureRank rankOf(int error_code);
    static bool isConsequence(int error_code) { return rankOf(error_code) == FailureRank::Consequence; }

    /// The pipeline cancelled the source: stop, with no failure to report. From here on only a root
    /// cause is recorded: cancelling the tasks causes consequences by the dozen, and the cancellation
    /// is what the client asked for. Takes no lock, so it never waits on a waiter that holds `mutex`.
    void cancel();
    bool isCancelledByPipeline() const { return cancelled_by_pipeline; }

    /// Record the exception as the query's failure and cancel. Returns true while the driving source
    /// still runs and reports the failure; false once it is done and the caller has to report it.
    bool recordException(std::exception_ptr exception);
    bool recordCurrentException() { return recordException(std::current_exception()); }

    bool isCancelled() const { return cancelled; }

    /// The recorded failure; null if the query was cancelled without one (or not at all).
    std::exception_ptr getFailure() const;

    /// Rethrow the recorded failure, if there is one.
    void rethrowIfFailed() const;

    /// Rethrow the recorded failure. Without one, throw `QUERY_WAS_CANCELLED` if cancelled.
    void throwIfCancelled() const;

    /// The driving source is done, finished or torn down. Called right before it reports the recorded
    /// failure: a failure recorded later is reported by whoever records it.
    void markExecutionFinished();

    /// Fires on every state change here and on every task reaching a terminal state.
    const StageWakeupPtr & getWakeup() const { return wakeup; }

private:
    void rethrowIfFailedLocked() const TSA_REQUIRES(mutex);

    std::atomic<bool> cancelled = false;
    std::atomic<bool> cancelled_by_pipeline = false;
    mutable std::mutex mutex;
    std::exception_ptr failure TSA_GUARDED_BY(mutex);
    FailureRank failure_rank TSA_GUARDED_BY(mutex) = FailureRank::Consequence;
    bool execution_finished TSA_GUARDED_BY(mutex) = false;
    StageWakeupPtr wakeup;
};

using DistributedQueryCancellationPtr = std::shared_ptr<DistributedQueryCancellation>;

/// Implements distributed query plan execution logic by executing stages according to dependencies between them.
class DistributedQueryPlanExecutor
{
public:
    virtual ~DistributedQueryPlanExecutor() = default;

    void start();
    /// Returns true if the execution is finished, false if it is still in progress and should be called again later.
    /// `poll_timeout_ms` is how long to wait for the current stage before giving up and returning; pass 0 to only
    /// look at the current state, so the caller can wait somewhere it does not hold an execution thread.
    bool execute(UInt64 poll_timeout_ms);

    virtual void cleanup() = 0;

private:
    void startStageWithDependencies(const String & stage_name, UnorderedSetWithMemoryTracking<String> & executed_stages);

protected:
    DistributedQueryPlanExecutor(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_, DistributedQueryCancellationPtr cancellation_, StageWakeupPtr stage_wakeup_);

    virtual void startStage(const String & stage_name, const DistributedQueryStage & stage) = 0;
    virtual bool waitForStage(const String & stage_name, std::optional<UInt64> timeout_ms) = 0;

    void checkCancelled() const;

    const UUID unique_query_id;
    const DistributedQueryPlan & distributed_query_plan;
    ContextPtr context;
    QueryStatusPtr query_status;
    DistributedQueryCancellationPtr cancellation;
    StageWakeupPtr stage_wakeup;
    DequeWithMemoryTracking<String> running_stages;
    LoggerPtr logger;
};

std::unique_ptr<DistributedQueryPlanExecutor> createDistributedQueryExecutor(
    const UUID & unique_query_id,
    const DistributedQueryPlan & distributed_query_plan,
    TaskToHostMapPtr task_to_host_map,
    ContextPtr context,
    DistributedQueryCancellationPtr cancellation,
    StageWakeupPtr stage_wakeup);

/// Wake every in-memory exchange waiter of the query; the waiters rethrow `failure` (or a
/// generic cancellation error when it is null) instead of treating the stream as complete.
/// Idempotent and lock-free with respect to the executor lifecycle, so cancellation paths can call
/// it without waiting for the executor mutex.
void cancelDistributedQueryInMemoryExchanges(const UUID & unique_query_id, std::exception_ptr failure);

/// Contains info about hosts assigned to exchange buckets
struct ExchangeStreamSources
{
    /// Exchange stream id -> producer endpoint (host + that producer's streaming-exchange port)
    UnorderedMapWithMemoryTracking<String, StreamSourceAddress> stream_hosts;
};

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
    /// Wire-format version of the task. A worker that knows only an older version rejects it.
    UInt64 serialization_version = DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION;
};

/// Executes a task locally. `distributed_query_id` is the node-independent identifier of the whole
/// distributed query (the same value on every node); it keys the in-memory and streaming exchanges,
/// while `object_storage_path` locates this node's persisted temporary files.
/// `execute_locally` is true only when the whole query runs in-process on the initiator, false on a
/// worker. It selects the exchange transport, so the caller must pass it (see `createExchangeLookup`).
void doExecuteTask(const DistributedQueryTaskDescription & task, ObjectStoragePtr object_storage,
    const String & object_storage_path, const String & distributed_query_id, ContextMutablePtr context,
    bool execute_locally, std::function<bool()> is_cancelled = nullptr, ProgressCallback progress_callback = nullptr);

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

/// `execute_locally` must be the value the plan was built with, not a fresh read of
/// `distributed_plan_execute_locally`: an ambient read can disagree with the plan and pick a
/// transport the plan holds no hosts for.
/// `cancellation` is the query's shared state on the initiator, for the streaming source that reads
/// the result (see `StreamingExchangeSource`); null on a worker.
ExchangeLookupPtr createExchangeLookup(
    const String & query_id,
    const ExchangeDescriptions & exchanges_,
    const ExchangeStreamSources & exchange_stream_sources,
    TemporaryFileLookupPtr temporary_files_,
    ContextPtr context,
    bool execute_locally,
    DistributedQueryCancellationPtr cancellation);

class IProcessor;

/// Transform that drops zero-row chunks emitted as scheduling ticks by in-memory exchange
/// sources, so they do not escape the exchange path (e.g. to the client as empty `Data` packets).
std::shared_ptr<IProcessor> makeSkipZeroRowChunksTransform(SharedHeader header);

class ICustomResourceHolder;

/// Helper to clean temporary files after query execution
std::shared_ptr<ICustomResourceHolder> makeTemporaryFilesCleaner(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & temporary_files_);

/// Helper to drop the query's in-memory exchanges once the query pipeline is destroyed.
std::shared_ptr<ICustomResourceHolder> makeInMemoryExchangesCleaner(const String & query_id);

}
