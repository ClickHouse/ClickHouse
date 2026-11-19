#include "config.h"

#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <Common/scope_guard_safe.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <QueryPipeline/receiveExchangeStreams.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Processors/QueryPlan/Optimizations/Cascades/CascadesParams.h>
#include <Server/StatelessWorker/IStatelessWorkersProvider.h>
#include <Server/StatelessWorker/StatelessWorkerAllocation.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <QueryPipeline/printPipeline.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/TemporaryFiles.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/NativeCompressedSink.h>
#include <Common/ThreadStatus.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/QueryScope.h>
#include <Processors/Sources/NativeCompressedSource.h>
#include <Planner/Utils.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <Server/StatelessWorker/WorkerControlLoop.h>
#include <Server/DistributedQuery/StreamingExchangeLookup.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Processors/Transforms/MergeRuntimeFiltersTransform.h>
#include <Interpreters/Cluster.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/executeQuery.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <base/defines.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Message.h>


namespace ProfileEvents
{
    extern const Event DistributedPlanRemoteTasks;
    extern const Event DistributedPlanTaskBatches;
    extern const Event DistributedPlanTaskStatusRequests;
    extern const Event DistributedPlanTaskCancelRequests;
    extern const Event DistributedPlanTaskForgetRequests;
    extern const Event DistributedPlanLocalExecution;
    extern const Event DistributedPlanHostsUsed;
    extern const Event RuntimeFilterReceiveBranchFailures;
}


namespace DB
{

/// The comment at `MAX_TRANSPORTED_RUNTIME_FILTER_STATE_BYTES` explains why the two limits are equal.
static_assert(MAX_TRANSPORTED_RUNTIME_FILTER_STATE_BYTES == StreamingExchangeProtocol::MAX_DATA_PACKET_BODY_BYTES);

namespace Setting
{
    extern const SettingsBool distributed_plan_task_dispatch_batching;
    extern const SettingsUInt64 distributed_plan_task_dispatch_stages_per_flush;
    extern const SettingsBool distributed_plan_execute_locally;
    extern const SettingsBool enable_join_runtime_filters_index_analysis;
    extern const SettingsUInt64 distributed_plan_workers_num;
    extern const SettingsString distributed_plan_worker_cluster;
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int QUERY_WAS_CANCELLED;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int FAULT_INJECTED;
    extern const int SW_SERVER_NO_WORKERS_AVAILABLE;
    extern const int EXCHANGE_PEER_DISCONNECTED;
    extern const int INCORRECT_DATA;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
}

namespace FailPoints
{
    extern const char distributed_plan_record_failure_while_starting_tasks[];
    extern const char distributed_plan_local_start_stage_fails_after_first_task[];
    extern const char distributed_plan_runtime_filter_receive_branch_fails_before_connect[];
}

class TaskParameters : public IParameterLookup
{
public:
    explicit TaskParameters(const QueryPlanParameters & parameters_)
        : parameters(parameters_)
    {
    }

    [[nodiscard]] Field getParameter(const String & name) const override
    {
        auto it = parameters.parameters.find(name);
        if (it == parameters.parameters.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Parameter {} not found", name);
        return it->second;
    }

private:
    const QueryPlanParameters parameters;
};


/// Creates read and write buffers for temporary files in object storage using just logical name of the file.
class TemporaryFilesInObjectStorage : public ITemporaryFileLookup
{
public:
    TemporaryFilesInObjectStorage(ObjectStoragePtr object_storage_, const String & object_storage_path_,
        const Strings & input_temporary_files_, const Strings & output_temporary_files_)
        : object_storage(std::move(object_storage_))
        , object_storage_path(object_storage_path_)
        , input_temporary_files(input_temporary_files_.begin(), input_temporary_files_.end())
        , output_temporary_files(output_temporary_files_.begin(), output_temporary_files_.end())
    {
    }

    WriteBuffer & getTemporaryFileForWriting(const String & file_name) override
    {
        LOG_DEBUG(logger, "Writing to temporary file '{}'", file_name);

        if (!output_temporary_files.contains(file_name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected output temporary file requested: '{}'", file_name);
        StoredObject object(object_storage_path + "/" + file_name, file_name);
        write_buffers.emplace_back(object_storage->writeObject(object, WriteMode::Rewrite));
        return *write_buffers.back();
    }

    std::unique_ptr<ReadBuffer> getTemporaryFileForReading(const String & file_name) override
    {
        LOG_TRACE(logger, "Reading from temporary file '{}'", file_name);

        if (!input_temporary_files.contains(file_name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected input temporary file requested: '{}'", file_name);
        StoredObject object(object_storage_path + "/" + file_name, file_name);
        return object_storage->readObject(object, {});
    }

private:
    ObjectStoragePtr object_storage;
    const String object_storage_path;
    const UnorderedSetWithMemoryTracking<String> input_temporary_files;
    const UnorderedSetWithMemoryTracking<String> output_temporary_files;
    VectorWithMemoryTracking<std::unique_ptr<WriteBuffer>> write_buffers;
    LoggerPtr logger = getLogger("TemporaryFilesInObjectStorage");
};


class ExchangeViaTemporaryFiles : public IExchangeLookup
{
public:
    explicit ExchangeViaTemporaryFiles(TemporaryFileLookupPtr temporary_files_)
        : temporary_files(std::move(temporary_files_))
    {
    }

    /// `advisory` is ignored: a persisted stream has no peer, and its reader starts only after this
    /// stage finishes. A write failure is a failure of the object storage, so it stays an error.
    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId & exchange_stream_id, bool /*advisory*/) override
    {
        if (!temporary_files)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Object storage for Persisted exchanges is not configured, exchange stream id: {}",
                exchange_stream_id.toString());
        auto file_name = exchange_stream_id.toString();
        return std::make_shared<NativeCompressedSink>(input_header, temporary_files->getTemporaryFileForWriting(file_name), file_name);
    }

    /// `advisory` is ignored: a persisted stream is read only after its producer finished, and a
    /// missing file means a failed producer, which already fails the query.
    std::shared_ptr<ISource> createSource(
        SharedHeader output_header, const ExchangeStreamId & exchange_stream_id, bool output_is_serialized, bool /*advisory*/) override
    {
        if (!temporary_files)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Object storage for Persisted exchanges is not configured, exchange stream id: {}",
                exchange_stream_id.toString());
        if (output_is_serialized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Persisted exchange {} has no deserializer, its source gives data chunks", exchange_stream_id.toString());

        auto file_name = exchange_stream_id.toString();
        std::unique_ptr<QueryPipelineBuilder> pipeline_ptr = std::make_unique<QueryPipelineBuilder>();
        return std::make_shared<NativeCompressedSource>(output_header, temporary_files->getTemporaryFileForReading(file_name), file_name);
    }

private:
    TemporaryFileLookupPtr temporary_files;
};

/// Simple implementation of streaming exchange for local execution.
/// It just holds a queue of chunks in memory.
class InMemoryExchange : boost::noncopyable
{
public:
    explicit InMemoryExchange(const String & name_)
        : name(name_)
    {
    }

    /// Appends a data chunk. Throws if the exchange is cancelled, so the producer stops early.
    /// Drops the chunk if the reader is detached.
    void appendChunk(Chunk chunk)
    {
        LOG_TEST(log, "Appending chunk to exchange '{}', rows {}", name, chunk.getNumRows());

        std::lock_guard lock(mutex);
        if (cancelled)
            throwCancelled();
        if (reader_detached)
            return;
        chunks.emplace_back(std::move(chunk));
        has_data.notify_one();
    }

    /// Appends the end-of-data marker. Allowed after a cancel: the queued stream is complete,
    /// so a draining consumer may still use it.
    void finish()
    {
        LOG_TEST(log, "Finishing exchange '{}'", name);

        std::lock_guard lock(mutex);
        chunks.emplace_back(Chunk{});
        has_data.notify_one();
    }

    /// Wake any waiter. Consumers get `reason_` (or a generic cancellation error) instead of
    /// end-of-data, so an aborted stream cannot pass for a complete one.
    void cancel(std::exception_ptr reason_)
    {
        std::lock_guard lock(mutex);
        cancelled = true;
        if (!reason)
            reason = reason_;
        has_data.notify_all();
    }

    /// The reader stopped and does not need more data, e.g. its pipeline finished early.
    /// Wakes a blocked `getChunk`; chunks appended after this are dropped. Unlike `cancel`,
    /// this is not a failure: the producer stops this stream but finishes successfully.
    void detachReader()
    {
        std::lock_guard lock(mutex);
        reader_detached = true;
        has_data.notify_all();
    }

    /// True after `detachReader`: the reader is gone and appended chunks are dropped.
    bool isReaderDetached()
    {
        std::lock_guard lock(mutex);
        return reader_detached;
    }

    /// Waits up to `timeout` for a chunk. Returns std::nullopt if nothing arrived in time.
    /// An empty chunk is the producer's end-of-data marker. Chunks queued before a cancel are
    /// still handed out; once a cancelled queue is empty, throws the cancellation reason.
    std::optional<Chunk> getChunk(std::chrono::milliseconds timeout)
    {
        LOG_TEST(log, "Waiting for chunk from exchange '{}'", name);

        Chunk chunk;
        {
            std::unique_lock lock(mutex);
            if (!has_data.wait_for(lock, timeout, [this] { return !chunks.empty() || cancelled || reader_detached; }))
                return std::nullopt;
            /// The reader is stopping and does not need more data.
            if (reader_detached)
                return std::nullopt;
            /// The wait ended with an empty queue only when cancelled.
            if (chunks.empty())
                throwCancelled();
            chunk = std::move(chunks.front());
            chunks.pop_front();
        }

        LOG_TEST(log, "Got chunk from exchange '{}', rows {}", name, chunk.getNumRows());

        return chunk;
    }

private:
    [[noreturn]] void throwCancelled() const
    {
        if (reason)
            std::rethrow_exception(reason);
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
            "Distributed query was cancelled before exchange '{}' transferred all data", name);
    }

    LoggerPtr log = getLogger("InMemoryExchange");
    String name;
    std::mutex mutex;
    std::condition_variable has_data;
    DequeWithMemoryTracking<Chunk> chunks;
    bool cancelled = false;
    bool reader_detached = false;
    /// The first failure passed to `cancel`.
    std::exception_ptr reason;
};

using InMemoryExchangePtr = std::shared_ptr<InMemoryExchange>;


/// A map of in-memory exchanges addressed by their logical names
class InMemoryExchanges : boost::noncopyable
{
public:
    InMemoryExchangePtr getExchange(const String & query_id, const String & exchange_id)
    {
        std::lock_guard lock(mutex);
        auto & element = exchanges_by_query_id[query_id][exchange_id];
        if (!element)
        {
            element = std::make_shared<InMemoryExchange>(exchange_id);
            /// A task built concurrently with the cancellation may look up its exchange after
            /// cancelQuery already ran; hand it out pre-cancelled so its reads fail right away.
            if (auto cancelled_it = cancelled_queries.find(query_id); cancelled_it != cancelled_queries.end())
                element->cancel(cancelled_it->second);
        }
        return element;
    }

    /// Cancel every exchange of the query so waiting tasks stop, reporting `failure` as the
    /// reason (null for a plain cancellation). The exchanges stay in the registry so a result
    /// reader that looks one up afterwards still finds the produced chunks and their end-of-data
    /// marker; removeQuery drops them once the whole query pipeline is destroyed.
    void cancelQuery(const String & query_id, std::exception_ptr failure)
    {
        std::lock_guard lock(mutex);
        auto [cancelled_it, inserted] = cancelled_queries.emplace(query_id, failure);
        /// Keep the first failure, but let a later one replace a plain cancellation.
        if (!inserted && !cancelled_it->second && failure)
            cancelled_it->second = failure;
        auto it = exchanges_by_query_id.find(query_id);
        if (it == exchanges_by_query_id.end())
            return;
        for (auto & [_, exchange] : it->second)
            exchange->cancel(failure);
    }

    /// Drop the query's exchanges from the registry. Called when the query pipeline is destroyed.
    void removeQuery(const String & query_id)
    {
        std::lock_guard lock(mutex);
        exchanges_by_query_id.erase(query_id);
        cancelled_queries.erase(query_id);
    }

    static std::shared_ptr<InMemoryExchanges> instance()
    {
        static std::shared_ptr<InMemoryExchanges> self = std::make_shared<InMemoryExchanges>();
        return self;
    }

private:
    using InMemoryExchangeMap = UnorderedMapWithMemoryTracking<String, InMemoryExchangePtr>;

    UnorderedMapWithMemoryTracking<String, InMemoryExchangeMap> exchanges_by_query_id TSA_GUARDED_BY(mutex);
    /// Cancelled query -> its root failure (null for a plain cancellation).
    UnorderedMapWithMemoryTracking<String, std::exception_ptr> cancelled_queries TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

class ExchangeViaChunks : public IExchangeLookup
{
public:
    explicit ExchangeViaChunks(const String & query_id_)
        : query_id(query_id_)
    {
    }

    /// `advisory` is ignored: this sink already drops chunks when its reader detaches.
    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId & exchange_stream_id, bool /*advisory*/) override
    {
        auto file_name = exchange_stream_id.toString();
        auto exchange = InMemoryExchanges::instance()->getExchange(query_id, file_name);
        return std::make_shared<SinkFromInMemoryExchange>(input_header, exchange);
    }

    /// `advisory` is ignored: an in-memory source has no peer to lose and fails only when the
    /// query is cancelled.
    std::shared_ptr<ISource> createSource(
        SharedHeader output_header, const ExchangeStreamId & exchange_stream_id, bool output_is_serialized, bool /*advisory*/) override
    {
        if (output_is_serialized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "In-memory exchange {} has no deserializer, its source gives data chunks", exchange_stream_id.toString());

        auto file_name = exchange_stream_id.toString();
        auto exchange = InMemoryExchanges::instance()->getExchange(query_id, file_name);
        return std::make_shared<SourceFromInMemoryExchange>(output_header, exchange);
    }

private:
    class SinkFromInMemoryExchange final : public ISink
    {
    public:
        SinkFromInMemoryExchange(SharedHeader header_, InMemoryExchangePtr exchange_)
            : ISink(header_)
            , exchange(std::move(exchange_))
        {
        }

        String getName() const override { return "SinkFromInMemoryExchange"; }

        Status prepare() override
        {
            /// The reader detached, so appended chunks would be dropped. Close the input so the
            /// stop propagates to the upstream stages; without this they would keep computing
            /// data that nobody reads.
            if (exchange->isReaderDetached())
            {
                input.close();
                return Status::Finished;
            }
            return ISink::prepare();
        }

        void consume(Chunk chunk) override
        {
            /// Zero-row chunks are scheduling ticks from an upstream `SourceFromInMemoryExchange`;
            /// forwarding them would only grow the queue and wake the consumer for nothing.
            if (!chunk.hasRows() && chunk.getChunkInfos().empty())
                return;
            /// The consumer's header is deserialized without constants, so a constant column must not cross this exchange.
            convertToFullIfConst(chunk);
            exchange->appendChunk(std::move(chunk));
        }

        void onFinish() override
        {
            exchange->finish();
        }

    private:
        InMemoryExchangePtr exchange;
    };

    class SourceFromInMemoryExchange final : public ISource
    {
    public:
        SourceFromInMemoryExchange(SharedHeader header_, InMemoryExchangePtr exchange_)
            : ISource(header_)
            , exchange(std::move(exchange_))
        {
        }

        String getName() const override { return "SourceFromInMemoryExchange"; }

        Status prepare() override
        {
            /// The output port is closed, for example by a satisfied LIMIT downstream. Tell the
            /// exchange, so the producer's sink stops instead of queueing chunks that nobody
            /// reads. `onCancel` covers the cancellation path in the same way.
            /// Decide on the same read that finishes the source, the port can be closed concurrently.
            /// A source that ended by itself (`finished`, e.g. at the end of the data) needs no detach.
            const auto status = ISource::prepare();
            if (status == Status::Finished && !finished && !detach_notified)
            {
                detach_notified = true;
                exchange->detachReader();
            }
            return status;
        }

        std::optional<Chunk> tryGenerate() override
        {
            /// This source's own pipeline is being torn down: stop quietly, its output is
            /// discarded anyway.
            if (isCancelled())
                return std::nullopt;

            /// A processor must not block the pipeline thread, so wait with a timeout. On the
            /// timeout, push a chunk with no rows: a source that yields without producing output
            /// stays ready and gets rescheduled at once, monopolizing the thread, while pushed
            /// output makes the port full and lets other processors run.
            auto chunk = exchange->getChunk(waitTimeout());
            if (!chunk)
                return Chunk(getPort().getHeader().cloneEmptyColumns(), 0);
            if (chunk->empty())
                return std::nullopt;   /// End-of-data marker.
            return chunk;
        }

        /// Wake the timed wait so the source stops right away instead of on its next poll.
        void onCancel() noexcept override
        {
            exchange->detachReader();
        }

    private:
        /// A stream with no columns (case of `SELECT count()`) cannot fill the output port, so this
        /// source polls instead of yielding - keep its wait short so other processors run sooner.
        std::chrono::milliseconds waitTimeout() const
        {
            return getPort().getHeader().empty() ? std::chrono::milliseconds(1) : std::chrono::milliseconds(10);
        }

        InMemoryExchangePtr exchange;
        bool detach_notified = false;
    };

    const String query_id;
};


/// Drops zero-row chunks emitted as scheduling ticks by `SourceFromInMemoryExchange`; without
/// this filter they would escape the exchange path, e.g. to the client as empty `Data` packets.
class SkipZeroRowChunksTransform final : public ISimpleTransform
{
public:
    explicit SkipZeroRowChunksTransform(SharedHeader header_)
        : ISimpleTransform(header_, header_, /*skip_empty_chunks_=*/ true)
    {
    }

    String getName() const override { return "SkipZeroRowChunksTransform"; }

    void transform(Chunk & chunk) override
    {
        /// Keep zero-row chunks that carry chunk infos: they are not ticks.
        if (!chunk.hasRows() && chunk.getChunkInfos().empty())
            chunk = Chunk();
    }
};

std::shared_ptr<IProcessor> makeSkipZeroRowChunksTransform(SharedHeader header)
{
    return std::make_shared<SkipZeroRowChunksTransform>(std::move(header));
}


/// A wrapper that looks up exchanges by their kind and delegates to the corresponding exchange lookup: Persistent or Streaming
class AllKindsExchangeLookup : public IExchangeLookup
{
public:
    AllKindsExchangeLookup(
        const ExchangeDescriptions & exchanges_,
        ExchangeLookupPtr persistent_exchange_lookup_,
        ExchangeLookupPtr streaming_exchange_lookup_)
        : exchanges(exchanges_)
        , persistent_exchange_lookup(std::move(persistent_exchange_lookup_))
        , streaming_exchange_lookup(std::move(streaming_exchange_lookup_))
    {
    }

    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId & exchange_stream_id, bool advisory) override
    {
        return lookupFor(exchange_stream_id.exchange_id).createSink(std::move(input_header), exchange_stream_id, advisory);
    }

    std::shared_ptr<ISource>
    createSource(SharedHeader output_header, const ExchangeStreamId & exchange_stream_id, bool output_is_serialized, bool advisory) override
    {
        return lookupFor(exchange_stream_id.exchange_id)
            .createSource(std::move(output_header), exchange_stream_id, output_is_serialized, advisory);
    }

    std::shared_ptr<IProcessor> createSerializer(SharedHeader input_header, const String & exchange_id) override
    {
        return lookupFor(exchange_id).createSerializer(std::move(input_header), exchange_id);
    }

    std::shared_ptr<IProcessor> createDeserializer(SharedHeader output_header, const String & exchange_id) override
    {
        return lookupFor(exchange_id).createDeserializer(std::move(output_header), exchange_id);
    }

private:
    [[nodiscard]] IExchangeLookup & lookupFor(const String & exchange_id) const
    {
        auto it = exchanges.find(exchange_id);
        if (it == exchanges.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange '{}'", exchange_id);

        if (it->second.kind == ExchangeDescription::Kind::Persisted)
            return *persistent_exchange_lookup;
        if (it->second.kind == ExchangeDescription::Kind::Streaming)
            return *streaming_exchange_lookup;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange kind '{}'", static_cast<int>(it->second.kind));
    }

    const ExchangeDescriptions exchanges;
    ExchangeLookupPtr persistent_exchange_lookup;
    ExchangeLookupPtr streaming_exchange_lookup;
};

/// Cleans up temporary files produced by distributed query execution.
class TemporaryFilesInObjectStorageCleaner : public ICustomResourceHolder
{
public:
    TemporaryFilesInObjectStorageCleaner(ObjectStoragePtr object_storage_, const String & object_storage_path_,
        const Strings & temporary_files_)
        : object_storage(std::move(object_storage_))
        , object_storage_path(object_storage_path_)
        , temporary_files(temporary_files_.begin(), temporary_files_.end())
    {
    }

    ~TemporaryFilesInObjectStorageCleaner() override
    {
        /// TODO: add them to some background cleanup queue to avoid garbage in case of exceptions?
        try
        {
            cleanup();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void cleanup()
    {
        StoredObjects all_objects;
        for (const auto & file_name : temporary_files)
        {
            StoredObject object(object_storage_path + "/" + file_name, file_name);
            all_objects.emplace_back(std::move(object));
        }
        LOG_TRACE(getLogger("TemporaryFilesInObjectStorageCleaner"), "Removing temporary files at path {} : [{}]",
            object_storage_path, fmt::join(temporary_files, ", "));
        object_storage->removeObjectsIfExist(all_objects);
    }

private:
    ObjectStoragePtr object_storage;
    const String object_storage_path;
    const UnorderedSetWithMemoryTracking<String> temporary_files;
};

std::shared_ptr<ICustomResourceHolder> makeTemporaryFilesCleaner(ObjectStoragePtr object_storage_, const String & object_storage_path_, const Strings & temporary_files_)
{
    return std::make_shared<TemporaryFilesInObjectStorageCleaner>(object_storage_, object_storage_path_, temporary_files_);
}

/// Removes the query's in-memory exchanges from the registry when the query pipeline is destroyed.
/// Their lifetime spans the whole pipeline because the result reader drains final_result after the
/// executor (the driver) has finished, so removal cannot be tied to the executor's completion.
class InMemoryExchangesCleaner : public ICustomResourceHolder
{
public:
    explicit InMemoryExchangesCleaner(String query_id_) : query_id(std::move(query_id_)) {}

    ~InMemoryExchangesCleaner() override
    {
        try
        {
            InMemoryExchanges::instance()->removeQuery(query_id);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    const String query_id;
};

std::shared_ptr<ICustomResourceHolder> makeInMemoryExchangesCleaner(const String & query_id)
{
    return std::make_shared<InMemoryExchangesCleaner>(query_id);
}

TemporaryFileLookupPtr createTemporaryFilesLookup(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & input_temporary_files_, const Strings & output_temporary_files_)
{
    if (!object_storage_)
        return nullptr;

    return std::make_shared<TemporaryFilesInObjectStorage>(object_storage_, object_storage_path_, input_temporary_files_, output_temporary_files_);
}

/// `query_id` must be the node-independent distributed query id: it keys the in-memory and streaming
/// exchanges, so producers and consumers on different nodes (and the cleanup paths) must agree on it.
/// It must not embed any node-local object-storage subpath, which would differ between nodes.
ExchangeLookupPtr createExchangeLookup(
    const String & query_id,
    const ExchangeDescriptions & exchanges_,
    const ExchangeStreamSources & exchange_stream_sources,
    TemporaryFileLookupPtr temporary_files_,
    ContextPtr context,
    bool execute_locally,
    DistributedQueryCancellationPtr cancellation)
{
    if (execute_locally)
    {
        LOG_DEBUG(getLogger("createExchangeLookup"), "`distributed_plan_execute_locally` setting is enabled, using in-memory queues for all exchanges");
        return std::make_shared<ExchangeViaChunks>(query_id);
    }

    auto persisted_exchanges = std::make_shared<ExchangeViaTemporaryFiles>(temporary_files_);

    bool has_streaming_exchange = false;
    for (const auto & [exchange_id, exchange] : exchanges_)
        if (exchange.kind == ExchangeDescription::Kind::Streaming)
        {
            has_streaming_exchange = true;
            break;
        }

    /// Persisted exchanges only need the temporary-file lookup, so a plan where every exchange
    /// is Persisted runs without a streaming transport (and on any platform). The streaming
    /// port and lookup are required only when the plan actually contains a Streaming exchange.
    if (!has_streaming_exchange)
    {
        UNUSED(exchange_stream_sources);
        return std::make_shared<AllKindsExchangeLookup>(exchanges_, persisted_exchanges, /*streaming_exchange_lookup=*/nullptr);
    }

#if defined(OS_LINUX) || defined(OS_DARWIN)
    auto streaming_exchange_port = context->getConfigRef().getUInt("distributed_query.streaming_exchange_port", 0);
    if (streaming_exchange_port == 0)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Streaming exchange requires `distributed_query.streaming_exchange_port` to be configured; "
            "set the port, force `distributed_plan_force_exchange_kind = 'Persisted'`, or enable "
            "`distributed_plan_execute_locally` for in-process testing");
    if (streaming_exchange_port > 65535)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "`distributed_query.streaming_exchange_port` must be in range 1..65535, got {}", streaming_exchange_port);

    /// The listener starts only when a listen host is also configured, so streaming peers are
    /// unreachable without one. Reject here instead of connecting to a listener that never started.
    if (getMultipleValuesFromConfig(context->getConfigRef(), "distributed_query", "streaming_exchange_listen_host").empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Streaming exchange requires `distributed_query.streaming_exchange_listen_host` to be configured; "
            "set it, force `distributed_plan_force_exchange_kind = 'Persisted'`, or enable "
            "`distributed_plan_execute_locally` for in-process testing");

    /// A task from an older initiator (version 1) ships no per-stream ports; fall back to this
    /// node's configured exchange port to preserve the previous single-port behavior.
    ExchangeStreamSources sources_with_ports = exchange_stream_sources;
    for (auto & [stream, address] : sources_with_ports.stream_hosts)
        if (address.port == 0)
            address.port = static_cast<UInt16>(streaming_exchange_port);

    /// The auth token this node presents when opening an outbound exchange connection, taken from
    /// the query context (empty when connection authentication is not configured).
    auto streaming_exchanges = createStreamingExchangeLookup(
        query_id, ExchangeConnections::instance(), sources_with_ports, std::move(cancellation), /*auth_token=*/ String{},
        streamingExchangeCompressionCodec(context->getSettingsRef()));
    return std::make_shared<AllKindsExchangeLookup>(exchanges_, persisted_exchanges, streaming_exchanges);
#else
    UNUSED(exchange_stream_sources, context, cancellation);
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "Streaming exchanges are only supported on Linux and macOS; "
        "use `distributed_plan_force_exchange_kind = 'Persisted'`");
#endif
}


static String serializeQueryPlan(const QueryPlan & query_plan, const ContextPtr & context)
{
    /// A shipped set must be complete, so the overflow mode is always throw;
    /// `transfer_overflow_mode = 'break'` does not apply to it.
    const auto & settings = context->getSettingsRef();
    SizeLimits sets_transfer_limits(
        settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], OverflowMode::THROW);

    WriteBufferFromOwnString out;
    query_plan.serializeForDistributedTask(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, sets_transfer_limits);
    return out.str();
}

static QueryPlan deserializeQueryPlan(const String & serialized_query_plan, ContextPtr context)
{
    ReadBufferFromString in(serialized_query_plan);
    /// Trusted server-to-server plan fragment: decode types without the input complexity limit.
    auto plan_and_sets = QueryPlan::deserialize(in, context, 0);
    return QueryPlan::makeSets(std::move(plan_and_sets), context);
}

/// Runs each runtime filter receive branch of a task in its own thread, beside the data pipeline.
/// A branch starts reading at once, without waiting for the data pipeline. `finish` cancels the
/// branches that still wait. A branch error is logged and the filter is skipped, so rows pass
/// unfiltered.
class RuntimeFilterReceiveBranches
{
public:
    explicit RuntimeFilterReceiveBranches(LoggerPtr logger_) : logger(std::move(logger_)) {}

    void start(QueryPipeline pipeline, const String & filter_name)
    {
        auto branch = std::make_shared<Branch>(std::move(pipeline));
        branches.push_back(branch);
        try
        {
            branch->thread = ThreadFromGlobalPool(
                [branch, filter_name, thread_group = CurrentThread::getGroup(), log = logger]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::DISTRIBUTED_QUERY_TASK);
                    try
                    {
                        fiu_do_on(FailPoints::distributed_plan_runtime_filter_receive_branch_fails_before_connect,
                        {
                            throw Exception(ErrorCodes::EXCHANGE_PEER_DISCONNECTED, "Injected receive branch failure before it connects");
                        });
                        CompletedPipelineExecutor executor(branch->pipeline);
                        executor.setCancelCallback([branch] { return branch->cancelled.load(); }, /*interactive_timeout_ms_*/ 50);
                        executor.execute();
                    }
                    catch (...)
                    {
                        ProfileEvents::increment(ProfileEvents::RuntimeFilterReceiveBranchFailures);
                        tryLogCurrentException(
                            log,
                            fmt::format(
                                "Receive branch for runtime filter '{}' failed; the filter is skipped and rows pass unfiltered",
                                filter_name));
                    }
                });
        }
        catch (...)
        {
            /// The thread did not start: skip the filter.
            branches.pop_back();
            ProfileEvents::increment(ProfileEvents::RuntimeFilterReceiveBranchFailures);
            tryLogCurrentException(
                logger,
                fmt::format(
                    "Cannot start the receive branch for runtime filter '{}'; the filter is skipped and rows pass unfiltered",
                    filter_name));
        }
    }

    /// Cancel the branches that are still waiting and join their threads. Idempotent.
    void finish() noexcept
    {
        for (const auto & branch : branches)
            branch->cancelled = true;
        for (const auto & branch : branches)
            if (branch->thread.joinable())
                branch->thread.join();
        branches.clear();
    }

    ~RuntimeFilterReceiveBranches() { finish(); }

private:
    struct Branch
    {
        explicit Branch(QueryPipeline pipeline_) : pipeline(std::move(pipeline_)) {}

        QueryPipeline pipeline;
        std::atomic<bool> cancelled{false};
        ThreadFromGlobalPool thread;
    };

    LoggerPtr logger;
    VectorWithMemoryTracking<std::shared_ptr<Branch>> branches;
};

/// The identity of the task on its `DistributedPlanTask::execute` span. No-op when tracing is off.
static void addTaskSpanAttributes(
    OpenTelemetry::Span & span,
    const DistributedQueryTaskDescription & task_description,
    const ContextPtr & context,
    UInt64 query_plan_hash,
    bool execute_locally,
    const Strings & input_exchange_streams,
    const Strings & output_exchange_streams)
{
    if (!span.isTraceEnabled())
        return;

    span.addAttribute("clickhouse.distributed.task_id", task_description.task.task_id);
    /// From the task's context rather than the description: a worker copies the description's
    /// initial query id into its `ClientInfo`, while an in-process task inherits the initiator's.
    span.addAttribute("clickhouse.initial_query_id", context->getClientInfo().initial_query_id);
    span.addAttribute("clickhouse.distributed.plan_hash", query_plan_hash);
    span.addAttribute("clickhouse.distributed.serialization_version", task_description.serialization_version);
    span.addAttribute("clickhouse.distributed.execute_locally", static_cast<UInt64>(execute_locally));
    span.addAttribute("clickhouse.exchange.inputs", fmt::format("[{}]", fmt::join(input_exchange_streams, ", ")));
    span.addAttribute("clickhouse.exchange.outputs", fmt::format("[{}]", fmt::join(output_exchange_streams, ", ")));
}

void doExecuteTask(const DistributedQueryTaskDescription & task_description, ObjectStoragePtr object_storage,
    const String & object_storage_path, const String & distributed_query_id, ContextMutablePtr context,
    bool execute_locally, std::function<bool()> is_cancelled, ProgressCallback progress_callback,
    std::function<void()> on_failure)
{
    Stopwatch execute_task_watch;
    const auto & task = task_description.task;

    /// The task's span is analogous to the `query` span of a regular query. Its parent is the initiator's trace.
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span
        = std::make_shared<OpenTelemetry::SpanHolder>("DistributedPlanTask::execute", OpenTelemetry::SpanKind::SERVER);

    auto logger = Poco::Logger::getShared("executeDistributedQuery");

    /// Disable the query condition cache: its per-worker state could make workers read inconsistent
    /// data for the same fragment.
    context->setSetting("use_query_condition_cache", false);

    Strings input_exchange_streams;
    for (const auto & stream_id : task.input_exchange_streams)
        input_exchange_streams.push_back(stream_id.toString());

    Strings output_exchange_streams;
    for (const auto & stream_id : task.output_exchange_streams)
        output_exchange_streams.push_back(stream_id.toString());

    chassert(task_description.serialized_query_plan);
    /// Identifies the plan fragment in the span and in the query log alike.
    const UInt64 query_plan_hash = sipHash64(*task_description.serialized_query_plan);

    addTaskSpanAttributes(*query_span, task_description, context, query_plan_hash, execute_locally, input_exchange_streams, output_exchange_streams);

    LOG_TRACE(logger, "Task '{}' input exchange streams: [{}], output exchange streams: [{}]",
        task.task_id, fmt::join(input_exchange_streams, ", "), fmt::join(output_exchange_streams, ", "));

#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// Release this task's pending streaming exchange connections on the worker when it ends. A
    /// consumer that never connects (e.g. its query was cancelled) would otherwise leave them behind.
    /// Only this task's output streams are dropped, so sibling tasks of the same query are unaffected.
    SCOPE_EXIT_SAFE(ExchangeConnections::instance()->removePendingStreams(distributed_query_id, output_exchange_streams));
#endif

    auto temporary_files = createTemporaryFilesLookup(
        object_storage, object_storage_path, input_exchange_streams, output_exchange_streams);

    auto pipeline_settings = BuildQueryPipelineSettings(context);
    pipeline_settings.temporary_file_lookup = temporary_files;
    pipeline_settings.parameter_lookup = std::make_shared<TaskParameters>(task.parameters);
    pipeline_settings.exchange_lookup = createExchangeLookup(
        distributed_query_id,
        task_description.exchanges,
        task_description.exchange_stream_sources,
        temporary_files,
        context,
        execute_locally,
        /*cancellation=*/ nullptr);

    auto optimization_settings = QueryPlanOptimizationSettings(context);

    /// Disable stats-driven plan-shape rewrites on the worker side: per-worker
    /// stats can diverge and produce incompatible plans across workers (e.g. one
    /// swaps the join sides while the others don't), breaking exchange partitioning.
    optimization_settings.join_swap_table = std::make_optional(false);
    optimization_settings.query_plan_optimize_join_order_limit = 0;
    optimization_settings.query_plan_optimize_join_order_randomize = 0;
    optimization_settings.convert_join_to_in = false;
    optimization_settings.convert_outer_join_to_inner_join = false;
    optimization_settings.convert_any_join_to_semi_or_anti_join = false;
    optimization_settings.merge_filter_into_join_condition = false;
    optimization_settings.top_k_through_join = false;

    /// The fragment's read is bucketed; re-introducing the implicit count projection would count the
    /// whole part per bucket. Keep it off so counts read the bucket's mark ranges.
    optimization_settings.optimize_use_implicit_projections = false;

    QueryPipeline pipeline;

    {
        QueryPlan query_plan = deserializeQueryPlan(*task_description.serialized_query_plan, context);

        /// A deserialized plan carries neither the thread limit nor the concurrency-control flag,
        /// so both come from the query's settings.
        query_plan.setMaxThreads(pipeline_settings.max_threads);
        query_plan.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);

        auto builder = query_plan.buildQueryPipeline(
                optimization_settings,
                pipeline_settings);

        pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    }

    RuntimeFilterReceiveBranches receive_branches(logger);
    for (const auto & descriptor : task.runtime_filter_descriptors)
    {
        const auto partials_header = runtimeFilterPartialsHeader();
        auto partials = receiveExchangeStreams(
            partials_header,
            descriptor.stream.exchange_id,
            {descriptor.stream},
            pipeline_settings,
            /*spread_over_max_threads*/ false,
            /*advisory*/ true);
        auto merge = std::make_shared<MergeRuntimeFiltersTransform>(
            partials_header,
            /*num_inputs=*/1,
            MergeRuntimeFiltersTransform::Mode::RegisterUnion,
            descriptor.filter_name,
            descriptor.filter_key,
            descriptor.key_column_type,
            descriptor.geometry,
            context->getRuntimeFilterLookup());
        if (context->getSettingsRef()[Setting::enable_join_runtime_filters_index_analysis])
            merge->enableIndexAnalysis();
        partials.addTransform(std::move(merge));
        auto branch = QueryPipelineBuilder::getPipeline(std::move(partials));
        branch.complete(std::make_shared<EmptySink>(partials_header));
        receive_branches.start(std::move(branch), descriptor.filter_name);
    }

    /// No AST: this fragment is built from a serialized query plan, not parsed. The query-log
    /// helpers below treat a null AST as QueryKind::Select, which is correct here.
    const ASTPtr no_ast;

    auto query_log_elem = logQueryStart(
        std::chrono::system_clock::now(),
        context,
        /*query_for_logging*/ task.task_id,
        query_plan_hash,
        no_ast, pipeline,
        /*interpreter*/ nullptr,
        /*internal*/ false,
        /*log_as_internal*/ false,
        /*database*/ "",
        /*table*/ "",
        /*async_insert*/ false);

    try
    {
        LOG_TEST(logger, "Executing task '{}', pipeline:\n{}",
            task.task_id,
            [&pipeline]() -> String
            {
                WriteBufferFromOwnString out;
                printPipeline(pipeline.getProcessors(), out);
                return out.str();
            }());

        if (!pipeline.completed())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Task pipeline must be completed");

        pipeline.setProcessListElement(context->getProcessListElement());

        pipeline.setProgressCallback(progress_callback ? progress_callback : context->getProgressCallback());

        {
            CompletedPipelineExecutor executor(pipeline);
            if (is_cancelled)
                executor.setCancelCallback(is_cancelled, 100);
            executor.execute();
        }

        /// The data work is done; a filter that has not arrived by now has nothing left to serve.
        receive_branches.finish();

        logQueryFinish(query_log_elem, context, no_ast, std::move(pipeline), false,
            query_span, QueryResultCacheUsage::None, false, /*log_as_internal*/ false);
    }
    catch (...)
    {
        logQueryException(query_log_elem, context, execute_task_watch, no_ast, query_span, false, /*log_as_internal*/ false, true);
        if (on_failure)
        {
            /// The task's own exception is rethrown below, so an error of the callback is only logged.
            try
            {
                on_failure();
            }
            catch (...)
            {
                tryLogCurrentException(logger, "Cannot report the task failure early");
            }
        }
        throw;
    }
}

/// Storage path (and thus the exchange query key) for a query's temporary files. Kept separate
/// from object-storage creation so it can be recomputed cheaply (e.g. for cleanup).
static String getTemporaryFilesPath(const String & unique_temp_file_path, ContextPtr context)
{
    const auto & config = context->getConfigRef();
    String config_prefix = "distributed_query.temporary_files_storage";
    if (config.has(config_prefix))
        return config.getString(config_prefix + ".endpoint_subpath") + unique_temp_file_path;
    return unique_temp_file_path;
}

std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context)
{
    const auto & config = context->getConfigRef();
    String config_prefix = "distributed_query.temporary_files_storage";
    String object_storage_path = getTemporaryFilesPath(unique_temp_file_path, context);
    if (config.has(config_prefix))
    {
        ObjectStoragePtr object_storage = ObjectStorageFactory::instance().create("distributed_query_temp_files", config, config_prefix, context, /*run_access_check=*/true, /*run_local_paths_check=*/false, RemoteHostFilterCheck::None);
        return {object_storage, object_storage_path};
    }
    return {nullptr, object_storage_path};
}

/// `initial_query_id` is shared by every fragment of a plan, and is empty when the client itself sent a secondary query.
/// The settings a task runs with on a worker. `max_threads` describes the initiator's machine when it is `auto` or
/// comes from the user's settings profile, so then the worker uses its own; only a value the user set over the
/// profile caps the worker.
static SettingsChanges getTaskSettingsChanges(const ContextPtr & context)
{
    SettingsChanges changes = context->getSettingsRef().changes();
    const auto & max_threads = context->getSettingsRef()[Setting::max_threads];

    bool user_capped_max_threads = !max_threads.is_auto;
    if (user_capped_max_threads)
    {
        for (const auto & change : context->getAccess()->getDefaultSettings())
            if (change.name == "max_threads")
                user_capped_max_threads = SettingFieldMaxThreads(change.value).value != max_threads.value;
    }

    if (!user_capped_max_threads)
        std::erase_if(changes, [](const SettingChange & change) { return change.name == "max_threads"; });
    return changes;
}

static String logicalQueryId(const ClientInfo & client_info)
{
    return client_info.initial_query_id.empty() ? client_info.current_query_id : client_info.initial_query_id;
}

static void executeTask(const UUID & unique_query_id, const DistributedQueryTaskDescription & task, ContextPtr context, DistributedQueryCancellationPtr cancellation)
{
    auto [object_storage, object_storage_path] = getObjectStorageForTemporaryFiles(toString(unique_query_id), context);

    /// Run each task as an independent query fragment with its own query context and thread group,
    /// matching the worker path. Attaching the task context to this thread (instead of sharing the
    /// initiator's) gives the task its own per-query state, such as the runtime filter lookup.
    auto task_context = Context::createCopy(context);
    task_context->makeQueryContext();

    {
        ClientInfo client_info = task_context->getClientInfo();
        client_info.initial_query_id = logicalQueryId(client_info);
        client_info.current_query_id = makeUniqueTaskId(unique_query_id, task.task.task_id);
        client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        client_info.distributed_depth += 1;
        task_context->setClientInfo(client_info);
    }

    auto query_scope = QueryScope::create(task_context);
    setThreadName(ThreadName::DISTRIBUTED_QUERY_TASK);

    /// A query's log row reports the profile counters of its process-list entry's thread group.
    auto process_list_entry = task_context->getProcessList().insert(
        task.task.task_id, sipHash64(*task.serialized_query_plan), /*ast=*/ nullptr, task_context,
        clock_gettime_ns(CLOCK_MONOTONIC), /*is_internal=*/ true);
    task_context->setProcessListElement(process_list_entry->getQueryStatus());

    /// Only DistributedQueryPlanExecutorLocal reaches here, so the task always runs in-process.
    doExecuteTask(task, object_storage, object_storage_path, toString(unique_query_id), std::move(task_context),
        /*execute_locally=*/true, [cancellation]() -> bool { return cancellation->isCancelled(); });
}

/// Runs tasks in local threads. Useful for testing and debugging.
class DistributedQueryPlanExecutorLocal final : public DistributedQueryPlanExecutor
{
public:
    DistributedQueryPlanExecutorLocal(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_, DistributedQueryCancellationPtr cancellation_, StageWakeupPtr stage_wakeup_)
        : DistributedQueryPlanExecutor(unique_query_id_, distributed_query_plan_, makeContextForLocalExecution(context_), std::move(cancellation_), std::move(stage_wakeup_))
    {
    }

    ~DistributedQueryPlanExecutorLocal() override
    {
        /// Guarantee no task thread outlives the executor even on a teardown path that did not reach
        /// the driver's cleanup() (e.g. an exception during the run). The threads are always joined,
        /// never detached, so the std::thread members below are never destroyed while joinable.
        cleanup();
    }

    void cleanup() override
    {
        /// Cancel the query's in-memory exchanges before joining the task threads, or a task stuck in
        /// InMemoryExchange::getChunk never returns. The exchanges are not removed here: the result
        /// reader still drains final_result after the driver finishes; removal happens when the query
        /// pipeline is destroyed (see makeInMemoryExchangesCleaner).
        cancelDistributedQueryInMemoryExchanges(unique_query_id, cancellation->getFailure());

        joinAllThreads();
        stage_tasks.clear();
    }

protected:
    static ContextPtr makeContextForLocalExecution(ContextPtr ctx)
    {
        auto new_context = Context::createCopy(ctx);
        /// We will execute tasks with local plan fragments. They should not be converted into distributed plan themselves.
        new_context->setSetting("make_distributed_plan", false);
        new_context->setSetting("enable_cascades_optimizer", false);
        return new_context;
    }

    std::future<void> startTask(const DistributedQueryTaskDescription & task_description, VectorWithMemoryTracking<std::thread> & threads)
    {
        std::promise<void> task_promise;
        std::future<void> future = task_promise.get_future();

        /// A raw thread inherits no tracing context (unlike a `ThreadPool` job), so hand the initiator's context over explicitly.
        threads.emplace_back([promise = std::move(task_promise), query_id = unique_query_id, task_description, ctx = context, cancellation = this->cancellation, stage_wakeup = this->stage_wakeup,
            parent_trace_context = OpenTelemetry::CurrentContext()]() mutable
        {
            ThreadStatus thread_status;
            /// A guard, not a `TracingContextHolder`: the holder would log a span of its own for this
            /// thread, while the task's `DistributedPlanTask::execute` span is meant to attach to the
            /// initiator's context directly, the same as a task dispatched to a worker does.
            OpenTelemetry::TracingContextGuard thread_trace_context(parent_trace_context);
            /// The task attaches its own query context and thread group inside executeTask (matching
            /// the worker path), so this thread is intentionally not attached to the initiator group.

            try
            {
                executeTask(query_id, task_description, ctx, cancellation);
                promise.set_value();
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }

            /// The task future this promise belongs to is what `waitForStage` looks at, so tell the
            /// waiter that its answer may have changed.
            notifyStageWakeup(stage_wakeup);
        });

        return future;
    }

    void startStage(const String & stage_name, const DistributedQueryStage & stage) override
    {
        DistributedQueryTaskDescription task_description;
        task_description.serialized_query_plan = std::make_shared<const String>(serializeQueryPlan(stage.query_plan_fragment, context));
        task_description.exchanges = distributed_query_plan.exchange_descriptions; /// TODO: add only exchanges for this stage

        /// Owned by the members before the first thread starts, so `cleanup` joins it if anything below throws.
        auto & started_tasks = stage_tasks[stage_name];
        auto & started_threads = stage_threads[stage_name];
        started_tasks.reserve(stage.tasks.size());
        started_threads.reserve(stage.tasks.size());

        for (const auto & task : stage.tasks)
        {
            task_description.task = task;
            started_tasks.emplace_back(startTask(task_description, started_threads).share());
            /// Stands for the exception the per-task copies of the plan once raised here (05407).
            fiu_do_on(FailPoints::distributed_plan_local_start_stage_fails_after_first_task,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure while starting the local tasks of stage {}", stage_name);
            });
        }
    }

    bool waitForStage(const String & stage_name, std::optional<UInt64> timeout_ms) override
    {
        auto & started_tasks = stage_tasks[stage_name];

        for (auto & task : started_tasks)
        {
            if (timeout_ms.has_value())
            {
                if (task.wait_for(std::chrono::milliseconds(timeout_ms.value())) != std::future_status::ready)
                {
                    /// While this stage is still running, surface a failure from any other stage so a
                    /// consumer blocked on an exchange does not wait forever on a dead producer.
                    rethrowFailedTasks();
                    return false;
                }
            }
            else
            {
                task.wait();
            }
        }

        /// All task futures of this stage are ready, so its threads are finishing right after fulfilling
        /// their promises. Join them now to release their resources promptly instead of holding the
        /// thread handles until cleanup().
        joinStageThreads(stage_name);

        auto tasks = std::move(started_tasks);
        started_tasks.clear();

        /// Throw exception if any task failed
        for (auto & task : tasks)
            task.get();

        return true;
    }

    /// Merge tasks end soon after their inputs here, so waiting for them is cheap and reports
    /// their errors.
    [[nodiscard]] bool waitsForFilterOnlyStages() const override { return true; }

private:
    /// Rethrow the exception of any already-finished failed task across all stages, without blocking.
    void rethrowFailedTasks()
    {
        for (auto & [stage_name, tasks] : stage_tasks)
            for (auto & task : tasks)
                if (task.valid() && task.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
                    task.get();
    }

    /// Join and drop the threads of a single stage. Call only when the stage's task futures are ready.
    void joinStageThreads(const String & stage_name)
    {
        auto it = stage_threads.find(stage_name);
        if (it == stage_threads.end())
            return;
        for (auto & thread : it->second)
            if (thread.joinable())
                thread.join();
        stage_threads.erase(it);
    }

    /// Join every task thread not already joined by waitForStage (e.g. for a cancelled or failed stage).
    void joinAllThreads()
    {
        for (auto & [_, threads] : stage_threads)
            for (auto & thread : threads)
                if (thread.joinable())
                    thread.join();
        stage_threads.clear();
    }

    UnorderedMapWithMemoryTracking<String, VectorWithMemoryTracking<std::shared_future<void>>> stage_tasks;
    UnorderedMapWithMemoryTracking<String, VectorWithMemoryTracking<std::thread>> stage_threads;
};


/// Resolve a worker's endpoint on the initiator from the cluster config (with server-level
/// fallbacks). The single place a future port source would plug in.
static WorkerAddress resolveWorkerAddress(const String & host, UInt16 cluster_streaming_exchange_port, ContextPtr context)
{
    WorkerAddress address;
    address.host = host;

    auto server_level_exchange_port = context->getConfigRef().getUInt("distributed_query.streaming_exchange_port", 0);
    /// Reject out-of-range values instead of silently narrowing them to a different or unset port.
    if (server_level_exchange_port > 65535)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "`distributed_query.streaming_exchange_port` must be in range 0..65535, got {}", server_level_exchange_port);
    address.streaming_exchange_port = cluster_streaming_exchange_port != 0
        ? cluster_streaming_exchange_port
        : static_cast<UInt16>(server_level_exchange_port);
    if (address.streaming_exchange_port == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Stateless worker '{}' has no streaming-exchange port, which the control connection of its tasks dials: set its "
            "`streaming_exchange_port` in the cluster or `distributed_query.streaming_exchange_port`", host);

    return address;
}

TaskToHostMap::TaskToHostMap(const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_)
{
    /// Only constructed for a plan that runs on workers; a local plan gets a null map instead.
    fillWorkerAddresses(context_);

    /// Cap the host list to match the node count the optimizer planned for.
    size_t max_nodes = getCascadesClusterNodeCountParam(context_);
    if (max_nodes > 0 && max_nodes < worker_addresses.size())
        worker_addresses.resize(max_nodes);

    assignHostsForTasks(distributed_query_plan_);
}

size_t getCascadesPlanningNodeCount(ContextPtr context)
{
    /// Read the settings from the query context, which the allocation also uses, so a subquery's own
    /// `SETTINGS` cannot plan a different cluster or worker count than execution gets.
    const ContextPtr query_context = context->hasQueryContext() ? context->getQueryContext() : context;
    const auto & settings = query_context->getSettingsRef();
    const size_t requested_workers = settings[Setting::distributed_plan_workers_num];

    /// Local execution runs in-process, not bound to a cluster; use the requested count when set.
    if (settings[Setting::distributed_plan_execute_locally] && requested_workers > 0)
        return requested_workers;

    /// With the client disabled the default cluster plans no distributed nodes, but an explicitly named
    /// cluster cannot be served, so reject it instead of silently running locally.
    const String worker_cluster = settings[Setting::distributed_plan_worker_cluster];
    if (!context->getConfigRef().getBool("stateless_worker_client.enabled", false))
    {
        if (!worker_cluster.empty())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Stateless worker client is not enabled; cannot use worker cluster '{}'", worker_cluster);
        return 0;
    }
    /// Size the plan from the provider the query executes on; an unusable selection throws here.
    return query_context->getStatelessWorkersProvider()->getPlanningNodeCount(requested_workers);
}

TaskToHostMap::~TaskToHostMap() = default;

String TaskToHostMap::getAuthToken() const
{
    return worker_allocation ? worker_allocation->getAuthToken() : String{};
}

String makeUniqueTaskId(const UUID & unique_query_id, const String & task_id)
{
    return makeUniqueTaskId(toString(unique_query_id), task_id);
}

String makeUniqueTaskId(const String & unique_query_id, const String & task_id)
{
    return unique_query_id + "::" + task_id;
}

void TaskToHostMap::fillWorkerAddresses(ContextPtr context)
{
    /// No `enabled` check here: a query that already resolved its provider drains on it even if the
    /// client is disabled meanwhile, while resolving a new provider is refused by the registry.
    /// One allocation per query, shared by its distributed plans.
    worker_allocation = context->getOrCreateStatelessWorkerAllocation();
    for (const auto & addr : worker_allocation->getAddresses())
        worker_addresses.push_back(
            resolveWorkerAddress(addr.host, addr.streaming_exchange_port, context));
    if (worker_addresses.empty())
        throw Exception(ErrorCodes::SW_SERVER_NO_WORKERS_AVAILABLE, "No stateless workers available");
}

void TaskToHostMap::assignHostsForTasks(const DistributedQueryPlan & distributed_query_plan)
{
    WorkerIndex current_host = 0;
    for (const auto & [stage_id, stage] : distributed_query_plan.stages)
    {
        for (const auto & task : stage.tasks)
        {
            const auto & assigned_worker = worker_addresses[current_host];
            task_worker_indices[task.task_id] = current_host;
            // TODO it would be better to split this logic of assigned worker to host assignment so we have dedicated function with logic
            current_host = (current_host + 1) % worker_addresses.size();
            for (const auto & output_stream : task.output_exchange_streams)
                exchange_stream_source_hosts[output_stream.toString()] = {assigned_worker.host, assigned_worker.streaming_exchange_port};
        }
    }
}


#if defined(OS_LINUX) || defined(OS_DARWIN)

/// Sends tasks to remote nodes.
class DistributedQueryPlanExecutorRemote final : public DistributedQueryPlanExecutor
{
public:
    DistributedQueryPlanExecutorRemote(
        const UUID & unique_query_id_,
        const DistributedQueryPlan & distributed_query_plan_,
        TaskToHostMapPtr task_to_host_map_,
        ContextPtr context_,
        DistributedQueryCancellationPtr cancellation_,
        StageWakeupPtr stage_wakeup_)
        : DistributedQueryPlanExecutor(unique_query_id_, distributed_query_plan_, std::move(context_), std::move(cancellation_), std::move(stage_wakeup_))
        , task_to_host_map(std::move(task_to_host_map_))
        , running_tasks(task_to_host_map, unique_query_id, context, cancellation, stage_wakeup, logger)
        , batch_dispatch(context->getSettingsRef()[Setting::distributed_plan_task_dispatch_batching])
        , stages_per_flush(stagesPerFlush(context))
    {
        /// A null map belongs to an in-process plan, which createDistributedQueryExecutor routes to
        /// the local executor instead.
        chassert(task_to_host_map);
        Strings worker_hosts;
        for (const auto & worker : task_to_host_map->getWorkerAddresses())
            worker_hosts.push_back(worker.host);
        LOG_DEBUG(logger, "Hosts for running distributed query: [{}]", fmt::join(worker_hosts, ", "));
        UnorderedSetWithMemoryTracking<String> distinct_hosts;
        for (const auto & [task_id, worker_index] : task_to_host_map->getTaskWorkerIndices())
            distinct_hosts.insert(task_to_host_map->getWorkerAddresses()[worker_index].host);
        ProfileEvents::increment(ProfileEvents::DistributedPlanHostsUsed, distinct_hosts.size());
    }

    void cleanup() override
    {
        running_tasks.cancel();
        /// Drop any still-pending exchange connection slots that belong to this query
        /// (the peer was cancelled or never arrived). Without this they would leak
        /// FutureConnection/eventfd entries in the ExchangeConnections singleton for
        /// the lifetime of the server.
        ExchangeConnections::instance()->cleanupQuery(toString(unique_query_id));
    }

protected:
    struct RunningTaskInfo
    {
        WorkerIndex worker = 0;
        String task_id;
    };

    static String describeWorker(const WorkerAddress & address)
    {
        return fmt::format("{}:{}", address.host, address.streaming_exchange_port);
    }

    /// Tracks the tasks of the query on its workers over one control connection per worker, which the worker's first batch
    /// opens, and collects their progress for the query thread. The replies are handled on the `WorkerControlLoop` thread.
    class TaskTracker
    {
    public:
        TaskTracker(TaskToHostMapPtr task_to_host_map_, const UUID & unique_query_id_, ContextPtr context_,
            DistributedQueryCancellationPtr cancellation_, StageWakeupPtr stage_wakeup_, LoggerPtr logger_)
            : task_to_host_map(std::move(task_to_host_map_))
            , unique_query_id(toString(unique_query_id_))
            , context(std::move(context_))
            , query_status(context->getProcessListElement())
            , cancellation(std::move(cancellation_))
            , stage_wakeup(std::move(stage_wakeup_))
            , logger(std::move(logger_))
        {
            const auto & addresses = task_to_host_map->getWorkerAddresses();
            std::lock_guard g(lock);
            worker_states.resize(addresses.size());
            for (WorkerIndex worker = 0; worker < addresses.size(); ++worker)
                channels.push_back(std::make_unique<WorkerChannel>(*this, worker, describeWorker(addresses[worker])));
        }

        ~TaskTracker()
        {
            /// Before the members that the callbacks use are destroyed.
            for (const auto & channel : channels)
                if (channel->connection)
                    channel->connection->detach();
        }

        /// Whether forwarded worker logs have somewhere to go: the initiator's `send_logs_level` queue.
        bool receivesWorkerLogs() const { return initiator_logs_queue != nullptr; }

        /// Tracks a task whose start is still to be sent: `waitForStage` counts it and `cancel` cancels it, nothing polls it.
        /// Never runs after `cancel`: `start` and `cleanup` both hold the source's `executor_mutex`.
        void registerTask(const String & stage_name, RunningTaskInfo task_info)
        {
            auto task_name = task_info.task_id;
            std::lock_guard g(lock);
            stage_tasks[stage_name][task_name] = std::move(task_info);

            /// Create stage info if this stage was not known before
            if (!all_stages.contains(stage_name))
                all_stages[stage_name] = std::make_shared<StageInfo>(stage_name);
            all_stages[stage_name]->started_tasks++;
        }

        /// Sends `batch` on the control connection of `worker`, opened by its first batch; the reply makes its tasks polled.
        void startTasks(WorkerIndex worker, DistributedQueryTaskBatch batch)
        {
            WorkerChannel & channel = *channels.at(worker);
            if (!channel.connection)
                channel.connection = WorkerControlLoop::instance().open(makeConnectionParams(worker), channel);

            StartRequest request;
            for (const auto & batch_stage : batch.stages)
                for (const auto & batch_task : batch_stage.tasks)
                    request.tasks.push_back({batch_stage.name, {worker, makeUniqueTaskId(unique_query_id, batch_task.task.task_id)}});
            LOG_DEBUG(logger, "Sending a batch of {} tasks of {} stages to {}", request.tasks.size(), batch.stages.size(), channel.description);

            StatelessWorkerControlProtocol::StartTasks message{
                .request_id = 0,
                .temp_path = unique_query_id,
                .collectors = {.logs = receivesWorkerLogs()},
                .trace_context = {},
                .batch = std::move(batch),
            };
            if (OpenTelemetry::CurrentContext().isTraceEnabled())
            {
                /// Finished at the reply, so the worker's spans hang under it.
                request.span = std::make_unique<OpenTelemetry::ManualSpan>("WorkerControlConnection::startTasks", OpenTelemetry::SpanKind::CLIENT);
                request.span->addAttribute("clickhouse.distributed.query_id", unique_query_id);
                request.span->addAttribute("clickhouse.distributed.task_count", request.tasks.size());
                request.span->addAttribute("clickhouse.distributed.stage_count", message.batch.stages.size());
                request.span->addAttribute("clickhouse.initial_query_id", message.batch.initial_query_id);
                request.span->addAttribute("clickhouse.target_host", channel.description);
                message.trace_context = OpenTelemetry::CurrentContext();
                message.trace_context.span_id = request.span->getSpanId();
            }

            /// Queued before the send, as the reply can come before `startTasks` returns.
            {
                std::lock_guard g(lock);
                auto & state = worker_states[worker];
                if (state.ended)
                {
                    if (state.lost)
                        std::rethrow_exception(state.lost);
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "A batch for stateless worker {} after its control connection ended", channel.description);
                }
                state.start_requests.push_back(std::move(request));
            }
            try
            {
                channel.connection->startTasks(std::move(message));
            }
            catch (...)
            {
                /// Not sent, so no reply took it.
                std::lock_guard g(lock);
                worker_states[worker].start_requests.pop_back();
                throw;
            }
        }

        /// Wait for all tasks of the stage to finish
        bool waitForStage(const String & stage_name, std::optional<UInt64> timeout_ms)
        {
            LOG_TRACE(logger, "Waiting for stage {} to finish", stage_name);
            applyProgress();

            std::shared_future<void> finished;
            {
                std::lock_guard g(lock);

                auto & stage = all_stages.at(stage_name);

                /// Is already finished?
                if (stage->started_tasks == stage->finished_tasks)
                    return true;

                /// Create a future that will be signaled by the last finishing task of this stage
                if (!stage_results.contains(stage_name))
                    stage_results[stage_name] = stage->promise.get_future();

                finished = stage_results.at(stage_name);
            }

            bool stage_finished = false;
            if (timeout_ms.has_value())
            {
                stage_finished = (finished.wait_for(std::chrono::milliseconds(timeout_ms.value())) == std::future_status::ready);
            }
            else
            {
                /// Poll with timeout instead of blocking indefinitely, so that
                /// checkCancelled can detect worker failures stored in first_exception.
                while (finished.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready)
                {
                    applyProgress();
                    checkCancelled();
                }
                stage_finished = true;
            }
            applyProgress();
            checkCancelled();
            return stage_finished;
        }

        /// Cancels the tasks still tracked when the query ends, once: `cleanup` calls this once. Each worker cancels its
        /// tasks, is polled until they end, forgets those that did and closes its connection, all on the loop; this thread waits
        /// for the connections to close for `TEARDOWN_TIMEOUT` and aborts the rest, whose workers reclaim the tasks.
        void cancel()
        {
            /// The thread of a `KILL QUERY` can run this, and the requests and logs still belong to this query.
            ThreadGroupSwitcher switcher(thread_group, ThreadName::DISTRIBUTED_QUERY_TASK, /*allow_existing_group=*/ true);
            try
            {
                applyProgress();
            }
            catch (...)
            {
                recordFailure(std::current_exception());
            }

            {
                std::lock_guard g(lock);
                if (cancel_started)
                    return;
                cancel_started = true;
                for (auto & [stage_name, started_tasks] : stage_tasks)
                {
                    for (auto & [task_name, task_info] : started_tasks)
                        worker_states[task_info.worker].teardown_running.push_back(std::move(task_info));
                    started_tasks.clear();
                }
                for (WorkerIndex worker = 0; worker < channels.size(); ++worker)
                    beginTeardownLocked(worker);
            }

            {
                std::unique_lock g(lock);
                teardown_progress.wait_until(g, std::chrono::steady_clock::now() + TEARDOWN_TIMEOUT,
                    [this]() TSA_NO_THREAD_SAFETY_ANALYSIS { return allConnectionsEndedLocked(); });
            }

            VectorWithMemoryTracking<WorkerIndex> unfinished;
            {
                std::lock_guard g(lock);
                for (WorkerIndex worker = 0; worker < channels.size(); ++worker)
                {
                    auto & state = worker_states[worker];
                    if (!channels[worker]->connection || state.ended)
                        continue;
                    LOG_WARNING(logger, "The control connection to {} did not close within {} ms after cancellation; aborting it, "
                        "so the worker reclaims {} tasks", channels[worker]->description, TEARDOWN_TIMEOUT.count(), state.teardown_running.size());
                    state.ended = true;
                    unfinished.push_back(worker);
                }
            }

            /// Without the lock: `abort` waits for a running callback, which takes it.
            for (const WorkerIndex worker : unfinished)
                channels[worker]->connection->abort();
        }

    private:
        struct PolledTask
        {
            String stage_name;
            RunningTaskInfo info;
        };

        struct StartRequest
        {
            VectorWithMemoryTracking<PolledTask> tasks;
            /// Null when the query is not traced.
            std::unique_ptr<OpenTelemetry::ManualSpan> span;
        };

        struct StatusRequest
        {
            UInt64 request_id = 0;
            Strings task_ids;
        };

        /// What the tracker knows of a worker and its control connection.
        struct WorkerState
        {
            /// The `StartTasks` sent and not answered, in order: the worker answers them in order.
            DequeWithMemoryTracking<StartRequest> start_requests;
            /// The tasks the worker started that have not ended; polled together.
            VectorWithMemoryTracking<PolledTask> polled_tasks;
            /// At most one `GetStatuses` is in flight.
            std::optional<StatusRequest> status_request;
            /// The `CancelTasks` and `ForgetTasks` in flight.
            UnorderedSetWithMemoryTracking<UInt64> other_requests;
            /// The connection ended: closed, refused, lost or aborted. No callback follows.
            bool ended = false;
            /// Why the connection ended, if it was refused or lost.
            std::exception_ptr lost;
            /// `close` was called, so no request may follow.
            bool close_sent = false;
            /// From `cancel` on: the cancelled tasks that have not ended, those that ended, and the polls sent for them.
            bool tearing_down = false;
            VectorWithMemoryTracking<RunningTaskInfo> teardown_running;
            Strings teardown_ended;
            size_t teardown_polls = 0;
        };

        /// The handler of the control connection of one worker.
        class WorkerChannel final : public IWorkerControlHandler
        {
        public:
            WorkerChannel(TaskTracker & tracker_, WorkerIndex worker_, String description_)
                : tracker(tracker_), worker(worker_), description(std::move(description_))
            {
            }

            void onHelloAccepted(const StatelessWorkerControlProtocol::ControlHelloReply &) override {}
            void onHelloRefused(const StatelessWorkerControlProtocol::ControlHelloReply & reply) override { tracker.onHelloRefused(worker, reply); }
            void onStartTasksReply(const StatelessWorkerControlProtocol::StartTasksReply & reply) override { tracker.onStartTasksReply(worker, reply); }
            void onGetStatusesReply(StatelessWorkerControlProtocol::GetStatusesReply & reply) override { tracker.onGetStatusesReply(worker, reply); }
            void onCancelTasksReply(UInt64 request_id) override { tracker.onRequestAnswered(worker, request_id); }
            void onForgetTasksReply(UInt64 request_id) override { tracker.onRequestAnswered(worker, request_id); }
            void onError(const StatelessWorkerControlProtocol::Error & error) override { tracker.onError(worker, error); }
            void onBroken(std::exception_ptr exception) override { tracker.onConnectionEnded(worker, exception); }
            void onClosed() override { tracker.onConnectionEnded(worker, nullptr); }

            TaskTracker & tracker;
            const WorkerIndex worker;
            /// `host:port` of the worker.
            const String description;
            /// Set by the query thread before its first request; only the callbacks that answer a request use it.
            WorkerControlConnectionPtr connection;
        };

        using StatusEntries = decltype(StatelessWorkerControlProtocol::GetStatusesReply::entries);

        static constexpr UInt64 STATUS_WAIT_MILLISECONDS = 300;
        static constexpr size_t MAX_TEARDOWN_POLLS = 10;
        static constexpr std::chrono::milliseconds TEARDOWN_TIMEOUT{5000};

        WorkerControlConnectionParams makeConnectionParams(WorkerIndex worker) const
        {
            const auto & address = task_to_host_map->getWorkerAddresses().at(worker);
            const auto credentials = context->getInterserverCredentials();
            return WorkerControlConnectionParams{
                .unique_query_id = unique_query_id,
                .host = address.host,
                .port = address.streaming_exchange_port,
                .auth_token = task_to_host_map->getAuthToken(),
                .user = credentials->getUser(),
                .password = String(credentials->getPassword()),
                .initiator = getFQDNOrHostName(),
                .thread_group = thread_group,
            };
        }

        /// Hands the progress the workers reported to the query's callback, on the query thread: the callback can write to the
        /// client's connection.
        void applyProgress()
        {
            Progress progress = pending_progress.fetchAndResetPiecewiseAtomically();
            if (progress.empty())
                return;
            if (auto progress_callback = context->getProgressCallback())
                progress_callback(progress);
        }

        /// A refusal for `max_queries` reads as a busy server.
        static int refusalErrorCode(int code)
        {
            return code == ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES ? ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS : code;
        }

        void onHelloRefused(WorkerIndex worker, const StatelessWorkerControlProtocol::ControlHelloReply & reply)
        {
            onConnectionEnded(worker, std::make_exception_ptr(Exception(refusalErrorCode(reply.code),
                "Stateless worker {} refused the control connection: {}", channels[worker]->description, reply.message)));
        }

        void onStartTasksReply(WorkerIndex worker, const StatelessWorkerControlProtocol::StartTasksReply & reply)
        {
            std::lock_guard g(lock);
            auto & state = worker_states[worker];
            StartRequest request = takeStartRequestLocked(worker);
            if (reply.code != 0)
            {
                failStartLocked(worker, request, reply.code, reply.message);
                return;
            }
            if (request.span)
                request.span->finish();
            ProfileEvents::increment(ProfileEvents::DistributedPlanTaskBatches);
            /// The teardown polls every registered task.
            if (state.tearing_down)
                return;
            for (auto & task : request.tasks)
                state.polled_tasks.push_back(std::move(task));
            sendStatusRequestLocked(worker);
        }

        void onGetStatusesReply(WorkerIndex worker, StatelessWorkerControlProtocol::GetStatusesReply & reply)
        {
            std::lock_guard g(lock);
            auto & state = worker_states[worker];
            if (!state.status_request || state.status_request->request_id != reply.request_id)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Stateless worker {} answered `GetStatuses` {}, which is not in flight",
                    channels[worker]->description, reply.request_id);
            const Strings task_ids = std::move(state.status_request->task_ids);
            state.status_request.reset();
            if (reply.entries.size() != task_ids.size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Stateless worker {} returned {} statuses for {} tasks",
                    channels[worker]->description, reply.entries.size(), task_ids.size());

            if (state.tearing_down)
                handleTeardownStatusesLocked(worker, task_ids, reply.entries);
            else
                handleStatusesLocked(worker, task_ids, reply.entries);
        }

        void onRequestAnswered(WorkerIndex worker, UInt64 request_id)
        {
            std::lock_guard g(lock);
            worker_states[worker].other_requests.erase(request_id);
        }

        void onError(WorkerIndex worker, const StatelessWorkerControlProtocol::Error & error)
        {
            std::lock_guard g(lock);
            auto & state = worker_states[worker];
            const String & description = channels[worker]->description;
            if (state.status_request && state.status_request->request_id == error.request_id)
            {
                state.status_request.reset();
                auto exception = std::make_exception_ptr(Exception(error.code, "Stateless worker {} failed `GetStatuses`: {}", description, error.message));
                if (state.tearing_down)
                {
                    tryLogException(exception, logger, "Waiting for the cancelled tasks to end", LogsLevel::warning);
                    finishTeardownLocked(worker);
                }
                else
                {
                    recordFailure(exception);
                }
                return;
            }
            if (state.other_requests.erase(error.request_id))
            {
                LOG_WARNING(logger, "Stateless worker {} failed a request to cancel or forget tasks: {}", description, error.message);
                return;
            }
            if (state.start_requests.empty())
            {
                recordFailure(std::make_exception_ptr(Exception(error.code, "Stateless worker {} failed request {}: {}", description, error.request_id, error.message)));
                return;
            }
            /// The worker answers the `StartTasks` in order, so this one is the oldest in flight.
            StartRequest request = takeStartRequestLocked(worker);
            failStartLocked(worker, request, error.code, error.message);
        }

        /// `exception` is null for a connection that closed after the teardown.
        void onConnectionEnded(WorkerIndex worker, std::exception_ptr exception)
        {
            std::lock_guard g(lock);
            auto & state = worker_states[worker];
            state.ended = true;
            state.lost = exception;
            if (exception)
            {
                const String message = getExceptionMessage(exception, /*with_stacktrace=*/ false);
                for (auto & request : state.start_requests)
                {
                    if (!request.span)
                        continue;
                    request.span->addAttribute("clickhouse.exception", message);
                    request.span->finish(OpenTelemetry::SpanStatus::ERROR, message);
                }
                /// The teardown leaves the tasks of a lost connection to the worker, which cancels them when the connection closes.
                if (cancel_started)
                    LOG_DEBUG(logger, "{}; the worker reclaims its tasks", message);
                else
                    recordFailure(exception);
            }
            state.start_requests.clear();
            state.polled_tasks.clear();
            state.status_request.reset();
            state.other_requests.clear();
            teardown_progress.notify_all();
        }

        StartRequest takeStartRequestLocked(WorkerIndex worker) TSA_REQUIRES(lock)
        {
            auto & state = worker_states[worker];
            if (state.start_requests.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Stateless worker {} answered a `StartTasks` that is not in flight", channels[worker]->description);
            StartRequest request = std::move(state.start_requests.front());
            state.start_requests.pop_front();
            return request;
        }

        void failStartLocked(WorkerIndex worker, StartRequest & request, int code, const String & message) TSA_REQUIRES(lock)
        {
            const String text = fmt::format("Stateless worker {} did not start {} tasks: {}", channels[worker]->description, request.tasks.size(), message);
            if (request.span)
            {
                request.span->addAttribute("clickhouse.exception", text);
                request.span->finish(OpenTelemetry::SpanStatus::ERROR, text);
            }
            recordFailure(std::make_exception_ptr(Exception(refusalErrorCode(code), "{}", text)));
        }

        /// Polls the worker's unfinished tasks, or the cancelled ones in the teardown, unless a poll is in flight.
        void sendStatusRequestLocked(WorkerIndex worker) TSA_REQUIRES(lock)
        {
            auto & state = worker_states[worker];
            if (state.status_request || state.ended || state.close_sent)
                return;
            /// A cancelled query stops polling until its teardown.
            if (!state.tearing_down && cancellation->isCancelled())
                return;

            Strings task_ids;
            if (state.tearing_down)
            {
                for (const auto & task : state.teardown_running)
                    task_ids.push_back(task.task_id);
                ++state.teardown_polls;
            }
            else
            {
                for (const auto & task : state.polled_tasks)
                    task_ids.push_back(task.info.task_id);
            }
            if (task_ids.empty())
                return;

            ProfileEvents::increment(ProfileEvents::DistributedPlanTaskStatusRequests);
            const UInt64 request_id = channels[worker]->connection->getStatuses(STATUS_WAIT_MILLISECONDS, task_ids);
            state.status_request = StatusRequest{.request_id = request_id, .task_ids = std::move(task_ids)};
        }

        void sendForgetLocked(WorkerIndex worker, Strings task_ids) TSA_REQUIRES(lock)
        {
            auto & state = worker_states[worker];
            if (state.ended || state.close_sent)
                return;
            ProfileEvents::increment(ProfileEvents::DistributedPlanTaskForgetRequests);
            state.other_requests.insert(channels[worker]->connection->forgetTasks(std::move(task_ids)));
        }

        void closeLocked(WorkerIndex worker) TSA_REQUIRES(lock)
        {
            auto & state = worker_states[worker];
            if (state.ended || state.close_sent)
                return;
            state.close_sent = true;
            channels[worker]->connection->close();
        }

        /// The statuses of the polled tasks, which are the first `task_ids.size()` of them: new tasks are appended and only
        /// this removes any.
        void handleStatusesLocked(WorkerIndex worker, const Strings & task_ids, StatusEntries & entries) TSA_REQUIRES(lock)
        {
            auto & state = worker_states[worker];
            Strings ended_ids;
            VectorWithMemoryTracking<PolledTask> finished;
            bool has_progress = false;
            for (size_t i = 0; i < task_ids.size(); ++i)
            {
                const PolledTask & task = state.polled_tasks.at(i);
                if (task.info.task_id != task_ids[i])
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Task {} is polled on {} in place of {}", task.info.task_id, channels[worker]->description, task_ids[i]);

                auto & task_status = entries[i].status;
                handleWorkerLogsLocked(task.info, task_status);
                has_progress |= pending_progress.incrementPiecewiseAtomically(task_status.progress);
                if (task_status.status != "Finished" && task_status.status != "Running")
                    recordTaskFailureOnceLocked(task.info, task_status);
                if (!isTerminalTaskStatus(task_status.status))
                    continue;
                ended_ids.push_back(task.info.task_id);
                if (task_status.status == "Finished")
                    finished.push_back(task);
            }

            if (!ended_ids.empty())
            {
                /// Only after recording the failures: a forgotten task answers the teardown's poll with "Unknown task", which
                /// carries none.
                const UnorderedSetWithMemoryTracking<String> ended(ended_ids.begin(), ended_ids.end());
                sendForgetLocked(worker, std::move(ended_ids));
                std::erase_if(state.polled_tasks, [&ended](const PolledTask & task) { return ended.contains(task.info.task_id); });
                for (const auto & task : finished)
                    setTaskFinishedLocked(task.stage_name, task.info.task_id);
            }
            /// The waiting source applies the progress.
            if (has_progress)
                notifyStageWakeup(stage_wakeup);
            sendStatusRequestLocked(worker);
        }

        /// The statuses of a poll that the teardown found in flight cover only some of the cancelled tasks, so they are
        /// matched by id.
        void handleTeardownStatusesLocked(WorkerIndex worker, const Strings & task_ids, StatusEntries & entries) TSA_REQUIRES(lock)
        {
            auto & state = worker_states[worker];
            UnorderedMapWithMemoryTracking<String, size_t> entry_of_task;
            for (size_t i = 0; i < task_ids.size(); ++i)
                entry_of_task.emplace(task_ids[i], i);

            VectorWithMemoryTracking<RunningTaskInfo> running;
            for (auto & task : state.teardown_running)
            {
                auto entry = entry_of_task.find(task.task_id);
                if (entry == entry_of_task.end())
                {
                    running.push_back(std::move(task));
                    continue;
                }
                auto & task_status = entries[entry->second].status;
                /// A task cancelled early (e.g. LIMIT satisfied) still delivers its logs through the cleanup polls.
                handleWorkerLogsLocked(task, task_status);
                if (isTerminalTaskStatus(task_status.status))
                {
                    /// Its failure can be the root cause of the teardown: the initiator often sees an effect first, e.g. the
                    /// result reader's socket closing.
                    if (task_status.status != "Finished" && task_status.status != "Unknown task")
                        recordTaskFailureOnceLocked(task, task_status);
                    state.teardown_ended.push_back(task.task_id);
                    continue;
                }
                /// Freeing the resources of a failing task can take longer than this wait.
                if (task_status.status == "Failing")
                    recordTaskFailureOnceLocked(task, task_status);
                running.push_back(std::move(task));
            }
            state.teardown_running = std::move(running);

            if (state.teardown_running.empty() || state.teardown_polls >= MAX_TEARDOWN_POLLS)
                finishTeardownLocked(worker);
            else
                sendStatusRequestLocked(worker);
        }

        /// Cancels the tasks of the worker and polls them, or closes the connection at once when the worker has none.
        void beginTeardownLocked(WorkerIndex worker) TSA_REQUIRES(lock)
        {
            auto & state = worker_states[worker];
            const WorkerChannel & channel = *channels[worker];
            state.polled_tasks.clear();
            if (!channel.connection || state.ended)
            {
                if (!state.teardown_running.empty())
                    LOG_DEBUG(logger, "Leaving {} tasks to stateless worker {}, whose control connection ended", state.teardown_running.size(), channel.description);
                state.teardown_running.clear();
                return;
            }

            state.tearing_down = true;
            if (state.teardown_running.empty())
            {
                closeLocked(worker);
                return;
            }

            Strings task_ids;
            for (const auto & task : state.teardown_running)
            {
                LOG_TRACE(logger, "Cancelling task {} on host {}", task.task_id, channel.description);
                task_ids.push_back(task.task_id);
            }
            ProfileEvents::increment(ProfileEvents::DistributedPlanTaskCancelRequests);
            state.other_requests.insert(channel.connection->cancelTasks(std::move(task_ids)));
            /// A poll in flight is answered first, and its reply sends the next one.
            sendStatusRequestLocked(worker);
        }

        /// `forget` drops the worker's only handle to a task, so forgetting one that is still running would orphan a fragment
        /// that keeps writing exchange and temporary outputs. So only the ended tasks are forgotten; the connection closes after
        /// that, and the worker cancels and reclaims the rest.
        void finishTeardownLocked(WorkerIndex worker) TSA_REQUIRES(lock)
        {
            auto & state = worker_states[worker];
            if (!state.teardown_ended.empty())
                sendForgetLocked(worker, std::exchange(state.teardown_ended, {}));
            for (const auto & task : state.teardown_running)
                LOG_WARNING(logger, "Task {} on {} did not reach a terminal state after cancellation; leaving it for the worker to reclaim",
                    task.task_id, channels[worker]->description);
            closeLocked(worker);
        }

        bool allConnectionsEndedLocked() const TSA_REQUIRES(lock)
        {
            for (WorkerIndex worker = 0; worker < channels.size(); ++worker)
                if (channels[worker]->connection && !worker_states[worker].ended)
                    return false;
            return true;
        }

        /// Record a task's failed status under the worker's error code, so the client sees it and the
        /// record can rank it. A status other than `Failed` or `Failing` carries no code and gets the
        /// generic remote error code, which ranks below a known root cause. A failing task reports its
        /// failure on every poll until it ends, but it is recorded once.
        void recordTaskFailureOnceLocked(const RunningTaskInfo & task, const DistributedQueryTaskStatus & task_status) TSA_REQUIRES(lock)
        {
            if (!tasks_with_recorded_failure.insert(task.task_id).second)
                return;
            const int code = task_status.error_code != 0 ? task_status.error_code : ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
            recordFailure(std::make_exception_ptr(Exception(code, "Task {} did not finish successfully (status: {}): {}",
                task.task_id, task_status.status, task_status.error_message)));
        }

        /// Log the exception, record it as the query's failure, and request cancellation, so a failure on the loop surfaces
        /// through `checkCancelled`.
        void recordFailure(std::exception_ptr exception)
        {
            /// A consequence is expected while the query stops; only a root cause is worth an error entry.
            const bool is_consequence = DistributedQueryCancellation::isConsequence(getExceptionErrorCode(exception));
            if (is_consequence)
                LOG_TRACE(logger, "Task ended with a failure that another one caused: {}", getExceptionMessage(exception, /*with_stacktrace=*/ false));
            else
                tryLogException(exception, __PRETTY_FUNCTION__);
            /// The teardown waits a bounded time for each task's outcome; a root cause reported later
            /// is recorded, but the query has been reported already.
            if (!cancellation->recordException(exception) && !is_consequence)
                LOG_WARNING(logger, "The failure arrived after the query reported another one, the client did not see it");
        }

        void checkCancelled()
        {
            if (query_status)
                query_status->checkTimeLimit();

            cancellation->throwIfCancelled();
        }

        /// Pushes a warning to the client under the task's id.
        void pushInitiatorLogLine(const String & query_id, const String & text)
        {
            static const String source = "DistributedQueryPlanExecutor";
            if (initiator_logs_queue && initiator_logs_queue->isNeeded(Poco::Message::PRIO_WARNING, source))
                initiator_logs_queue->pushMessage(Poco::Message::PRIO_WARNING, source, query_id, text);
        }

        /// Forwards the batch and warns about lines missing before `begin_offset` and about lines dropped on the worker.
        void handleWorkerLogsLocked(const RunningTaskInfo & task, DistributedQueryTaskStatus & task_status) TSA_REQUIRES(lock)
        {
            if (!initiator_logs_queue || !task_status.logs)
                return;

            auto & logs = *task_status.logs;
            const UInt64 num_rows = logs.rows.rows();

            if (num_rows != 0)
                initiator_logs_queue->pushBlock(std::move(logs.rows));

            UInt64 lost_in_transit = 0;
            UInt64 newly_dropped = 0;
            auto & cursor = worker_log_cursors[task.task_id];

            if (logs.begin_offset >= cursor.expected_offset)
            {
                lost_in_transit = logs.begin_offset - cursor.expected_offset;
                cursor.expected_offset = logs.begin_offset + num_rows;
            }

            if (logs.dropped_total > cursor.dropped_reported)
            {
                newly_dropped = logs.dropped_total - cursor.dropped_reported;
                cursor.dropped_reported = logs.dropped_total;
            }

            const String & description = channels[task.worker]->description;
            if (lost_in_transit != 0)
                pushInitiatorLogLine(task.task_id, fmt::format(
                    "{} worker log line(s) from {} were lost in transit", lost_in_transit, description));

            if (newly_dropped != 0)
                pushInitiatorLogLine(task.task_id, fmt::format(
                    "{} worker log line(s) were dropped on {} because the forwarding buffer was full", newly_dropped, description));
        }

        /// The task has ended on the worker. A `Failing` task has not: it still frees its resources.
        static bool isTerminalTaskStatus(const String & status)
        {
            return status != "Running" && status != "Failing";
        }

        void setTaskFinishedLocked(const String & stage_name, const String & task_name) TSA_REQUIRES(lock)
        {
            auto & stage = all_stages[stage_name];
            stage->finished_tasks++;

            /// Signal completion at most once: a status-poll race can make the counts match again
            /// after the stage finished, and setting the promise twice throws.
            if (stage->finished_tasks == stage->started_tasks && stage_results.contains(stage_name) && !stage->promise_signaled)
            {
                stage->promise_signaled = true;
                stage->promise.set_value();
                /// `waitForStage` waits on this promise, so tell the waiter its answer changed.
                notifyStageWakeup(stage_wakeup);
            }

            stage_tasks[stage_name].erase(task_name); // TODO: really need to erase?
        }

        struct StageInfo
        {
            const String name;
            Int64 started_tasks = 0;
            Int64 finished_tasks = 0;
            std::promise<void> promise;
            bool promise_signaled = false;
        };

        using StageInfoPtr = std::shared_ptr<StageInfo>;

        const TaskToHostMapPtr task_to_host_map;
        const String unique_query_id;
        ContextPtr context;
        QueryStatusPtr query_status;

        std::mutex lock;
        UnorderedMapWithMemoryTracking<String, StageInfoPtr> all_stages TSA_GUARDED_BY(lock);
        UnorderedMapWithMemoryTracking<String, MapWithMemoryTracking<String, RunningTaskInfo>> stage_tasks TSA_GUARDED_BY(lock);
        /// Per task: where the next batch should start and how many worker drops were already reported.
        /// Kept after the task ends, so a late poll reads as "nothing new".
        struct WorkerLogCursor
        {
            UInt64 expected_offset = 0;
            UInt64 dropped_reported = 0;
        };
        UnorderedMapWithMemoryTracking<String, WorkerLogCursor> worker_log_cursors TSA_GUARDED_BY(lock);
        bool cancel_started TSA_GUARDED_BY(lock) = false;
        /// Tasks whose failure is recorded, see `recordTaskFailureOnceLocked`.
        UnorderedSetWithMemoryTracking<String> tasks_with_recorded_failure TSA_GUARDED_BY(lock);
        UnorderedMapWithMemoryTracking<String, std::shared_future<void>> stage_results TSA_GUARDED_BY(lock);
        /// Indexed by `WorkerIndex`.
        VectorWithMemoryTracking<WorkerState> worker_states TSA_GUARDED_BY(lock);
        /// Notified when a connection ends, for `cancel`.
        std::condition_variable teardown_progress;
        /// Summed on the loop, taken by `applyProgress`.
        Progress pending_progress;
        DistributedQueryCancellationPtr cancellation;
        StageWakeupPtr stage_wakeup;
        LoggerPtr logger;

        /// Initiator logs queue captured at construction so it is tied to the main query's thread
        InternalTextLogsQueuePtr initiator_logs_queue = CurrentThread::getInternalTextLogsQueue();
        /// The query's, so the work of its control connections counts in its profile events.
        ThreadGroupPtr thread_group = CurrentThread::getGroup();

        /// Indexed by `WorkerIndex`; never resized after the constructor. Last, so the connections are destroyed first.
        VectorWithMemoryTracking<std::unique_ptr<WorkerChannel>> channels;
    };

    static std::optional<UInt64> stagesPerFlush(const ContextPtr & query_context)
    {
        if (const UInt64 stages = query_context->getSettingsRef()[Setting::distributed_plan_task_dispatch_stages_per_flush])
            return stages;
        return std::nullopt;
    }

    void startStage(const String & stage_name, const DistributedQueryStage & stage) override
    {
        DistributedQueryTaskDescription task_description;
        task_description.initial_query_id = logicalQueryId(context->getClientInfo());
        task_description.serialized_query_plan = std::make_shared<const String>(serializeQueryPlan(stage.query_plan_fragment, context));
        task_description.exchanges = distributed_query_plan.exchange_descriptions; /// TODO: add only exchanges for this stage
        task_description.settings_changes = getTaskSettingsChanges(context);
        queueStageTasks(stage_name, stage, task_description);
        if (stages_per_flush && ++stages_since_flush >= *stages_per_flush)
        {
            stages_since_flush = 0;
            dispatchQueuedTasks();
        }
    }

    void checkBeforeStartingTask()
    {
        /// Reproduce the ordering a failing worker task creates: a task fails and records its
        /// exception while this loop is still dispatching the rest of the stage, so the next
        /// `checkCancelled` observes the cancellation. It must report the recorded failure.
        fiu_do_on(FailPoints::distributed_plan_record_failure_while_starting_tasks,
        {
            try
            {
                throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Injected task failure while starting tasks");
            }
            catch (...)
            {
                /// `recordCurrentException` stores it as the query's first failure. Ok.
                cancellation->recordCurrentException();
            }
        });

        checkCancelled();
    }

    /// The hosts that serve the input streams of `task`.
    ExchangeStreamSources getExchangeStreamSources(const DistributedQueryTask & task) const
    {
        ExchangeStreamSources exchange_stream_sources;
        for (const auto & input_stream : task.input_exchange_streams)
        {
            String input_stream_name = input_stream.toString();
            exchange_stream_sources.stream_hosts[input_stream_name] = task_to_host_map->getExchangeStreamSourceHosts().at(input_stream_name);
        }
        return exchange_stream_sources;
    }

    bool waitForStage(const String & stage_name, std::optional<UInt64> timeout_ms) override
    {
        return running_tasks.waitForStage(stage_name, timeout_ms);
    }

    /// Upper bound of the plans in one `StartTasks` batch; a single larger plan goes alone.
    static constexpr size_t MAX_TASK_BATCH_PLAN_BYTES = 100 * 1024 * 1024;

    struct QueuedBatch
    {
        DistributedQueryTaskBatch batch;
        size_t plan_bytes = 0;
    };

    using StagePerWorker = MapWithMemoryTracking<WorkerIndex, DistributedQueryTaskBatch::Stage>;

    /// Appends the tasks of a stage to the batches of their workers, or one batch per task when batching is off;
    /// `dispatchQueuedTasks` sends them.
    void queueStageTasks(const String & stage_name, const DistributedQueryStage & stage, const DistributedQueryTaskDescription & stage_description)
    {
        if (batch_dispatch)
        {
            appendStagesToBatches(groupStageTasksByWorker(stage_name, stage, stage_description.serialized_query_plan), stage_description);
            return;
        }

        for (const auto & task : stage.tasks)
        {
            checkBeforeStartingTask();
            auto & queued = addBatch(task_to_host_map->getTaskWorkerIndex(task.task_id), stage_description);
            queued.batch.stages.push_back({stage_name, stage_description.serialized_query_plan, {{task, getExchangeStreamSources(task)}}});
            queued.plan_bytes += stage_description.serialized_query_plan->size();
        }
    }

    /// The tasks of a stage grouped by their worker, as one batch stage per worker.
    StagePerWorker groupStageTasksByWorker(const String & stage_name, const DistributedQueryStage & stage, const SerializedQueryPlanPtr & serialized_query_plan)
    {
        StagePerWorker stage_per_worker;
        for (const auto & task : stage.tasks)
        {
            checkBeforeStartingTask();
            auto & batch_stage = stage_per_worker[task_to_host_map->getTaskWorkerIndex(task.task_id)];
            batch_stage.name = stage_name;
            batch_stage.serialized_query_plan = serialized_query_plan;
            batch_stage.tasks.push_back({task, getExchangeStreamSources(task)});
        }
        return stage_per_worker;
    }

    /// Appends each worker's stage to its last queued batch, or to a new one when the plans would pass the cap.
    void appendStagesToBatches(StagePerWorker stage_per_worker, const DistributedQueryTaskDescription & stage_description)
    {
        const size_t plan_bytes = stage_description.serialized_query_plan->size();
        for (auto & [worker, batch_stage] : stage_per_worker)
        {
            auto & batches = queued_batches[worker];
            auto & queued = batches.empty() || batches.back().plan_bytes + plan_bytes > MAX_TASK_BATCH_PLAN_BYTES
                ? addBatch(worker, stage_description)
                : batches.back();
            queued.batch.stages.push_back(std::move(batch_stage));
            queued.plan_bytes += plan_bytes;
        }
    }

    /// A new queued batch of `worker`, with the query's fields.
    QueuedBatch & addBatch(WorkerIndex worker, const DistributedQueryTaskDescription & stage_description)
    {
        auto & queued = queued_batches[worker].emplace_back();
        queued.batch.initial_query_id = stage_description.initial_query_id;
        queued.batch.exchanges = stage_description.exchanges;
        queued.batch.settings_changes = stage_description.settings_changes;
        return queued;
    }

    /// Sends the queued batches of every worker; the sends do not wait for the workers.
    void dispatchQueuedTasks() override
    {
        for (auto & [worker, batches] : queued_batches)
        {
            const String description = describeWorker(task_to_host_map->getWorkerAddresses().at(worker));
            for (const auto & queued : batches)
            {
                for (const auto & batch_stage : queued.batch.stages)
                {
                    for (const auto & batch_task : batch_stage.tasks)
                    {
                        RunningTaskInfo task_info{.worker = worker, .task_id = makeUniqueTaskId(unique_query_id, batch_task.task.task_id)};
                        LOG_DEBUG(logger, "Queueing task {} for {}", task_info.task_id, description);
                        /// Before the send, so the teardown cancels a task whose start is in flight.
                        running_tasks.registerTask(batch_stage.name, std::move(task_info));
                        ProfileEvents::increment(ProfileEvents::DistributedPlanRemoteTasks);
                    }
                }
            }

            for (auto & queued : batches)
                running_tasks.startTasks(worker, std::move(queued.batch));
        }
        queued_batches.clear();
    }

    TaskToHostMapPtr task_to_host_map;
    TaskTracker running_tasks;
    const bool batch_dispatch;
    const std::optional<UInt64> stages_per_flush;
    /// Stages queued since `startStage` last flushed; query thread only.
    size_t stages_since_flush = 0;
    /// The batches `startStage` built and `dispatchQueuedTasks` has not sent yet; query thread only.
    UnorderedMapWithMemoryTracking<WorkerIndex, VectorWithMemoryTracking<QueuedBatch>> queued_batches;
};

#endif


DistributedQueryCancellation::DistributedQueryCancellation()
    : wakeup(std::make_shared<WakeupFd>())
    , cancelled_wakeup(std::make_shared<WakeupFd>())
{
}

DistributedQueryCancellation::FailureRank DistributedQueryCancellation::rankOf(int error_code)
{
    if (error_code == ErrorCodes::QUERY_WAS_CANCELLED || error_code == ErrorCodes::EXCHANGE_PEER_DISCONNECTED)
        return FailureRank::Consequence;
    if (error_code == ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER)
        return FailureRank::Unclassified;
    return FailureRank::RootCause;
}

void DistributedQueryCancellation::cancel()
{
    cancelled_by_pipeline = true;
    cancelled = true;
    notifyStageWakeup(wakeup);
    notifyStageWakeup(cancelled_wakeup);
}

bool DistributedQueryCancellation::recordException(std::exception_ptr exception)
{
    const FailureRank rank = rankOf(getExceptionErrorCode(exception));
    bool driving_source_reports = false;
    {
        /// Publish the failure and the flag in one critical section. Setting the flag outside it
        /// would let a waiter that already read no failure still see the flag and report
        /// `Query was cancelled`.
        std::lock_guard lock(mutex);
        const bool worth_recording = rank == FailureRank::RootCause || !cancelled_by_pipeline;
        if (worth_recording && (!failure || rank > failure_rank))
        {
            failure = exception;
            failure_rank = rank;
        }
        cancelled = true;
        driving_source_reports = !execution_finished;
    }
    notifyStageWakeup(wakeup);
    notifyStageWakeup(cancelled_wakeup);
    return driving_source_reports;
}

void DistributedQueryCancellation::markExecutionFinished()
{
    std::lock_guard lock(mutex);
    execution_finished = true;
}

bool DistributedQueryCancellation::isExecutionFinished() const
{
    std::lock_guard lock(mutex);
    return execution_finished;
}

std::exception_ptr DistributedQueryCancellation::getFailure() const
{
    std::lock_guard lock(mutex);
    return failure;
}

void DistributedQueryCancellation::rethrowIfFailedLocked() const
{
    if (failure)
        std::rethrow_exception(failure);
}

void DistributedQueryCancellation::rethrowIfFailed() const
{
    std::lock_guard lock(mutex);
    rethrowIfFailedLocked();
}

void DistributedQueryCancellation::throwIfCancelled() const
{
    /// One critical section for both, so a failure recorded by `recordCurrentException` is always
    /// seen together with the flag it set.
    std::lock_guard lock(mutex);
    rethrowIfFailedLocked();

    if (cancelled)
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
}


void notifyStageWakeup(const StageWakeupPtr & stage_wakeup) noexcept
{
    if (!stage_wakeup)
        return;
    try
    {
        stage_wakeup->notify();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

DistributedQueryPlanExecutor::DistributedQueryPlanExecutor(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_, DistributedQueryCancellationPtr cancellation_, StageWakeupPtr stage_wakeup_)
    : unique_query_id(unique_query_id_)
    , distributed_query_plan(distributed_query_plan_)
    , context(std::move(context_))
    , query_status(context->getProcessListElement())
    , cancellation(std::move(cancellation_))
    , stage_wakeup(std::move(stage_wakeup_))
    , logger(getLogger("DistributedQueryPlanExecutor"))
{
}

void DistributedQueryPlanExecutor::checkCancelled() const
{
    if (query_status)
        query_status->checkTimeLimit();

    /// A task failure cancels the query, so report that failure rather than the cancellation it
    /// caused; otherwise the real error would stay in the log and the client would see only
    /// `Query was cancelled`.
    cancellation->throwIfCancelled();
}

void DistributedQueryPlanExecutor::startStageWithDependencies(const String & stage_name, UnorderedSetWithMemoryTracking<String> & executed_stages)
{
    if (executed_stages.contains(stage_name))
        return;

    /// A streaming producer runs concurrently with its consumer and blocks until the consumer
    /// connects, so start streaming producers only after this stage (their consumer); starting them
    /// first can fill the worker pool with producers waiting for consumers that have not run yet.
    /// A persisted producer runs to completion before the consumer reads its output, so start and
    /// wait for persisted producers up front.
    Strings streaming_dependencies;
    if (distributed_query_plan.stage_depends_on.contains(stage_name))
    {
        Strings dependencies_to_wait;

        for (const auto & [dependency, exchange_id] : distributed_query_plan.stage_depends_on.at(stage_name))
        {
            if (distributed_query_plan.exchange_descriptions.at(exchange_id).kind == ExchangeDescription::Kind::Persisted)
            {
                startStageWithDependencies(dependency, executed_stages);
                dependencies_to_wait.push_back(dependency);
            }
            else
            {
                streaming_dependencies.push_back(dependency);
            }
        }

        if (!dependencies_to_wait.empty())
            dispatchQueuedTasks();

        /// Wait in bounded steps so cancellation interrupts the wait. The caller holds the
        /// source's executor mutex, so blocking here unboundedly would also block onCancel.
        for (const auto & dependency : dependencies_to_wait)
        {
            while (!waitForStage(dependency, /*timeout_ms=*/ 100))
                checkCancelled();
        }
    }

    const auto & stage = distributed_query_plan.stages.at(stage_name);
    LOG_DEBUG(logger,
        "\n====================== Executing stage '{}' =========================\n"
        "PLAN:\n{}\nTASKS: {}\n"
        "==========================================================================",
        stage_name, dumpQueryPlan(stage.query_plan_fragment), stage.tasks.size());
    startStage(stage_name, stage);
    executed_stages.insert(stage_name);

    /// The consumer is running now and can accept connections, so start its streaming producers.
    for (const auto & dependency : streaming_dependencies)
        startStageWithDependencies(dependency, executed_stages);
}

void DistributedQueryPlanExecutor::start()
{
    LOG_DEBUG(logger, "Starting distributed query, unique id: {}", toString(unique_query_id));

    /// Start from the root stages (those no other stage depends on) so the recursion can start each
    /// consumer before its streaming producers. Entering at an arbitrary stage could start a producer
    /// before its consumer is running.
    {
        UnorderedSetWithMemoryTracking<String> depended_on_stages;
        for (const auto & [_, dependencies] : distributed_query_plan.stage_depends_on)
            for (const auto & [dependency, exchange_id] : dependencies)
                depended_on_stages.insert(dependency);

        UnorderedSetWithMemoryTracking<String> executed_stages;
        for (const auto & [stage_name, _] : distributed_query_plan.stages)
            if (!depended_on_stages.contains(stage_name))
                startStageWithDependencies(stage_name, executed_stages);

        /// Start anything not reachable from a root (a disconnected stage should not occur, but if it
        /// does it must still run).
        for (const auto & [stage_name, _] : distributed_query_plan.stages)
            startStageWithDependencies(stage_name, executed_stages);
    }
    dispatchQueuedTasks();

    /// Wait for all data stages to finish. Whether the filter-only stages are waited for depends on
    /// the executor, see `DistributedQueryStage::filter_only`.
    for (const auto & [stage_name, stage] : distributed_query_plan.stages)
        if (!stage.filter_only || waitsForFilterOnlyStages())
            running_stages.push_back(stage_name);
}

bool DistributedQueryPlanExecutor::execute(UInt64 poll_timeout_ms)
{
    /// Multiple stages could have finished already: drain them until the first unfinished one.
    /// Only the first wait may block; the rest are non-blocking checks.
    while (!running_stages.empty())
    {
        auto & stage_name = running_stages.front();
        if (!waitForStage(stage_name, poll_timeout_ms))
            return false;

        LOG_DEBUG(logger, "Stage '{}' finished", stage_name);
        running_stages.pop_front();
        poll_timeout_ms = 0;
    }

    return true;
}

std::unique_ptr<DistributedQueryPlanExecutor> createDistributedQueryExecutor(
    const UUID & unique_query_id,
    const DistributedQueryPlan & distributed_query_plan,
    TaskToHostMapPtr task_to_host_map,
    ContextPtr context,
    DistributedQueryCancellationPtr cancellation,
    StageWakeupPtr stage_wakeup)
{
    /// A null map means the plan was built for in-process execution, so it carries no worker hosts.
    /// Deriving the branch from the map instead of re-reading `distributed_plan_execute_locally` keeps
    /// the executor kind and the map consistent by construction.
    std::unique_ptr<DistributedQueryPlanExecutor> executor;
    if (!task_to_host_map)
    {
        ProfileEvents::increment(ProfileEvents::DistributedPlanLocalExecution);
        executor = std::make_unique<DistributedQueryPlanExecutorLocal>(unique_query_id, distributed_query_plan, context, cancellation, stage_wakeup);
    }
    else
    {
#if defined(OS_LINUX) || defined(OS_DARWIN)
        executor = std::make_unique<DistributedQueryPlanExecutorRemote>(unique_query_id, distributed_query_plan, task_to_host_map, context, cancellation, stage_wakeup);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Running a distributed query plan on stateless workers is supported only on Linux and macOS");
#endif
    }

    return executor;
}

void cancelDistributedQueryInMemoryExchanges(const UUID & unique_query_id, std::exception_ptr failure)
{
    /// Must not fail on the memory limit: it unblocks the task threads that are joined next.
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    InMemoryExchanges::instance()->cancelQuery(toString(unique_query_id), failure);
}

}
