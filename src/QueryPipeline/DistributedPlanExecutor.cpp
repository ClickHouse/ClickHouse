#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <QueryPipeline/DistributedPlanExecutor.h>
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
#include <Processors/Sinks/NativeCompressedSink.h>
#include <Processors/Sources/NativeCompressedSource.h>
#include <Planner/Utils.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <Poco/URI.h>
#include <Server/StatelessWorker/StatelessWorkerClient.h>
#include <Server/DistributedQuery/StreamingExchangeLookup.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>
#include <base/defines.h>
#include <base/getFQDNOrHostName.h>


namespace CurrentMetrics
{
    extern const Metric TaskTrackerThreads;
    extern const Metric TaskTrackerThreadsActive;
    extern const Metric TaskTrackerThreadsScheduled;
}


namespace DB
{

namespace Setting
{
    extern const SettingsBool distributed_plan_execute_locally;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int QUERY_WAS_CANCELLED;
}

class TaskParameters : public IParameterLookup
{
public:
    explicit TaskParameters(const QueryPlanParameters & parameters_)
        : parameters(parameters_)
    {
    }

    Field getParameter(const String & name) const override
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
    const std::unordered_set<String> input_temporary_files;
    const std::unordered_set<String> output_temporary_files;
    std::vector<std::unique_ptr<WriteBuffer>> write_buffers;
    LoggerPtr logger = getLogger("TemporaryFilesInObjectStorage");
};


class ExchangeViaTemporaryFiles : public IExchangeLookup
{
public:
    explicit ExchangeViaTemporaryFiles(TemporaryFileLookupPtr temporary_files_)
        : temporary_files(std::move(temporary_files_))
    {
    }

    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId & exchange_stream_id) override
    {
        if (!temporary_files)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Object storage for Persisted exchanges is not configured, exchange stream id: {}",
                exchange_stream_id.toString());

        auto file_name = exchange_stream_id.toString();
        return std::make_shared<NativeCompressedSink>(input_header, temporary_files->getTemporaryFileForWriting(file_name), file_name);
    }

    std::shared_ptr<ISource> createSource(SharedHeader output_header, const ExchangeStreamId & exchange_stream_id) override
    {
        if (!temporary_files)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Object storage for Persisted exchanges is not configured, exchange stream id: {}",
                exchange_stream_id.toString());

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

    void appendChunk(Chunk chunk)
    {
        LOG_TEST(log, "Appending chunk to exchange '{}', rows {}", name, chunk.getNumRows());

        std::lock_guard lock(mutex);
        chunks.emplace_back(std::move(chunk));
        has_data.notify_one();
    }

    Chunk getChunk()
    {
        LOG_TEST(log, "Waiting for chunk from exchange '{}'", name);

        Chunk chunk;
        {
            std::unique_lock lock(mutex);
            has_data.wait(lock, [this] { return !chunks.empty(); });
            chunk = std::move(chunks.front());
            chunks.pop_front();
        }

        LOG_TEST(log, "Got chunk from exchange '{}', rows {}", name, chunk.getNumRows());

        return chunk;
    }

private:
    LoggerPtr log = getLogger("InMemoryExchange");
    String name;
    std::mutex mutex;
    std::condition_variable has_data;
    std::deque<Chunk> chunks;
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
            element = std::make_shared<InMemoryExchange>(exchange_id);
        return element;
    }

    static std::shared_ptr<InMemoryExchanges> instance()
    {
        static std::shared_ptr<InMemoryExchanges> self = std::make_shared<InMemoryExchanges>();
        return self;
    }

private:
    using InMemoryExchangeMap = std::unordered_map<String, InMemoryExchangePtr>;

    std::unordered_map<String, InMemoryExchangeMap> exchanges_by_query_id TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

class ExchangeViaChunks : public IExchangeLookup
{
public:
    explicit ExchangeViaChunks(const String & query_id_)
        : query_id(query_id_)
    {
    }

    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId & exchange_stream_id) override
    {
        auto file_name = exchange_stream_id.toString();
        auto exchange = InMemoryExchanges::instance()->getExchange(query_id, file_name);
        return std::make_shared<SinkFromInMemoryExchange>(input_header, exchange);
    }

    std::shared_ptr<ISource> createSource(SharedHeader output_header, const ExchangeStreamId & exchange_stream_id) override
    {
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

        void consume(Chunk chunk) override
        {
            exchange->appendChunk(std::move(chunk));
        }

        void onFinish() override
        {
            exchange->appendChunk({});
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

        Chunk generate() override
        {
            return exchange->getChunk();
        }
    private:
        InMemoryExchangePtr exchange;
    };

    const String query_id;
};


/// A wrapper that looks up exchanges by their kind and delegates to the corresponding exchange lookup: Persistent or Streaming
class AllKindsExchangeLookup : public IExchangeLookup
{
public:
    AllKindsExchangeLookup(
        const std::unordered_map<String, ExchangeDescription> & exchanges_,
        ExchangeLookupPtr persistent_exchange_lookup_,
        ExchangeLookupPtr streaming_exchange_lookup_)
        : exchanges(exchanges_)
        , persistent_exchange_lookup(std::move(persistent_exchange_lookup_))
        , streaming_exchange_lookup(std::move(streaming_exchange_lookup_))
    {
    }

    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId & exchange_stream_id) override
    {
        auto it = exchanges.find(exchange_stream_id.exchange_id);
        if (it == exchanges.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange '{}'", exchange_stream_id.exchange_id);

        if (it->second.kind == ExchangeDescription::Kind::Persisted)
            return persistent_exchange_lookup->createSink(input_header, exchange_stream_id);
        else if (it->second.kind == ExchangeDescription::Kind::Streaming)
            return streaming_exchange_lookup->createSink(input_header, exchange_stream_id);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange kind '{}'", static_cast<int>(it->second.kind));
    }

    std::shared_ptr<ISource> createSource(SharedHeader output_header, const ExchangeStreamId & exchange_stream_id) override
    {
        auto it = exchanges.find(exchange_stream_id.exchange_id);
        if (it == exchanges.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange '{}'", exchange_stream_id.exchange_id);

        if (it->second.kind == ExchangeDescription::Kind::Persisted)
            return persistent_exchange_lookup->createSource(output_header, exchange_stream_id);
        else if (it->second.kind == ExchangeDescription::Kind::Streaming)
            return streaming_exchange_lookup->createSource(output_header, exchange_stream_id);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange kind '{}'", static_cast<int>(it->second.kind));
    }

private:
    const std::unordered_map<String, ExchangeDescription> exchanges;
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
    const std::unordered_set<String> temporary_files;
};

std::shared_ptr<ICustomResourceHolder> makeTemporaryFilesCleaner(ObjectStoragePtr object_storage_, const String & object_storage_path_, const Strings & temporary_files_)
{
    return std::make_shared<TemporaryFilesInObjectStorageCleaner>(object_storage_, object_storage_path_, temporary_files_);
}

TemporaryFileLookupPtr createTemporaryFilesLookup(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & input_temporary_files_, const Strings & output_temporary_files_)
{
    if (!object_storage_)
        return nullptr;

    return std::make_shared<TemporaryFilesInObjectStorage>(object_storage_, object_storage_path_, input_temporary_files_, output_temporary_files_);
}

ExchangeLookupPtr createExchangeLookup(
    const String & query_id,
    const std::unordered_map<String, ExchangeDescription> & exchanges_,
    const ExchangeStreamSources & exchange_stream_sources,
    TemporaryFileLookupPtr temporary_files_,
    ContextPtr context)
{
    bool run_locally = context->getSettingsRef()[Setting::distributed_plan_execute_locally];
    if (run_locally)
    {
        LOG_DEBUG(getLogger("createExchangeLookup"), "`distributed_plan_execute_locally` setting is enabled, using in-memory queues for all exchanges");
        return std::make_shared<ExchangeViaChunks>(query_id);
    }

    ExchangeLookupPtr streaming_exchanges;
#ifdef OS_LINUX
    auto streaming_exchange_port = context->getConfigRef().getUInt("distributed_query.streaming_exchange_port", 0);
    streaming_exchanges = streaming_exchange_port != 0 ?
        createStreamingExchangeLookup(query_id, ExchangeConnections::instance(), exchange_stream_sources, static_cast<UInt16>(streaming_exchange_port)) :
        std::make_shared<ExchangeViaChunks>(query_id);
#else
    UNUSED(exchanges_, exchange_stream_sources, context);
    streaming_exchanges = std::make_shared<ExchangeViaChunks>(query_id);
#endif
    auto persisted_exchanges = std::make_shared<ExchangeViaTemporaryFiles>(temporary_files_);

    return std::make_shared<AllKindsExchangeLookup>(exchanges_, persisted_exchanges, streaming_exchanges);
}


String serializeQueryPlan(const QueryPlan & query_plan)
{
    WriteBufferFromOwnString out;
    query_plan.serialize(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    return out.str();
}

QueryPlan deserializeQueryPlan(const String & serialized_query_plan, ContextPtr context)
{
    ReadBufferFromString in(serialized_query_plan);
    auto plan_and_sets = QueryPlan::deserialize(in, context);
    return QueryPlan::makeSets(std::move(plan_and_sets), context);
}

void doExecuteTask(const DistributedQueryTaskDescription & task_description, ObjectStoragePtr object_storage,
    const String & object_storage_path, ContextMutablePtr context, std::function<bool()> is_cancelled, ProgressCallback progress_callback)
{
    Stopwatch execute_task_watch;
    const auto & task = task_description.task;

    std::shared_ptr<OpenTelemetry::SpanHolder> query_span = std::make_shared<OpenTelemetry::SpanHolder>(task.task_id);

    auto logger = Poco::Logger::getShared("executeDistributedQuery");

    Strings input_exchange_streams;
    for (const auto & stream_id : task.input_exchange_streams)
        input_exchange_streams.push_back(stream_id.toString());

    Strings output_exchange_streams;
    for (const auto & stream_id : task.output_exchange_streams)
        output_exchange_streams.push_back(stream_id.toString());

    LOG_TRACE(logger, "Task '{}' input exchange streams: [{}], output exchange streams: [{}]",
        task.task_id, fmt::join(input_exchange_streams, ", "), fmt::join(output_exchange_streams, ", "));

    auto temporary_files = createTemporaryFilesLookup(
        object_storage, object_storage_path, input_exchange_streams, output_exchange_streams);

    auto pipeline_settings = BuildQueryPipelineSettings(context);
    pipeline_settings.temporary_file_lookup = temporary_files;
    pipeline_settings.parameter_lookup = std::make_shared<TaskParameters>(task.parameters);
    pipeline_settings.exchange_lookup = createExchangeLookup(
        object_storage_path,
        task_description.exchanges,
        task_description.exchange_stream_sources,
        temporary_files,
        context);

    auto optimization_settings = QueryPlanOptimizationSettings(context);

    QueryPipeline pipeline;

    {
        QueryPlan query_plan = deserializeQueryPlan(task_description.serialized_query_plan, context);

        auto builder = query_plan.buildQueryPipeline(
                optimization_settings,
                pipeline_settings);

        pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    }

    ASTPtr ast_stub = make_intrusive<ASTSelectQuery>(); /// FIXME: this is only used to populate query_kind
    UInt64 query_plan_hash = sipHash64(task_description.serialized_query_plan);

    auto query_log_elem = logQueryStart(
        std::chrono::system_clock::now(),
        context,
        /*query_for_logging*/ task.task_id,
        query_plan_hash,
        ast_stub, pipeline,
        /*interpreter*/ nullptr,
        /*internal*/ false,
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

        pipeline.setProgressCallback(progress_callback);

        CompletedPipelineExecutor executor(pipeline);
        if (is_cancelled)
            executor.setCancelCallback(is_cancelled, 100);
        executor.execute();

        logQueryFinish(query_log_elem, context, ast_stub, std::move(pipeline), false,
            query_span, QueryResultCacheUsage::None, false);
    }
    catch (...)
    {
        logQueryException(query_log_elem, context, execute_task_watch, ast_stub, query_span, false, true);
        throw;
    }
}

std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context)
{
    const auto & config = context->getConfigRef();
    String config_prefix = "distributed_query.temporary_files_storage";
    if (config.has(config_prefix))
    {
        ObjectStoragePtr object_storage = ObjectStorageFactory::instance().create("distributed_query_temp_files", config, config_prefix, context, false);

        String object_storage_path_prefix = config.getString("distributed_query.temporary_files_storage.endpoint_subpath");
        String object_storage_path = object_storage_path_prefix + unique_temp_file_path;

        return {object_storage, object_storage_path};
    }
    else
    {
        return {nullptr, unique_temp_file_path};
    }
}

void executeTask(const UUID & unique_query_id, const DistributedQueryTaskDescription & task, ContextPtr context, std::shared_ptr<std::atomic<bool>> is_cancelled)
{
    auto [object_storage, object_storage_path] = getObjectStorageForTemporaryFiles(toString(unique_query_id), context);

    doExecuteTask(task, object_storage, object_storage_path, Context::createCopy(context), [is_cancelled]() -> bool { return *is_cancelled; });
}

/// Runs tasks in local threads. Useful for testing and debugging.
class DistributedQueryPlanExecutorLocal final : public DistributedQueryPlanExecutor
{
public:
    DistributedQueryPlanExecutorLocal(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_, std::shared_ptr<std::atomic<bool>> is_cancelled_)
        : DistributedQueryPlanExecutor(unique_query_id_, distributed_query_plan_, makeContextForLocalExecution(context_), std::move(is_cancelled_))
    {
    }

    void cleanup() override
    {
        /// TODO: cancel all remaining tasks
    }

protected:
    static ContextPtr makeContextForLocalExecution(ContextPtr ctx)
    {
        auto new_context = Context::createCopy(ctx);
        /// We will execute tasks with local plan fragments. They should not be converted into distributed plan themselves.
        new_context->setSetting("make_distributed_plan", false);
        return new_context;
    }

    std::future<void> startTask(const DistributedQueryTaskDescription & task_description)
    {
        std::promise<void> task_promise;
        std::future<void> future = task_promise.get_future();

        std::thread([promise = std::move(task_promise), thread_group = CurrentThread::getGroup(), query_id = unique_query_id, task_description, ctx = context, is_cancelled = this->is_cancelled]() mutable
        {
            ThreadStatus thread_status;
            ThreadGroupSwitcher switcher(thread_group, ThreadName::DISTRIBUTED_QUERY_TASK);

            try
            {
                executeTask(query_id, task_description, ctx, is_cancelled);
                promise.set_value();
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        }).detach();

        return future;
    }

    void startStage(const String & stage_name, const DistributedQueryStage & stage) override
    {
        std::vector<std::future<void>> started_tasks;
        started_tasks.reserve(stage.tasks.size());
        DistributedQueryTaskDescription task_description;
        task_description.serialized_query_plan = serializeQueryPlan(stage.query_plan_fragment);
        task_description.exchanges = distributed_query_plan.exchange_descriptions; /// TODO: add only exchanges for this stage

        for (const auto & task : stage.tasks)
        {
            task_description.task = task;
            started_tasks.emplace_back(startTask(task_description));
        }

        stage_tasks[stage_name] = std::move(started_tasks);
    }

    bool waitForStage(const String & stage_name, std::optional<UInt64> /*timeout_ms*/) override
    {
        auto & started_tasks = stage_tasks[stage_name];

        /// TODO: periodically check for cancellation
        for (auto & task : started_tasks)
            task.wait();

        auto tasks = std::move(started_tasks);
        started_tasks.clear();

        /// Throw exception if any task failed
        for (auto & task : tasks)
            task.get(); /// TODO: add timeout

        return true;
    }

private:
    std::unordered_map<String, std::vector<std::future<void>>> stage_tasks;
};


TaskToHostMap::TaskToHostMap(const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_)
{
    fillHostnames(context_);
    assignHostsForTasks(distributed_query_plan_);
}

void TaskToHostMap::fillHostnames(ContextPtr context)
{
    if (!context->getConfigRef().getBool("stateless_worker_client.enabled", false))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Stateless worker client is not enabled in configuration");

    String host;
    String cluster_name = context->getConfigRef().getString("stateless_worker_client.cluster", "");
    if (!cluster_name.empty())
    {
        auto cluster = context->tryGetCluster(cluster_name);
        if (!cluster)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cluster '{}' not found", cluster_name);

        auto shard_addresses = cluster->getShardsAddresses();
        if (shard_addresses.empty())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cluster '{}' has no shards", cluster_name);
        for (const auto & shard : shard_addresses[0])
            hostnames.push_back(shard.host_name);
    }
    else
    {
        host = context->getConfigRef().getString("stateless_worker_client.host");
        if (!host.empty())
            hostnames.push_back(host);
    }

    if (hostnames.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "No hosts specified for stateless worker client");
}

void TaskToHostMap::assignHostsForTasks(const DistributedQueryPlan & distributed_query_plan)
{
    size_t current_host = 0;
    for (const auto & [stage_id, stage] : distributed_query_plan.stages)
    {
        for (const auto & task : stage.tasks)
        {
            const auto & assigned_host = hostnames[current_host];
            current_host = (current_host + 1) % hostnames.size();
            task_hosts[task.task_id] = assigned_host;
            for (const auto & output_stream : task.output_exchange_streams)
                exchange_stream_source_hosts[output_stream.toString()] = assigned_host;
        }
    }
}


/// Sends tasks to remote nodes.
class DistributedQueryPlanExecutorRemote final : public DistributedQueryPlanExecutor
{
public:
    DistributedQueryPlanExecutorRemote(
        const UUID & unique_query_id_,
        const DistributedQueryPlan & distributed_query_plan_,
        TaskToHostMapPtr task_to_host_map_,
        ContextPtr context_,
        std::shared_ptr<std::atomic<bool>> is_cancelled_)
        : DistributedQueryPlanExecutor(unique_query_id_, distributed_query_plan_, std::move(context_), std::move(is_cancelled_))
        , task_to_host_map(std::move(task_to_host_map_))
        , running_tasks(8, context, is_cancelled, logger)
    {
        QueryStatusPtr query_status = context->getProcessListElement();
        LOG_DEBUG(logger, "Hosts for running distributed query: [{}]", fmt::join(task_to_host_map->getHostnames(), ", "));
    }

    void cleanup() override
    {
        running_tasks.cancel();
    }

protected:
    struct RunningTaskInfo
    {
        String endpoint_uri;
        String task_id;
    };

    /// Tracks the statuses of running tasks in parallel, collects progress counters.
    class TaskTracker
    {
    public:
        TaskTracker(Int64 max_in_flight_requests_, ContextPtr context_, std::shared_ptr<std::atomic<bool>> is_cancelled_, LoggerPtr logger_)
            : context(std::move(context_))
            , query_status(context->getProcessListElement())
            , max_in_flight_requests(max_in_flight_requests_)
            , is_cancelled(std::move(is_cancelled_))
            , thread_pool(CurrentMetrics::TaskTrackerThreads, CurrentMetrics::TaskTrackerThreadsActive, CurrentMetrics::TaskTrackerThreadsScheduled,
                max_in_flight_requests, max_in_flight_requests, 2 * max_in_flight_requests)
            , logger(std::move(logger_))
        {}

        ~TaskTracker()
        {
            thread_pool.wait();
        }

        /// Add started task to be tracked
        void addTask(const String & stage_name, RunningTaskInfo task_info)
        {
            auto task_name = task_info.task_id;

            {
                std::lock_guard g(lock);
                stage_tasks[stage_name][task_name] = std::move(task_info);

                /// Create stage info if this stage was not known before
                if (!all_stages.contains(stage_name))
                    all_stages[stage_name] = std::make_shared<StageInfo>(stage_name);
                all_stages[stage_name]->started_tasks++;
            }

            addTaskToCheckQueue(stage_name, task_name);
            enqueueGetStatus();
        }

        /// Wait for all tasks of the stage to finish
        bool waitForStage(const String & stage_name, std::optional<UInt64> timeout_ms)
        {
            LOG_DEBUG(logger, "Waiting for stage {} to finish", stage_name);

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
                finished.wait();
                stage_finished = true;
            }
            checkCancelled();
            return stage_finished;
        }

        /// Cancel all unfinished tasks
        void cancel()
        {
            std::lock_guard g(lock);
            for (auto & [stage_name, started_tasks] : stage_tasks)
            {
                while (!started_tasks.empty())
                {
                    auto & task = started_tasks.begin()->second;
                    LOG_TRACE(logger, "Cancelling task {} on host {}", task.task_id, task.endpoint_uri);
                    try
                    {
                        cancelTask(task.endpoint_uri, task.task_id, context);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                    started_tasks.erase(started_tasks.begin());
                }
            }
        }

    private:
        void checkCancelled()
        {
            if (query_status)
                query_status->checkTimeLimit();

            if (*is_cancelled)
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
        }

        /// Thead function to check one task. If the task is not finished, adds the task back to the queue for checking.
        void checkStatusFunc(const String & stage_name, const RunningTaskInfo & task)
        {
            checkCancelled();

            UInt32 wait_milliseconds = 300;

            auto task_status = getTaskStatus(task.endpoint_uri, task.task_id, wait_milliseconds, context);

            auto progress_callback = context->getProgressCallback();
            if (progress_callback)
                progress_callback(task_status.progress);

            if (task_status.status == "Running")
            {
                /// Add the task back to the end of the queue
                addTaskToCheckQueue(stage_name, task.task_id);
            }
            else
            {
                String error_message;
                if (task_status.status != "Finished")
                    error_message += " Task " + task.task_id + " error: " + task_status.error_message + "\n";

                if (!error_message.empty())
                    throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "Failures: {}", error_message);

                /// Update task state
                setTaskFinished(stage_name, task.task_id);
            }

            /// Try to enqueue next check
            enqueueGetStatus();
        }

        void addTaskToCheckQueue(const String & stage_name, const String & task_name)
        {
            std::lock_guard g(lock);
            StageInfoPtr stage;
            stage = all_stages[stage_name];
            stage->tasks_to_check.push_back(task_name);

            /// If this stage didn't have any tasks to check before we added this task then the stage is not it the queue for checking,
            /// so we add it to the queue.
            if (stage->tasks_to_check.size() == 1)
                stages_to_check.push_back(stage);
        }

        void setTaskFinished(const String & stage_name, const String & task_name)
        {
            std::lock_guard g(lock);
            auto & stage = all_stages[stage_name];
            stage->finished_tasks++;

            /// Is somebody already waiting for the result?
            if (stage->finished_tasks == stage->started_tasks && stage_results.contains(stage_name))
                stage->promise.set_value();

            stage_tasks[stage_name].erase(task_name); // TODO: really need to erase?
        }

        /// Picks the next task from the queue.
        /// The queue contains stages and within each stage there is a queue of unfinished tasks.
        /// This allows to pick tasks from all stages and thus track progress of all stages in parallel.
        std::optional<std::pair<String, String>> getNextTaskToCheck() TSA_REQUIRES(lock)
        {
            if (stages_to_check.empty())
                return {};

            auto stage = stages_to_check.front();
            stages_to_check.pop_front();

            if (stage->tasks_to_check.empty())    /// TODO: should not happen, but let's be safe
                return {};

            auto task = stage->tasks_to_check.front();
            stage->tasks_to_check.pop_front();

            /// If there are more tasks to check in the stage then put the stage back to the end of the queue
            if (!stage->tasks_to_check.empty())
                stages_to_check.push_back(stage);

            return std::make_pair(stage->name, task);
        }

        void enqueueGetStatus()
        {
            std::lock_guard g(lock);

            if (in_flight_request_count >= max_in_flight_requests)
                return;

            /// Choose next task to check
            auto task = getNextTaskToCheck();
            if (!task)
                return;

            auto task_info = stage_tasks[task->first][task->second];

            thread_pool.scheduleOrThrow([this, task, task_info]()
                {
                    try
                    {
                        checkStatusFunc(task->first, task_info);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                    --in_flight_request_count;
                });
            ++in_flight_request_count;
        }

        struct StageInfo
        {
            const String name;
            std::deque<String> tasks_to_check;  /// Queue of the tasks from this stage that are not finished and are not being checked at the moment
            Int64 started_tasks = 0;
            Int64 finished_tasks = 0;
            std::promise<void> promise;
        };

        using StageInfoPtr = std::shared_ptr<StageInfo>;

        ContextPtr context;
        QueryStatusPtr query_status;

        const Int64 max_in_flight_requests;

        std::mutex lock;
        std::unordered_map<String, StageInfoPtr> all_stages TSA_GUARDED_BY(lock);
        std::unordered_map<String, std::map<String, RunningTaskInfo>> stage_tasks TSA_GUARDED_BY(lock);
        std::atomic<Int64> in_flight_request_count = 0;
        std::deque<StageInfoPtr> stages_to_check TSA_GUARDED_BY(lock);  /// Queue of stages that have unfinished tasks to be checked
        std::unordered_map<String, std::shared_future<void>> stage_results TSA_GUARDED_BY(lock);
        std::shared_ptr<std::atomic<bool>> is_cancelled;
        ThreadPool thread_pool;
        LoggerPtr logger;
    };

    RunningTaskInfo startTask(const DistributedQueryTaskDescription & task_description)
    {
        const String host = task_to_host_map->getTaskHosts().at(task_description.task.task_id);
        String stateless_worker_endpoint_uri;
        {
            auto default_port = context->getInterserverIOAddress().second;
            auto port = context->getConfigRef().getUInt("stateless_worker_client.port", default_port);
            String default_endpoint = context->getConfigRef().getString("stateless_worker_server.endpoint", "localhost");
            auto endpoint = context->getConfigRef().getString("stateless_worker_client.endpoint", "stateless_worker/" + default_endpoint);
            Poco::URI stateless_worker_uri;
            stateless_worker_uri.setScheme("http");
            stateless_worker_uri.setHost(host);
            stateless_worker_uri.setPort(static_cast<UInt16>(port));
            stateless_worker_uri.addQueryParameter("endpoint", endpoint);
            stateless_worker_endpoint_uri = stateless_worker_uri.toString();
        }

        String unique_task_id = toString(unique_query_id) + "::" + task_description.task.task_id;
        String unique_temp_file_path = toString(unique_query_id);

        LOG_DEBUG(logger, "Sending task {} to host {}", unique_task_id, host);

        sendTask(stateless_worker_endpoint_uri, unique_task_id, task_description, unique_temp_file_path, context);

        return {stateless_worker_endpoint_uri, unique_task_id};
    }

    void startStage(const String & stage_name, const DistributedQueryStage & stage) override
    {
        std::deque<RunningTaskInfo> started_tasks;
        DistributedQueryTaskDescription task_description;
        task_description.initial_query_id = context->getCurrentQueryId();
        task_description.serialized_query_plan = serializeQueryPlan(stage.query_plan_fragment);
        task_description.exchanges = distributed_query_plan.exchange_descriptions; /// TODO: add only exchanges for this stage

        for (const auto & task : stage.tasks)
        {
            checkCancelled();

            task_description.task = task;

            /// Add exchange destinations for output streams
            task_description.exchange_stream_sources = {};
            for (const auto & input_stream : task.input_exchange_streams)
            {
                String input_stream_name = input_stream.toString();
                task_description.exchange_stream_sources.stream_hosts[input_stream_name] = task_to_host_map->getExchangeStreamSourceHosts().at(input_stream_name);
            }

            running_tasks.addTask(stage_name, startTask(task_description));
        }
    }

    bool waitForStage(const String & stage_name, std::optional<UInt64> timeout_ms) override
    {
        return running_tasks.waitForStage(stage_name, timeout_ms);
    }

    TaskToHostMapPtr task_to_host_map;
    TaskTracker running_tasks;
};


DistributedQueryPlanExecutor::DistributedQueryPlanExecutor(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_, std::shared_ptr<std::atomic<bool>> is_cancelled_)
    : unique_query_id(unique_query_id_)
    , distributed_query_plan(distributed_query_plan_)
    , context(std::move(context_))
    , query_status(context->getProcessListElement())
    , is_cancelled(std::move(is_cancelled_))
    , logger(getLogger("DistributedQueryPlanExecutor"))
{
}

void DistributedQueryPlanExecutor::checkCancelled() const
{
    if (query_status)
        query_status->checkTimeLimit();

    if (*is_cancelled)
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
}

void DistributedQueryPlanExecutor::startStageWithDependencies(const String & stage_name, std::unordered_set<String> & executed_stages)
{
    if (executed_stages.contains(stage_name))
        return;

    if (distributed_query_plan.stage_depends_on.contains(stage_name))
    {
        Strings dependencies_to_wait;

        for (const auto & [dependency, exchange_id] : distributed_query_plan.stage_depends_on.at(stage_name))
        {
            startStageWithDependencies(dependency, executed_stages);

            /// If exchange data is persistent then we will need to wait for stage to finish
            if (distributed_query_plan.exchange_descriptions.at(exchange_id).kind == ExchangeDescription::Kind::Persisted)
                dependencies_to_wait.push_back(dependency);
        }

        for (const auto & dependency : dependencies_to_wait)
            waitForStage(dependency, std::nullopt);
    }

    const auto & stage = distributed_query_plan.stages.at(stage_name);
    LOG_DEBUG(logger,
        "\n====================== Executing stage '{}' =========================\n"
        "PLAN:\n{}\nTASKS: {}\n"
        "==========================================================================",
        stage_name, dumpQueryPlan(stage.query_plan_fragment), stage.tasks.size());
    startStage(stage_name, stage);
    executed_stages.insert(stage_name);
}

void DistributedQueryPlanExecutor::start()
{
    LOG_DEBUG(logger, "Starting distributed query, unique id: {}", toString(unique_query_id));

    /// Execute stages in topological order
    {
        std::unordered_set<String> executed_stages;
        for (const auto & [stage_name, _] : distributed_query_plan.stages)
            startStageWithDependencies(stage_name, executed_stages);
    }

    /// Wait for all stages to finish
    for (const auto & [stage_name, _] : distributed_query_plan.stages)
        running_stages.push_back(stage_name);
}

bool DistributedQueryPlanExecutor::execute()
{
    if (running_stages.empty())
        return true;

    auto & stage_name = running_stages.front();
    bool stage_finished = waitForStage(stage_name, 100);
    if (stage_finished)
    {
        LOG_DEBUG(logger, "Stage '{}' finished", stage_name);
        running_stages.pop_front();
    }

    return false;
}

std::unique_ptr<DistributedQueryPlanExecutor> createDistributedQueryExecutor(
    const UUID & unique_query_id,
    const DistributedQueryPlan & distributed_query_plan,
    TaskToHostMapPtr task_to_host_map,
    ContextPtr context,
    std::shared_ptr<std::atomic<bool>> is_cancelled)
{
    bool run_locally = context->getSettingsRef()[Setting::distributed_plan_execute_locally];
    std::unique_ptr<DistributedQueryPlanExecutor> executor;
    if (run_locally)
        executor = std::make_unique<DistributedQueryPlanExecutorLocal>(unique_query_id, distributed_query_plan, context, is_cancelled);
    else
        executor = std::make_unique<DistributedQueryPlanExecutorRemote>(unique_query_id, distributed_query_plan, task_to_host_map, context, is_cancelled);

    return executor;
}

}
