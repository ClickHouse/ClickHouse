#include <condition_variable>
#include <future>
#include <memory>
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
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteBufferFromString.h>
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
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <base/getFQDNOrHostName.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool execute_distributed_plan_locally;
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

    std::shared_ptr<ISink> createSink(const Header & input_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) override
    {
        auto file_name = streamNameForExchange(exchange_id, source_bucket_id, destination_bucket_id);
        return std::make_shared<NativeCompressedSink>(input_header, temporary_files->getTemporaryFileForWriting(file_name));
    }

    std::shared_ptr<ISource> createSource(const Header & output_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) override
    {
        auto file_name = streamNameForExchange(exchange_id, source_bucket_id, destination_bucket_id);
        std::unique_ptr<QueryPipelineBuilder> pipeline_ptr = std::make_unique<QueryPipelineBuilder>();
        return std::make_shared<NativeCompressedSource>(output_header, temporary_files->getTemporaryFileForReading(file_name));
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
        Chunk chunk;
        {
            std::unique_lock lock(mutex);
            has_data.wait(lock, [this] { return !chunks.empty(); });
            chunk = std::move(chunks.front());
            chunks.pop_front();
        }

        LOG_TEST(log, "Getting chunk from exchange '{}', rows {}", name, chunk.getNumRows());

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

    std::shared_ptr<ISink> createSink(const Header & input_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) override
    {
        auto file_name = streamNameForExchange(exchange_id, source_bucket_id, destination_bucket_id);
        auto exchange = InMemoryExchanges::instance()->getExchange(query_id, file_name);
        return std::make_shared<SinkFromInMemoryExchange>(input_header, exchange);
    }

    std::shared_ptr<ISource> createSource(const Header & output_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) override
    {
        auto file_name = streamNameForExchange(exchange_id, source_bucket_id, destination_bucket_id);
        auto exchange = InMemoryExchanges::instance()->getExchange(query_id, file_name);
        return std::make_shared<SourceFromInMemoryExchange>(output_header, exchange);
    }

private:
    class SinkFromInMemoryExchange final : public ISink
    {
    public:
        SinkFromInMemoryExchange(const Header & header_, InMemoryExchangePtr exchange_)
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
        SourceFromInMemoryExchange(const Header & header_, InMemoryExchangePtr exchange_)
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

    std::shared_ptr<ISink> createSink(const Header & input_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) override
    {
        auto it = exchanges.find(exchange_id);
        if (it == exchanges.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange '{}'", exchange_id);

        if (it->second.kind == ExchangeDescription::Kind::Persisted)
            return persistent_exchange_lookup->createSink(input_header, exchange_id, source_bucket_id, destination_bucket_id);
        else if (it->second.kind == ExchangeDescription::Kind::Streaming)
            return streaming_exchange_lookup->createSink(input_header, exchange_id, source_bucket_id, destination_bucket_id);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange kind '{}'", static_cast<int>(it->second.kind));
    }

    std::shared_ptr<ISource> createSource(const Header & output_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) override
    {
        auto it = exchanges.find(exchange_id);
        if (it == exchanges.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange '{}'", exchange_id);

        if (it->second.kind == ExchangeDescription::Kind::Persisted)
            return persistent_exchange_lookup->createSource(output_header, exchange_id, source_bucket_id, destination_bucket_id);
        else if (it->second.kind == ExchangeDescription::Kind::Streaming)
            return streaming_exchange_lookup->createSource(output_header, exchange_id, source_bucket_id, destination_bucket_id);
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


/// Implements distributed query plan execution logic by executing stages according to dependencies between them.
class DistributedQueryPlanExecutor
{
public:
    virtual ~DistributedQueryPlanExecutor() = default;

    void execute();

    virtual void cleanup() = 0;

private:
    void startStageWithDependencies(const String & stage_name, std::unordered_set<String> & executed_stages);

protected:
    DistributedQueryPlanExecutor(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_, std::shared_ptr<std::atomic<bool>> is_cancelled_)
        : unique_query_id(unique_query_id_)
        , distributed_query_plan(distributed_query_plan_)
        , context(std::move(context_))
        , query_status(context->getProcessListElement())
        , is_cancelled(std::move(is_cancelled_))
    {
    }

    virtual void startStage(const String & stage_name, const DistributedQueryStage & stage) = 0;
    virtual void waitForStage(const String & stage_name) = 0;

    void checkCancelled() const
    {
        if (query_status)
            query_status->checkTimeLimit();

        if (*is_cancelled)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
    }

    const UUID unique_query_id;
    const DistributedQueryPlan & distributed_query_plan;
    ContextPtr context;
    QueryStatusPtr query_status;
    std::shared_ptr<std::atomic<bool>> is_cancelled;
    LoggerPtr logger = getLogger("DistributedQueryPlanExecutor");
};


TemporaryFileLookupPtr createTemporaryFilesLookup(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & input_temporary_files_, const Strings & output_temporary_files_)
{
    return std::make_shared<TemporaryFilesInObjectStorage>(object_storage_, object_storage_path_, input_temporary_files_, output_temporary_files_);
}

ExchangeLookupPtr createExchangeLookup(
    const String & query_id,
    const std::unordered_map<String, ExchangeDescription> & exchanges_,
    const ExchangeStreamDestinations & exchange_stream_destinations,
    TemporaryFileLookupPtr temporary_files_,
    ContextPtr context)
{
    auto persisted_exchanges = std::make_shared<ExchangeViaTemporaryFiles>(temporary_files_);

    auto streaming_exchange_port = context->getConfigRef().getUInt("distributed_query.streaming_exchange_port", 0);
    auto streaming_exchanges = streaming_exchange_port != 0 ?
        createStreamingExchangeLookup(query_id, ExchangeConnections::instance(), exchange_stream_destinations, streaming_exchange_port) :
        std::make_shared<ExchangeViaChunks>(query_id);

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
    return QueryPlan::deserialize(in, context).plan;
}

void doExecuteTask(const DistributedQueryTaskDescription & task_description, ObjectStoragePtr object_storage,
    const String & object_storage_path, ContextMutablePtr context, std::function<bool()> is_cancelled, ProgressCallback progress_callback)
{
    Stopwatch execute_task_watch;
    const auto & task = task_description.task;

    std::shared_ptr<OpenTelemetry::SpanHolder> query_span = std::make_shared<OpenTelemetry::SpanHolder>(task.task_id);

    auto logger = Poco::Logger::getShared("executeDistributedQuery");

    QueryPlan query_plan = deserializeQueryPlan(task_description.serialized_query_plan, context);

    LOG_TRACE(logger, "Task '{}' input exchange streams: [{}], output exchange streams: [{}]",
        task.task_id, fmt::join(task.input_exchange_streams, ", "), fmt::join(task.output_exchange_streams, ", "));

    auto temporary_files = createTemporaryFilesLookup(
        object_storage, object_storage_path, task.input_exchange_streams, task.output_exchange_streams);

    auto pipeline_settings = BuildQueryPipelineSettings(context);
    pipeline_settings.temporary_file_lookup = temporary_files;
    pipeline_settings.parameter_lookup = std::make_shared<TaskParameters>(task.parameters);
    pipeline_settings.exchange_lookup = createExchangeLookup(
        object_storage_path,
        task_description.exchanges,
        task_description.exchange_stream_destinations,
        temporary_files,
        context);

    auto optimization_settings = QueryPlanOptimizationSettings(context);

    auto builder = query_plan.buildQueryPipeline(
        optimization_settings,
        pipeline_settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

    ASTPtr ast_stub = std::make_shared<ASTSelectQuery>(); /// FIXME: this is only used to populate query_kind
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

        logQueryFinish(query_log_elem, context, ast_stub, pipeline, false,
            query_span, QueryResultCacheUsage::None, false);
    }
    catch (...)
    {
        logQueryException(query_log_elem, context, execute_task_watch, ast_stub, query_span, false, true);
        throw;
    }

    if (!pipeline.completed())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline is not completed");

    pipeline.setProcessListElement(context->getProcessListElement());

    CompletedPipelineExecutor executor(pipeline);
    if (is_cancelled)
        executor.setCancelCallback(is_cancelled, 100);
    executor.execute();
}

std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context)
{
    const auto & config = context->getConfigRef();
    String config_prefix = "distributed_query.temporary_files_storage";
    ObjectStoragePtr object_storage = ObjectStorageFactory::instance().create("distributed_query_temp_files", config, config_prefix, context, false);

    String object_storage_path_prefix = config.getString("distributed_query.temporary_files_storage.endpoint_subpath");
    String object_storage_path = object_storage_path_prefix + unique_temp_file_path;

    return {object_storage, object_storage_path};
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

        std::thread([promise = std::move(task_promise), query_id = unique_query_id, task_description, ctx = context, is_cancelled = this->is_cancelled]() mutable
        {
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

    void waitForStage(const String & stage_name) override
    {
        auto & started_tasks = stage_tasks[stage_name];

        /// TODO: periodically check for cancellation
        for (auto & task : started_tasks)
            task.wait();

        auto tasks = std::move(started_tasks);
        started_tasks.clear();

        /// Throw exception if any task failed
        for (auto & task : tasks)
            task.get();
    }

private:
    std::unordered_map<String, std::vector<std::future<void>>> stage_tasks;
};


/// Sends tasks to remote nodes.
class DistributedQueryPlanExecutorRemote final : public DistributedQueryPlanExecutor
{
public:
    DistributedQueryPlanExecutorRemote(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_, std::shared_ptr<std::atomic<bool>> is_cancelled_)
        : DistributedQueryPlanExecutor(unique_query_id_, distributed_query_plan_, std::move(context_), std::move(is_cancelled_))
    {
        QueryStatusPtr query_status = context->getProcessListElement();

        fillHostnames();
        assignHostsForTasks();
    }

    void cleanup() override
    {
        for (auto & [stage_name, started_tasks] : stage_tasks)
        {
            while (!started_tasks.empty())
            {
                auto & task = started_tasks.front();
                LOG_TRACE(logger, "Cancelling task {} on host {}", task.task_id, task.endpoint_uri);
                try
                {
                    cancelTask(task.endpoint_uri, task.task_id, context);
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
                started_tasks.pop_front();
            }
        }
    }

protected:
    void fillHostnames()
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

        LOG_DEBUG(logger, "Hosts for running distributed query: [{}]", fmt::join(hostnames, ", "));
    }

    void assignHostsForTasks()
    {
        size_t current_host = 0;
        for (const auto & [stage_id, stage] : distributed_query_plan.stages)
        {
            for (const auto & task : stage.tasks)
            {
                const auto & assigned_host = hostnames[current_host];
                current_host = (current_host + 1) % hostnames.size();
                task_hosts[task.task_id] = assigned_host;
                for (const auto & input_stream : task.input_exchange_streams)
                    exchange_stream_destination_hosts[input_stream] = assigned_host;
            }
        }

        exchange_stream_destination_hosts[distributed_query_plan.final_result_stream_name] = getFQDNOrHostName();
    }

    struct RunningTaskInfo
    {
        String endpoint_uri;
        String task_id;
    };

    RunningTaskInfo startTask(const DistributedQueryTaskDescription & task_description)
    {
        const String host = task_hosts.at(task_description.task.task_id);
        String stateless_worker_endpoint_uri;
        if (context->getConfigRef().getBool("stateless_worker_client.enabled", true))
        {
            String host = context->getConfigRef().getString("stateless_worker_client.host", "localhost");
            auto default_port = context->getInterserverIOAddress().second;
            auto port = context->getConfigRef().getUInt("stateless_worker_client.port", default_port);
            String default_endpoint = context->getConfigRef().getString("stateless_worker_server.endpoint", "localhost");
            auto endpoint = context->getConfigRef().getString("stateless_worker_client.endpoint", "stateless_worker/" + default_endpoint);
            Poco::URI stateless_worker_uri;
            stateless_worker_uri.setScheme("http");
            stateless_worker_uri.setHost(host);
            stateless_worker_uri.setPort(port);
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
            task_description.exchange_stream_destinations = {};
            for (const auto & output_stream : task.output_exchange_streams)
                task_description.exchange_stream_destinations.stream_hosts[output_stream] = exchange_stream_destination_hosts.at(output_stream);

            started_tasks.emplace_back(startTask(task_description));
        }

        stage_tasks[stage_name] = std::move(started_tasks);
    }

    void waitForStage(const String & stage_name) override
    {
        auto & started_tasks = stage_tasks[stage_name];

        String error_message;
        while (!started_tasks.empty())
        {
            checkCancelled();

            auto & task = started_tasks.front();
            auto task_status = getTaskStatus(task.endpoint_uri, task.task_id, 1000, context);

            auto progress_callback = context->getProgressCallback();
            if (progress_callback)
                progress_callback(task_status.progress);

            if (task_status.status == "Running")
                continue;

            if (task_status.status != "Finished")
                error_message += " Task " + task.task_id + " error: " + task_status.error_message + "\n";

            started_tasks.pop_front();
        }

        if (!error_message.empty())
            throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "Failures: {}", error_message);
    }

    std::unordered_map<String, std::deque<RunningTaskInfo>> stage_tasks;
    Strings hostnames;
    std::unordered_map<String, String> task_hosts;
    std::unordered_map<String, String> exchange_stream_destination_hosts;
};

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
            waitForStage(dependency);
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

void DistributedQueryPlanExecutor::execute()
{
    LOG_DEBUG(logger, "Executing distributed query, unique id: {}", toString(unique_query_id));

    /// Execute stages in topological order
    {
        std::unordered_set<String> executed_stages;
        for (const auto & [stage_name, _] : distributed_query_plan.stages)
            startStageWithDependencies(stage_name, executed_stages);
    }

    /// Wait for all stages to finish
    for (const auto & [stage_name, _] : distributed_query_plan.stages)
        waitForStage(stage_name);
}

void executeDistributedQuery(const UUID & unique_query_id, const DistributedQueryPlan & distributed_query_plan, ContextPtr context, std::shared_ptr<std::atomic<bool>> is_cancelled)
{
    bool run_locally = context->getSettingsRef()[Setting::execute_distributed_plan_locally];
    std::unique_ptr<DistributedQueryPlanExecutor> executor;
    if (run_locally)
        executor = std::make_unique<DistributedQueryPlanExecutorLocal>(unique_query_id, distributed_query_plan, context, is_cancelled);
    else
        executor = std::make_unique<DistributedQueryPlanExecutorRemote>(unique_query_id, distributed_query_plan, context, is_cancelled);

    try
    {
        executor->execute();
        executor->cleanup();
    }
    catch (...)
    {
        executor->cleanup();
        throw;
    }
}

}
