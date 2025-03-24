#include <condition_variable>
#include <future>
#include <memory>
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
#include <Interpreters/Context.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include "Core/Types.h"
#include <Core/Settings.h>


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
}

class TaskParameters : public IParameterLookup
{
public:
    explicit TaskParameters(const QueryPlanParamaters & parameters_)
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
    const QueryPlanParamaters parameters;
};


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

private:
    using InMemoryExchangeMap = std::unordered_map<String, InMemoryExchangePtr>;

    std::unordered_map<String, InMemoryExchangeMap> exchanges_by_query_id TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

InMemoryExchanges in_memory_exchanges;


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
        auto exchange = in_memory_exchanges.getExchange(query_id, file_name);
        return std::make_shared<SinkFromInMemoryExchange>(input_header, exchange);
    }

    std::shared_ptr<ISource> createSource(const Header & output_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) override
    {
        auto file_name = streamNameForExchange(exchange_id, source_bucket_id, destination_bucket_id);
        auto exchange = in_memory_exchanges.getExchange(query_id, file_name);
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


/// A wrapper that looks up exchanges by their kind and delegates to the corresponding exchange lookup: Perssitent or Streaming
class AllKinkdsExchangeLookup : public IExchangeLookup
{
public:
    AllKinkdsExchangeLookup(
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

protected:
    DistributedQueryPlanExecutor(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_)
        : unique_query_id(unique_query_id_)
        , distributed_query_plan(distributed_query_plan_)
        , context(std::move(context_))
    {
    }

    virtual void startStage(const String & stage_name, const DistributedQueryStage & stage) = 0;
    virtual void waitForStage(const String & stage_name) = 0;

    const UUID unique_query_id;
    const DistributedQueryPlan & distributed_query_plan;
    ContextPtr context;
    LoggerPtr logger = getLogger("DistributedQueryPlanExecutor");
};


TemporaryFileLookupPtr createTemporaryFilesLookup(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & input_temporary_files_, const Strings & output_temporary_files_)
{
    return std::make_shared<TemporaryFilesInObjectStorage>(object_storage_, object_storage_path_, input_temporary_files_, output_temporary_files_);
}

ExchangeLookupPtr createExchangeLookup(const String & query_id, const std::unordered_map<String, ExchangeDescription> & exchanges_, TemporaryFileLookupPtr temporary_files_)
{
    auto persisted_exchanges = std::make_shared<ExchangeViaTemporaryFiles>(temporary_files_);
    auto streaming_exchanges = std::make_shared<ExchangeViaChunks>(query_id);
    return std::make_shared<AllKinkdsExchangeLookup>(exchanges_, persisted_exchanges, streaming_exchanges); 
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

void doExecuteTask(const DistributedQueryTaskDescription & task_description, ObjectStoragePtr object_storage, const String & object_storage_path, ContextPtr context)
{
    const auto & task = task_description.task;

    QueryPlan query_plan = deserializeQueryPlan(task_description.serialized_query_plan, context);

    auto logger = Poco::Logger::getShared("executeDistributedQuery");
    LOG_TRACE(logger, "Task '{}' exchange streams:\ninput: {}\noutput: {}",
        task.task_id, fmt::join(task.input_exchange_streams, ", "), fmt::join(task.output_exchange_streams, ", "));

    auto temporary_files = createTemporaryFilesLookup(
        object_storage, object_storage_path, task.input_exchange_streams, task.output_exchange_streams);

    auto pipeline_settings = BuildQueryPipelineSettings(context);
    pipeline_settings.temporary_file_lookup = temporary_files;
    pipeline_settings.parameter_lookup = std::make_shared<TaskParameters>(task.parameters);
    pipeline_settings.exchange_lookup = createExchangeLookup(object_storage_path, task_description.exchanges, temporary_files);

    auto optimization_settings = QueryPlanOptimizationSettings(context);

    auto builder = query_plan.buildQueryPipeline(
        optimization_settings,
        pipeline_settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

    {
        WriteBufferFromOwnString out;
        printPipeline(pipeline.getProcessors(), out);
        LOG_DEBUG(logger, "Executing task '{}', pipeline:\n{}", task.task_id, out.str());
    }

    if (!pipeline.completed())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline is not completed");

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
}

std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context)
{
    const auto & config = context->getConfigRef();
    String config_prefix = "distributed_query.temporary_files_storage";
    ObjectStoragePtr object_storage = ObjectStorageFactory::instance().create("distributed_query_temp_files", config, config_prefix, context, false);

    String object_storage_path_prefix = context->getConfigRef().getString("distributed_query.temporary_files_storage.endpoint_subpath");
    String object_storage_path = object_storage_path_prefix + unique_temp_file_path;

    return {object_storage, object_storage_path};
}

void executeTask(const UUID & unique_query_id, const DistributedQueryTaskDescription & task, ContextPtr context)
{
    auto [object_storage, object_storage_path] = getObjectStorageForTemporaryFiles(toString(unique_query_id), context);

    doExecuteTask(task, object_storage, object_storage_path, context);
}

/// Runs tasks in local threads. Useful for testing and debugging.
class DistributedQueryPlanExecutorLocal final : public DistributedQueryPlanExecutor
{
public:
    DistributedQueryPlanExecutorLocal(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_)
        : DistributedQueryPlanExecutor(unique_query_id_, distributed_query_plan_, makeContextForLocalExecution(context_))
    {
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

        std::thread([promise = std::move(task_promise), query_id = unique_query_id, task_description, ctx = context]() mutable
        {
            try
            {
                executeTask(query_id, task_description, ctx);
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
    DistributedQueryPlanExecutorRemote(const UUID & unique_query_id_, const DistributedQueryPlan & distributed_query_plan_, ContextPtr context_)
        : DistributedQueryPlanExecutor(unique_query_id_, distributed_query_plan_, std::move(context_))
    {
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

    struct RunningTaskInfo
    {
        String endpoint_uri;
        String task_id;
    };

    RunningTaskInfo startTask(const DistributedQueryTaskDescription & task_description)
    {
        String host = hostnames[current_host];
        current_host = (current_host + 1) % hostnames.size();
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

    void startStage(const String & start_name, const DistributedQueryStage & stage) override
    {
        std::deque<RunningTaskInfo> started_tasks;
        DistributedQueryTaskDescription task_description;
        task_description.serialized_query_plan = serializeQueryPlan(stage.query_plan_fragment);
        task_description.exchanges = distributed_query_plan.exchange_descriptions; /// TODO: add only exchanges for this stage
        for (const auto & task : stage.tasks)
        {
            task_description.task = task;
            started_tasks.emplace_back(startTask(task_description));
        }

        stage_tasks[start_name] = std::move(started_tasks);
    }

    void waitForStage(const String & stage_name) override
    {
        auto & started_tasks = stage_tasks[stage_name];

        /// TODO: periodically check for cancellation
        String error_message;
        while (!started_tasks.empty())
        {
            auto & task = started_tasks.front();
            auto task_status = getTaskStatus(task.endpoint_uri, task.task_id, 1000, context);

            if (task_status == "Running\n")
                continue;

            started_tasks.pop_front();
            if (task_status != "Finished\n")
                error_message += " Task " + task.task_id + " error: " + task_status;
        }

        if (!error_message.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failures: {}", error_message);
    }

    std::unordered_map<String, std::deque<RunningTaskInfo>> stage_tasks;
    Strings hostnames;
    size_t current_host = 0;    /// Chose host in round-robin manner
};

void DistributedQueryPlanExecutor::execute()
{
    LOG_DEBUG(logger, "Executing distributed query, unique id: {}", toString(unique_query_id));

    /// Execute stages in topological order
    std::unordered_set<String> executed_stages;
    std::function<void(const String &)> start_stage = [&](const String & stage_name)
    {
        if (executed_stages.contains(stage_name))
            return;

        if (distributed_query_plan.stage_depends_on.contains(stage_name))
        {
            Strings dependencies_to_wait;

            for (const auto & [dependency, exchange_id] : distributed_query_plan.stage_depends_on.at(stage_name))
            {
                start_stage(dependency);

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
    };

    for (const auto & [stage_name, _] : distributed_query_plan.stages)
        start_stage(stage_name);

    /// Wait for all stages to finish
    for (const auto & [stage_name, _] : distributed_query_plan.stages)
        waitForStage(stage_name);
}

void executeDistributedQuery(const UUID & unique_query_id, const DistributedQueryPlan & distributed_query_plan, ContextPtr context)
{
    bool run_locally = context->getSettingsRef()[Setting::execute_distributed_plan_locally];
    std::unique_ptr<DistributedQueryPlanExecutor> executor;
    if (run_locally)
        executor = std::make_unique<DistributedQueryPlanExecutorLocal>(unique_query_id, distributed_query_plan, context);
    else
        executor = std::make_unique<DistributedQueryPlanExecutorRemote>(unique_query_id, distributed_query_plan, context);

    executor->execute();
}

}
