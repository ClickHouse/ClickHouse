#include <future>
#include <memory>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/printPipeline.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/TemporaryFiles.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Planner/Utils.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Poco/URI.h>
#include <Common/logger_useful.h>
#include <Server/StatelessWorker/StatelessWorkerClient.h>


namespace DB
{

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

TemporaryFileLookupPtr createTemporaryFilesLookup(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & input_temporary_files_, const Strings & output_temporary_files_)
{
    return std::make_shared<TemporaryFilesInObjectStorage>(object_storage_, object_storage_path_, input_temporary_files_, output_temporary_files_);
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

void doExecuteTask(const String & serialized_query_plan, const DistributedQueryTask & task, ObjectStoragePtr object_storage, const String & object_storage_path, ContextPtr context)
{
    QueryPlan query_plan = deserializeQueryPlan(serialized_query_plan, context);

    auto logger = Poco::Logger::getShared("executeDistributedQuery");
    LOG_TRACE(logger, "Task '{}' temporary files:\ninput: {}\noutput: {}",
        task.task_id, fmt::join(task.input_temporary_files, ", "), fmt::join(task.output_temporary_files, ", "));

    auto temporary_files = createTemporaryFilesLookup(
        object_storage, object_storage_path, task.input_temporary_files, task.output_temporary_files);

    auto pipeline_settings = BuildQueryPipelineSettings(context);
    pipeline_settings.temporary_file_lookup = temporary_files;
    pipeline_settings.parameter_lookup = std::make_shared<TaskParameters>(task.parameters);

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

String sendTask(const String & endpoint_uri, const String & serialized_query_plan, const DistributedQueryTask & task, const ContextPtr & context);


std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(ContextPtr context)
{
    bool use_local_object_storage = true;
    ObjectStoragePtr object_storage;
    String object_storage_path;
    object_storage_path = "distributed_query_temporary_files";
    if (use_local_object_storage)
    {
        const auto & config = context->getConfigRef();
        String config_prefix = "storage_configuration.disks.local";
        object_storage = ObjectStorageFactory::instance().create("local", config, config_prefix, context, false);
    }
    else
    {
        const auto & config = context->getConfigRef();
        String config_prefix = "storage_configuration.disks.s3_disk_for_stateless_task";
        object_storage = ObjectStorageFactory::instance().create("s3", config, config_prefix, context, false);
    }

    return {object_storage, object_storage_path};
}

void executeTask(const String & serialized_query_plan, const DistributedQueryTask & task, ContextPtr context)
{
    auto [object_storage, object_storage_path] = getObjectStorageForTemporaryFiles(context);

    doExecuteTask(serialized_query_plan, task, object_storage, object_storage_path, context);
}

std::future<void> startTask(const QueryPlan & query_plan, const DistributedQueryTask & task, ContextPtr context)
{
    const String serialized_query_plan = serializeQueryPlan(query_plan);

#if 1
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
    sendTask(stateless_worker_endpoint_uri, serialized_query_plan, task, context);

    /// TODO: add task to the list of running tasks and check its status periodically
    while (true)
    {
        auto task_status = getTaskStatus(stateless_worker_endpoint_uri, task.task_id, 1000, context);

        if (task_status == "Finished\n")
        {
            /// Return ready future
            std::promise<void> task_promise;
            task_promise.set_value();
            return task_promise.get_future();
        }
        else if (task_status == "Running\n")
            continue;
        else
            break;
    }
#endif


    std::promise<void> task_promise;
    std::future<void> future = task_promise.get_future();

    std::thread([promise = std::move(task_promise), serialized_query_plan, task, context]() mutable
    {
        try
        {
            executeTask(serialized_query_plan, task, context);
            promise.set_value();
        }
        catch (...)
        {
            promise.set_exception(std::current_exception());
        }
    }).detach();

    return future;
}

void executeStage(const DistributedQueryStage & stage, ContextPtr context)
{
    std::vector<std::future<void>> started_tasks;
    started_tasks.reserve(stage.tasks.size());
    for (const auto & task : stage.tasks)
        started_tasks.emplace_back(startTask(stage.query_plan_fragment, task, context));

    /// TODO: periodically check for cancellation
    for (const auto & task : started_tasks)
        task.wait();

    /// Throw exception if any task failed
    for (auto & task : started_tasks)
        task.get();
}

void executeDistributedQuery(const DistributedQueryPlan & distributed_query_plan, ContextPtr context)
{
    auto logger = Poco::Logger::getShared("executeDistributedQuery");
    /// Execute stages in topological order
    std::unordered_set<String> executed_stages;
    std::function<void(const String &)> execute_stage = [&](const String & stage_name)
    {
        if (executed_stages.contains(stage_name))
            return;

        if (distributed_query_plan.stage_depends_on.contains(stage_name))
        {
            for (const auto & dependency : distributed_query_plan.stage_depends_on.at(stage_name))
                execute_stage(dependency);
        }

        const auto & stage = distributed_query_plan.stages.at(stage_name);
        LOG_DEBUG(logger,
            "\n====================== Executing stage '{}' =========================\n"
            "plan:\n{}\n"
            "==========================================================================",
            stage_name, dumpQueryPlan(stage.query_plan_fragment));
        executeStage(stage, context);
        executed_stages.insert(stage_name);
    };

    for (const auto & [stage_name, _] : distributed_query_plan.stages)
        execute_stage(stage_name);
}

}
