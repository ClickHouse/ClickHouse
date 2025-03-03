#include <memory>
#include <mutex>
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
#include <Common/logger_useful.h>


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
        LOG_DEBUG(logger, "Writing to temporary file {}", file_name);

        if (!output_temporary_files.contains(file_name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected output temporary file requested: {} ", file_name);
        StoredObject object(object_storage_path + "/" + file_name, file_name);
        write_buffers.emplace_back(object_storage->writeObject(object, WriteMode::Rewrite));
        return *write_buffers.back();
    }

    std::unique_ptr<ReadBuffer> getTemporaryFileForReading(const String & file_name) override
    {
        LOG_TRACE(logger, "Reading from temporary file {}", file_name);

        if (!input_temporary_files.contains(file_name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected input temporary file requested: {} ", file_name);
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

class ResultSink
{
public:
    virtual ~ResultSink() = default;
    virtual void addBlock(Block block) = 0;
};

void doExecuteTask(const String & serialized_query_plan, const DistributedQueryTask & task, ObjectStoragePtr object_storage, const String & object_storage_path, ResultSink & result, ContextPtr context)
{
    QueryPlan query_plan = deserializeQueryPlan(serialized_query_plan, context);

    auto logger = Poco::Logger::getShared("executeDistributedQuery");
    LOG_TRACE(logger, "Task '{}' temporary files:\ninput: {}\noutput: {}",
        task.task_id, fmt::join(task.input_temporary_files, ", "), fmt::join(task.output_temporary_files, ", "));

    auto temporary_files = std::make_shared<TemporaryFilesInObjectStorage>(
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

    if (pipeline.completed())
    {
        CompletedPipelineExecutor executor(pipeline);
        executor.execute();
    }
    else
    {
        PullingPipelineExecutor executor(pipeline);
        Block block;
        size_t block_num = 0;
        size_t rows_num = 0;

        while (executor.pull(block))
        {
            if (!block)
                continue;

            ++block_num;
            rows_num += block.rows();

            LOG_TEST(logger, "Block {}:\n{}", block_num, block.dumpStructure());

            result.addBlock(block);
        }

        LOG_DEBUG(logger, "Read {} blocks with {} rows from pipeline", block_num, rows_num);
    }
}

void executeTask(const String & serialized_query_plan, const DistributedQueryTask & task, ResultSink & result, ContextPtr context)
{
    bool use_local_object_storage = true;
    ObjectStoragePtr object_storage;
    String object_storage_path;
    if (use_local_object_storage)
//    {
//        object_storage_path = "/tmp";
//        object_storage = std::make_shared<LocalObjectStorage>(object_storage_path);
//    }
    {
        object_storage_path = "./local_object_storage/tmp";
        const auto & config = context->getConfigRef();
        String config_prefix = "storage_configuration.disks.local";
        object_storage = ObjectStorageFactory::instance().create("local", config, config_prefix, context, false);
    }
    else
    {
        object_storage_path = "/tmp";
        const auto & config = context->getConfigRef();
        String config_prefix = "storage_configuration.disks.s3_disk_for_stateless_task";
        object_storage = ObjectStorageFactory::instance().create("s3", config, config_prefix, context, false);
    }

    doExecuteTask(serialized_query_plan, task, object_storage, object_storage_path, result, context);
}

std::future<void> startTask(const QueryPlan & query_plan, const DistributedQueryTask & task, ResultSink & result, ContextPtr context)
{
    std::promise<void> task_promise;
    std::future<void> future = task_promise.get_future();

    const String serialized_query_plan = serializeQueryPlan(query_plan);

    std::thread([promise = std::move(task_promise), serialized_query_plan, task, &result, context]() mutable
    {
        try
        {
            executeTask(serialized_query_plan, task, result, context);
            promise.set_value();
        }
        catch (...)
        {
            promise.set_exception(std::current_exception());
        }
    }).detach();

    return future;
}

void executeStage(const DistributedQueryStage & stage, ResultSink & result, ContextPtr context)
{
    std::vector<std::future<void>> started_tasks;
    started_tasks.reserve(stage.tasks.size());
    for (const auto & task : stage.tasks)
        started_tasks.emplace_back(startTask(stage.query_plan_fragment, task, result, context));

    /// TODO: periodically check for cancellation
    for (const auto & task : started_tasks)
        task.wait();

    /// Throw exception if any task failed
    for (auto & task : started_tasks)
        task.get();
}

class StageResult : public ResultSink
{
public:
    void addBlock(Block block) override
    {
        std::lock_guard lock(mutex);
        blocks.push_back(std::move(block));
    }

    std::vector<Block> getBlocks()
    {
        std::lock_guard lock(mutex);
        return blocks;
    }

private:
    std::vector<Block> blocks;
    std::mutex mutex;
};

using StageResultPtr = std::shared_ptr<StageResult>;

Chunks executeDistributedQuery(const DistributedQueryPlan & distributed_query_plan, ContextPtr context)
{
    auto logger = Poco::Logger::getShared("executeDistributedQuery");
    /// Execute stages in topological order
    std::unordered_map<String, StageResultPtr> executed_stages;
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
        StageResultPtr stage_result = std::make_shared<StageResult>();
        executeStage(stage, *stage_result, context);
        executed_stages[stage_name] = stage_result;
    };

    for (const auto & [stage_name, _] : distributed_query_plan.stages)
        execute_stage(stage_name);

    Chunks result;
    auto main_result = executed_stages.at("main");
    for (const auto & block : main_result->getBlocks())
        result.push_back(Chunk(block.getColumns(), block.rows()));

    return result;
}

}
