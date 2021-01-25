#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Executors/ExecutingGraph.h>
#include <Processors/Executors/ExecutorTasks.h>
#include <Common/EventCounter.h>
#include <common/logger_useful.h>

#include <queue>
#include <stack>
#include <mutex>

namespace DB
{

class QueryStatus;
class ExecutingGraph;
using ExecutingGraphPtr = std::unique_ptr<ExecutingGraph>;

/// Executes query pipeline.
class PipelineExecutor
{
public:
    /// Get pipeline as a set of processors.
    /// Processors should represent full graph. All ports must be connected, all connected nodes are mentioned in set.
    /// Executor doesn't own processors, just stores reference.
    /// During pipeline execution new processors can appear. They will be added to existing set.
    ///
    /// Explicit graph representation is built in constructor. Throws if graph is not correct.
    explicit PipelineExecutor(Processors & processors_, QueryStatus * elem = nullptr);

    /// Execute pipeline in multiple threads. Must be called once.
    /// In case of exception during execution throws any occurred.
    void execute(size_t num_threads);

    /// Execute single step. Step will be stopped when yield_flag is true.
    /// Execution is happened in a single thread.
    /// Return true if execution should be continued.
    bool executeStep(std::atomic_bool * yield_flag = nullptr);

    const Processors & getProcessors() const { return processors; }

    /// Cancel execution. May be called from another thread.
    void cancel();

private:
    Processors & processors;
    std::mutex processors_mutex;

    ExecutingGraphPtr graph;

    ExecutorTasks tasks;
    using Stack = std::stack<UInt64>;

    /// Flag that checks that initializeExecution was called.
    bool is_execution_initialized = false;

    std::atomic_bool cancelled = false;

    Poco::Logger * log = &Poco::Logger::get("PipelineExecutor");

    /// Now it's used to check if query was killed.
    QueryStatus * process_list_element = nullptr;

    /// Graph related methods.
    bool expandPipeline(Stack & stack, UInt64 pid);

    using Queue = std::queue<ExecutingGraph::Node *>;

    /// Pipeline execution related methods.
    void addChildlessProcessorsToStack(Stack & stack);
    bool tryAddProcessorToStackIfUpdated(ExecutingGraph::Edge & edge, Queue & queue, Queue & async_queue, ExecutionThreadContext & thread_context);

    /// Prepare processor with pid number.
    /// Check parents and children of current processor and push them to stacks if they also need to be prepared.
    /// If processor wants to be expanded, ExpandPipelineTask from thread_number's execution context will be used.
    bool prepareProcessor(UInt64 pid, ExecutionThreadContext & thread_context, Queue & queue, Queue & async_queue, std::unique_lock<std::mutex> node_lock);

    void initializeExecution(size_t num_threads); /// Initialize executor contexts and task_queue.
    void finalizeExecution(); /// Check all processors are finished.

    /// Methods connected to execution.
    void executeImpl(size_t num_threads);
    void executeStepImpl(size_t thread_num, std::atomic_bool * yield_flag = nullptr);
    void executeSingleThread(size_t thread_num);
    void finish();

    String dumpPipeline() const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
