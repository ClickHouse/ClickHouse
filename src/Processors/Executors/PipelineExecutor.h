#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Executors/ExecutorTasks.h>
#include <Common/EventCounter.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/ConcurrencyControl.h>

#include <queue>
#include <mutex>

namespace DB
{

class QueryStatus;
class ExecutingGraph;
using ExecutingGraphPtr = std::unique_ptr<ExecutingGraph>;

class ReadProgressCallback;
using ReadProgressCallbackPtr = std::unique_ptr<ReadProgressCallback>;

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
    explicit PipelineExecutor(Processors & processors, QueryStatus * elem);
    ~PipelineExecutor();

    /// Execute pipeline in multiple threads. Must be called once.
    /// In case of exception during execution throws any occurred.
    void execute(size_t num_threads);

    /// Execute single step. Step will be stopped when yield_flag is true.
    /// Execution is happened in a single thread.
    /// Return true if execution should be continued.
    bool executeStep(std::atomic_bool * yield_flag = nullptr);

    const Processors & getProcessors() const;

    /// Cancel execution. May be called from another thread.
    void cancel();

    /// Checks the query time limits (cancelled or timeout). Throws on cancellation or when time limit is reached and the query uses "break"
    bool checkTimeLimit();
    /// Same as checkTimeLimit but it never throws. It returns false on cancellation or time limit reached
    [[nodiscard]] bool checkTimeLimitSoft();

    /// Set callback for read progress.
    /// It would be called every time when processor reports read progress.
    void setReadProgressCallback(ReadProgressCallbackPtr callback);

private:
    ExecutingGraphPtr graph;

    ExecutorTasks tasks;

    // Concurrency control related
    ConcurrencyControl::AllocationPtr slots;
    ConcurrencyControl::SlotPtr single_thread_slot; // slot for single-thread mode to work using executeStep()
    std::mutex threads_mutex;
    std::vector<ThreadFromGlobalPool> threads;

    /// Flag that checks that initializeExecution was called.
    bool is_execution_initialized = false;
    /// system.processors_profile_log
    bool profile_processors = false;
    /// system.opentelemetry_span_log
    bool trace_processors = false;

    std::atomic_bool cancelled = false;

    Poco::Logger * log = &Poco::Logger::get("PipelineExecutor");

    /// Now it's used to check if query was killed.
    QueryStatus * const process_list_element = nullptr;

    ReadProgressCallbackPtr read_progress_callback;

    using Queue = std::queue<ExecutingGraph::Node *>;

    void initializeExecution(size_t num_threads); /// Initialize executor contexts and task_queue.
    void finalizeExecution(); /// Check all processors are finished.
    void spawnThreads();
    void joinThreads();

    /// Methods connected to execution.
    void executeImpl(size_t num_threads);
    void executeStepImpl(size_t thread_num, std::atomic_bool * yield_flag = nullptr);
    void executeSingleThread(size_t thread_num);
    void finish();

    String dumpPipeline() const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
