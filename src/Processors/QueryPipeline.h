#pragma once
#include <Processors/PipelineResourcesHolder.h>

namespace DB
{

class InputPort;
class OutputPort;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

class QueryStatus;

struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;

struct StreamLocalLimits;
class EnabledQuota;

class Block;
class Pipe;
class Chain;
class IOutputFormat;
class SinkToStorage;
class ISource;

class QueryPipeline
{
public:
    QueryPipeline();
    QueryPipeline(QueryPipeline &&);
    QueryPipeline(const QueryPipeline &) = delete;

    QueryPipeline & operator=(QueryPipeline &&);
    QueryPipeline & operator=(const QueryPipeline &) = delete;

    ~QueryPipeline();

    /// pulling
    explicit QueryPipeline(Pipe pipe);
    explicit QueryPipeline(std::shared_ptr<ISource> source);
    /// pushing
    explicit QueryPipeline(Chain chain);
    explicit QueryPipeline(std::shared_ptr<SinkToStorage> sink);

    /// completed
    QueryPipeline(
        PipelineResourcesHolder resources_,
        Processors processors_);

    /// pushing
    QueryPipeline(
        PipelineResourcesHolder resources_,
        Processors processors_,
        InputPort * input_);

    /// pulling
    QueryPipeline(
        PipelineResourcesHolder resources_,
        Processors processors_,
        OutputPort * output_,
        OutputPort * totals_ = nullptr,
        OutputPort * extremes_ = nullptr);

    /// Exactly one of the following is true.
    bool initialized() const { return !processors.empty(); }
    /// Use PullingPipelineExecutor or PullingAsyncPipelineExecutor.
    bool pulling() const { return output != nullptr; }
    /// Use PushingPipelineExecutor or PushingAsyncPipelineExecutor.
    bool pushing() const { return input != nullptr; }
    /// Use PipelineExecutor. Call execute() to build one.
    bool completed() const { return !pulling() && !pushing(); }

    /// Only for pushing.
    void complete(Pipe pipe);
    /// Only for pulling.
    void complete(std::shared_ptr<IOutputFormat> format);

    /// Only for pushing and pulling.
    Block getHeader() const;

    size_t getNumThreads() const { return num_threads; }
    void setNumThreads(size_t num_threads_) { num_threads = num_threads_; }

    void setProcessListElement(QueryStatus * elem);
    void setProgressCallback(const ProgressCallback & callback);
    void setLimitsAndQuota(const StreamLocalLimits & limits, std::shared_ptr<const EnabledQuota> quota);
    bool tryGetResultRowsAndBytes(size_t & result_rows, size_t & result_bytes) const;

    const Processors & getProcessors() const { return processors; }

    void reset();

private:
    PipelineResourcesHolder resources;
    Processors processors;

    InputPort * input = nullptr;

    OutputPort * output = nullptr;
    OutputPort * totals = nullptr;
    OutputPort * extremes = nullptr;

    QueryStatus * process_list_element = nullptr;

    size_t num_threads = 0;

    friend class PushingPipelineExecutor;
    friend class PullingPipelineExecutor;
    friend class PushingAsyncPipelineExecutor;
    friend class PullingAsyncPipelineExecutor;
    friend class CompletedPipelineExecutor;
};

}
