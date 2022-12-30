#pragma once
#include <QueryPipeline/PipelineResourcesHolder.h>
#include <QueryPipeline/SizeLimits.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <functional>

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
class ISink;
class ReadProgressCallback;

struct ColumnWithTypeAndName;
using ColumnsWithTypeAndName = std::vector<ColumnWithTypeAndName>;

class QueryPipeline
{
public:
    QueryPipeline();
    QueryPipeline(QueryPipeline &&) noexcept;
    QueryPipeline(const QueryPipeline &) = delete;

    QueryPipeline & operator=(QueryPipeline &&) noexcept;
    QueryPipeline & operator=(const QueryPipeline &) = delete;

    ~QueryPipeline();

    /// pulling or completed
    explicit QueryPipeline(Pipe pipe);
    /// pulling
    explicit QueryPipeline(std::shared_ptr<ISource> source);
    /// pushing
    explicit QueryPipeline(Chain chain);
    explicit QueryPipeline(std::shared_ptr<SinkToStorage> sink);
    explicit QueryPipeline(std::shared_ptr<IOutputFormat> format);

    /// completed
    QueryPipeline(
        QueryPlanResourceHolder resources_,
        Processors processors_);

    /// pushing
    QueryPipeline(
        QueryPlanResourceHolder resources_,
        Processors processors_,
        InputPort * input_);

    /// pulling
    QueryPipeline(
        QueryPlanResourceHolder resources_,
        Processors processors_,
        OutputPort * output_,
        OutputPort * totals_ = nullptr,
        OutputPort * extremes_ = nullptr);

    bool initialized() const { return !processors.empty(); }
    /// When initialized, exactly one of the following is true.
    /// Use PullingPipelineExecutor or PullingAsyncPipelineExecutor.
    bool pulling() const { return output != nullptr; }
    /// Use PushingPipelineExecutor or PushingAsyncPipelineExecutor.
    bool pushing() const { return input != nullptr; }
    /// Use PipelineExecutor. Call execute() to build one.
    bool completed() const { return initialized() && !pulling() && !pushing(); }

    /// Only for pushing.
    void complete(Pipe pipe);
    /// Only for pulling.
    void complete(std::shared_ptr<IOutputFormat> format);
    void complete(Chain chain);
    void complete(std::shared_ptr<SinkToStorage> sink);
    void complete(std::shared_ptr<ISink> sink);

    /// Only for pushing and pulling.
    Block getHeader() const;

    size_t getNumThreads() const { return num_threads; }
    void setNumThreads(size_t num_threads_) { num_threads = num_threads_; }

    void setProcessListElement(QueryStatus * elem);
    void setProgressCallback(const ProgressCallback & callback);
    void setLimitsAndQuota(const StreamLocalLimits & limits, std::shared_ptr<const EnabledQuota> quota_);
    bool tryGetResultRowsAndBytes(UInt64 & result_rows, UInt64 & result_bytes) const;

    void setQuota(std::shared_ptr<const EnabledQuota> quota_);

    void addStorageHolder(StoragePtr storage);

    /// Existing resources are not released here, see move ctor for QueryPlanResourceHolder.
    void addResources(QueryPlanResourceHolder holder) { resources = std::move(holder); }

    /// Skip updating profile events.
    /// For merges in mutations it may need special logic, it's done inside ProgressCallback.
    void disableProfileEventUpdate() { update_profile_events = false; }

    /// Create progress callback from limits and quotas.
    std::unique_ptr<ReadProgressCallback> getReadProgressCallback() const;

    /// Add processors and resources from other pipeline. Other pipeline should be completed.
    void addCompletedPipeline(QueryPipeline other);

    const Processors & getProcessors() const { return processors; }

    /// For pulling pipeline, convert structure to expected.
    /// Trash, need to remove later.
    void convertStructureTo(const ColumnsWithTypeAndName & columns);

    void reset();

private:
    QueryPlanResourceHolder resources;

    ProgressCallback progress_callback;
    std::shared_ptr<const EnabledQuota> quota;
    bool update_profile_events = true;

    Processors processors;

    InputPort * input = nullptr;

    OutputPort * output = nullptr;
    OutputPort * totals = nullptr;
    OutputPort * extremes = nullptr;

    QueryStatus * process_list_element = nullptr;

    IOutputFormat * output_format = nullptr;

    size_t num_threads = 0;

    friend class PushingPipelineExecutor;
    friend class PullingPipelineExecutor;
    friend class PushingAsyncPipelineExecutor;
    friend class PullingAsyncPipelineExecutor;
    friend class CompletedPipelineExecutor;
    friend class QueryPipelineBuilder;
};

}
