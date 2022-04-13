#pragma once

#include <Processors/IProcessor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class QueryPipelineProcessorsCollector;

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

class QueryPlan;

class PipelineExecutor;
using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

struct SubqueryForSet;
using SubqueriesForSets = std::unordered_map<String, SubqueryForSet>;

struct SizeLimits;

struct ExpressionActionsSettings;

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class QueryPipelineBuilder
{
public:
    QueryPipelineBuilder() = default;
    ~QueryPipelineBuilder() = default;
    QueryPipelineBuilder(QueryPipelineBuilder &&) = default;
    QueryPipelineBuilder(const QueryPipelineBuilder &) = delete;
    QueryPipelineBuilder & operator= (QueryPipelineBuilder && rhs) = default;
    QueryPipelineBuilder & operator= (const QueryPipelineBuilder & rhs) = delete;

    /// All pipes must have same header.
    void init(Pipe pipe);
    void init(QueryPipeline pipeline);
    /// Clear and release all resources.
    void reset();

    bool initialized() { return !pipe.empty(); }
    bool isCompleted() { return pipe.isCompleted(); }

    using StreamType = Pipe::StreamType;

    /// Add transform with simple input and simple output for each port.
    void addSimpleTransform(const Pipe::ProcessorGetter & getter);
    void addSimpleTransform(const Pipe::ProcessorGetterWithStreamKind & getter);
    /// Add transform with getNumStreams() input ports.
    void addTransform(ProcessorPtr transform);
    void addTransform(ProcessorPtr transform, InputPort * totals, InputPort * extremes);

    void addChains(std::vector<Chain> chains);
    void addChain(Chain chain);

    using Transformer = std::function<Processors(OutputPortRawPtrs ports)>;
    /// Transform pipeline in general way.
    void transform(const Transformer & transformer);

    /// Add TotalsHavingTransform. Resize pipeline to single input. Adds totals port.
    void addTotalsHavingTransform(ProcessorPtr transform);
    /// Add transform which calculates extremes. This transform adds extremes port and doesn't change inputs number.
    void addExtremesTransform();
    /// Sink is a processor with single input port and no output ports. Creates sink for each output port.
    /// Pipeline will be completed after this transformation.
    void setSinks(const Pipe::ProcessorGetterWithStreamKind & getter);

    /// Add totals which returns one chunk with single row with defaults.
    void addDefaultTotals();

    /// Forget about current totals and extremes. It is needed before aggregation, cause they will be calculated again.
    void dropTotalsAndExtremes();

    /// Will read from this stream after all data was read from other streams.
    void addDelayedStream(ProcessorPtr source);

    void addMergingAggregatedMemoryEfficientTransform(AggregatingTransformParamsPtr params, size_t num_merging_processors);

    /// Changes the number of output ports if needed. Adds ResizeTransform.
    void resize(size_t num_streams, bool force = false, bool strict = false);

    /// Unite several pipelines together. Result pipeline would have common_header structure.
    /// If collector is used, it will collect only newly-added processors, but not processors from pipelines.
    static QueryPipelineBuilder unitePipelines(
            std::vector<std::unique_ptr<QueryPipelineBuilder>> pipelines,
            size_t max_threads_limit = 0,
            Processors * collected_processors = nullptr);

    /// Join two pipelines together using JoinPtr.
    /// If collector is used, it will collect only newly-added processors, but not processors from pipelines.
    static std::unique_ptr<QueryPipelineBuilder> joinPipelines(
        std::unique_ptr<QueryPipelineBuilder> left,
        std::unique_ptr<QueryPipelineBuilder> right,
        JoinPtr join,
        size_t max_block_size,
        Processors * collected_processors = nullptr);

    /// Add other pipeline and execute it before current one.
    /// Pipeline must have empty header, it should not generate any chunk.
    /// This is used for CreatingSets.
    void addPipelineBefore(QueryPipelineBuilder pipeline);

    void addCreatingSetsTransform(const Block & res_header, SubqueryForSet subquery_for_set, const SizeLimits & limits, ContextPtr context);

    PipelineExecutorPtr execute();

    size_t getNumStreams() const { return pipe.numOutputPorts(); }

    bool hasTotals() const { return pipe.getTotalsPort() != nullptr; }

    const Block & getHeader() const { return pipe.getHeader(); }

    void addTableLock(TableLockHolder lock) { pipe.addTableLock(std::move(lock)); }
    void addInterpreterContext(ContextPtr context) { pipe.addInterpreterContext(std::move(context)); }
    void addStorageHolder(StoragePtr storage) { pipe.addStorageHolder(std::move(storage)); }
    void addQueryPlan(std::unique_ptr<QueryPlan> plan);
    void setLimits(const StreamLocalLimits & limits) { pipe.setLimits(limits); }
    void setLeafLimits(const SizeLimits & limits) { pipe.setLeafLimits(limits); }
    void setQuota(const std::shared_ptr<const EnabledQuota> & quota) { pipe.setQuota(quota); }

    void setProgressCallback(const ProgressCallback & callback);
    void setProcessListElement(QueryStatus * elem);

    /// Recommend number of threads for pipeline execution.
    size_t getNumThreads() const
    {
        auto num_threads = pipe.maxParallelStreams();

        if (max_threads) //-V1051
            num_threads = std::min(num_threads, max_threads);

        return std::max<size_t>(1, num_threads);
    }

    /// Set upper limit for the recommend number of threads
    void setMaxThreads(size_t max_threads_) { max_threads = max_threads_; }

    /// Update upper limit for the recommend number of threads
    void limitMaxThreads(size_t max_threads_)
    {
        if (max_threads == 0 || max_threads_ < max_threads)
            max_threads = max_threads_;
    }

    /// Convert query pipeline to pipe.
    static Pipe getPipe(QueryPipelineBuilder pipeline) { return std::move(pipeline.pipe); }
    static QueryPipeline getPipeline(QueryPipelineBuilder builder);

private:

    Pipe pipe;

    /// Limit on the number of threads. Zero means no limit.
    /// Sometimes, more streams are created then the number of threads for more optimal execution.
    size_t max_threads = 0;

    QueryStatus * process_list_element = nullptr;

    void checkInitialized();
    void checkInitializedAndNotCompleted();

    void setCollectedProcessors(Processors * processors);

    friend class QueryPipelineProcessorsCollector;
};

/// This is a small class which collects newly added processors to QueryPipeline.
/// Pipeline must live longer than this class.
class QueryPipelineProcessorsCollector
{
public:
    explicit QueryPipelineProcessorsCollector(QueryPipelineBuilder & pipeline_, IQueryPlanStep * step_ = nullptr);
    ~QueryPipelineProcessorsCollector();

    Processors detachProcessors(size_t group = 0);

private:
    QueryPipelineBuilder & pipeline;
    IQueryPlanStep * step;
    Processors processors;
};

}
