#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/QueryPipeline.h>

#include <Common/setThreadName.h>
#include <ext/scope_guard.h>

namespace DB
{

struct PullingPipelineExecutor::Data
{
    PipelineExecutorPtr executor;
    std::exception_ptr exception;
    std::atomic_bool is_executed = false;
    std::atomic_bool has_exception = false;
    ThreadFromGlobalPool thread;

    ~Data()
    {
        if (thread.joinable())
            thread.join();
    }

    void rethrowExceptionIfHas()
    {
        if (has_exception)
        {
            has_exception = false;
            std::rethrow_exception(std::move(exception));
        }
    }
};

PullingPipelineExecutor::PullingPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    lazy_format = std::make_shared<LazyOutputFormat>(pipeline.getHeader());
    pipeline.setOutput(lazy_format);
}

PullingPipelineExecutor::~PullingPipelineExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("PullingPipelineExecutor");
    }
}

const Block & PullingPipelineExecutor::getHeader() const
{
    return lazy_format->getPort(IOutputFormat::PortKind::Main).getHeader();
}

static void threadFunction(PullingPipelineExecutor::Data & data, ThreadGroupStatusPtr thread_group, size_t num_threads)
{
    if (thread_group)
        CurrentThread::attachTo(thread_group);

    SCOPE_EXIT(
        if (thread_group)
            CurrentThread::detachQueryIfNotDetached();
    );

    setThreadName("QueryPipelineEx");

    try
    {
        data.executor->execute(num_threads);
    }
    catch (...)
    {
        data.exception = std::current_exception();
        data.has_exception = true;
    }
}


bool PullingPipelineExecutor::pull(Chunk & chunk, uint64_t milliseconds)
{
    if (!data)
    {
        data = std::make_unique<Data>();
        data->executor = pipeline.execute();

        auto func = [&, thread_group = CurrentThread::getGroup()]()
        {
            threadFunction(*data, thread_group, pipeline.getNumThreads());
        };

        data->thread = ThreadFromGlobalPool(std::move(func));
    }

    if (data->has_exception)
    {
        /// Finish lazy format in case of exception. Otherwise thread.join() may hung.
        lazy_format->finish();
        data->has_exception = false;
        std::rethrow_exception(std::move(data->exception));
    }

    if (lazy_format->isFinished())
    {
        data->is_executed = true;
        /// Wait thread ant rethrow exception if any.
        cancel();
        return false;
    }

    chunk = lazy_format->getChunk(milliseconds);
    return true;
}

bool PullingPipelineExecutor::pull(Block & block, uint64_t milliseconds)
{
    Chunk chunk;

    if (!pull(chunk, milliseconds))
        return false;

    if (!chunk)
    {
        /// In case if timeout exceeded.
        block.clear();
        return true;
    }

    block = lazy_format->getPort(IOutputFormat::PortKind::Main).getHeader().cloneWithColumns(chunk.detachColumns());

    if (auto chunk_info = chunk.getChunkInfo())
    {
        if (const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(chunk_info.get()))
        {
            block.info.bucket_num = agg_info->bucket_num;
            block.info.is_overflows = agg_info->is_overflows;
        }
    }

    return true;
}

void PullingPipelineExecutor::cancel()
{
    /// Cancel execution if it wasn't finished.
    if (data && !data->is_executed && data->executor)
        data->executor->cancel();

    /// Finish lazy format. Otherwise thread.join() may hung.
    if (!lazy_format->isFinished())
        lazy_format->finish();

    /// Join thread here to wait for possible exception.
    if (data && data->thread.joinable())
        data->thread.join();

    /// Rethrow exception to not swallow it in destructor.
    if (data)
        data->rethrowExceptionIfHas();
}

Chunk PullingPipelineExecutor::getTotals()
{
    return lazy_format->getTotals();
}

Chunk PullingPipelineExecutor::getExtremes()
{
    return lazy_format->getExtremes();
}

Block PullingPipelineExecutor::getTotalsBlock()
{
    auto totals = getTotals();

    if (totals.empty())
        return {};

    const auto & header = lazy_format->getPort(IOutputFormat::PortKind::Totals).getHeader();
    return header.cloneWithColumns(totals.detachColumns());
}

Block PullingPipelineExecutor::getExtremesBlock()
{
    auto extremes = getExtremes();

    if (extremes.empty())
        return {};

    const auto & header = lazy_format->getPort(IOutputFormat::PortKind::Extremes).getHeader();
    return header.cloneWithColumns(extremes.detachColumns());
}

BlockStreamProfileInfo & PullingPipelineExecutor::getProfileInfo()
{
    return lazy_format->getProfileInfo();
}

}
