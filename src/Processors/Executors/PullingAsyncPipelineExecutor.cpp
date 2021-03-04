#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/QueryPipeline.h>

#include <Common/setThreadName.h>
#include <ext/scope_guard.h>

namespace DB
{

struct PullingAsyncPipelineExecutor::Data
{
    PipelineExecutorPtr executor;
    std::exception_ptr exception;
    std::atomic_bool is_finished = false;
    std::atomic_bool has_exception = false;
    ThreadFromGlobalPool thread;
    Poco::Event finish_event;

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

PullingAsyncPipelineExecutor::PullingAsyncPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    if (!pipeline.isCompleted())
    {
        lazy_format = std::make_shared<LazyOutputFormat>(pipeline.getHeader());
        pipeline.setOutputFormat(lazy_format);
    }
}

PullingAsyncPipelineExecutor::~PullingAsyncPipelineExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("PullingAsyncPipelineExecutor");
    }
}

const Block & PullingAsyncPipelineExecutor::getHeader() const
{
    return lazy_format ? lazy_format->getPort(IOutputFormat::PortKind::Main).getHeader()
                       : pipeline.getHeader(); /// Empty.
}

static void threadFunction(PullingAsyncPipelineExecutor::Data & data, ThreadGroupStatusPtr thread_group, size_t num_threads)
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

    data.is_finished = true;
    data.finish_event.set();
}


bool PullingAsyncPipelineExecutor::pull(Chunk & chunk, uint64_t milliseconds)
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
        if (lazy_format)
            lazy_format->finish();

        data->has_exception = false;
        std::rethrow_exception(std::move(data->exception));
    }

    bool is_execution_finished = lazy_format ? lazy_format->isFinished()
                                             : data->is_finished.load();

    if (is_execution_finished)
    {
        /// If lazy format is finished, we don't cancel pipeline but wait for main thread to be finished.
        data->is_finished = true;
        /// Wait thread ant rethrow exception if any.
        cancel();
        return false;
    }

    if (lazy_format)
    {
        chunk = lazy_format->getChunk(milliseconds);
        return true;
    }

    chunk.clear();
    data->finish_event.tryWait(milliseconds);
    return true;
}

bool PullingAsyncPipelineExecutor::pull(Block & block, uint64_t milliseconds)
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

void PullingAsyncPipelineExecutor::cancel()
{
    /// Cancel execution if it wasn't finished.
    if (data && !data->is_finished && data->executor)
        data->executor->cancel();

    /// Finish lazy format. Otherwise thread.join() may hung.
    if (lazy_format && !lazy_format->isFinished())
        lazy_format->finish();

    /// Join thread here to wait for possible exception.
    if (data && data->thread.joinable())
        data->thread.join();

    /// Rethrow exception to not swallow it in destructor.
    if (data)
        data->rethrowExceptionIfHas();
}

Chunk PullingAsyncPipelineExecutor::getTotals()
{
    return lazy_format ? lazy_format->getTotals()
                       : Chunk();
}

Chunk PullingAsyncPipelineExecutor::getExtremes()
{
    return lazy_format ? lazy_format->getExtremes()
                       : Chunk();
}

Block PullingAsyncPipelineExecutor::getTotalsBlock()
{
    auto totals = getTotals();

    if (totals.empty())
        return {};

    const auto & header = lazy_format->getPort(IOutputFormat::PortKind::Totals).getHeader();
    return header.cloneWithColumns(totals.detachColumns());
}

Block PullingAsyncPipelineExecutor::getExtremesBlock()
{
    auto extremes = getExtremes();

    if (extremes.empty())
        return {};

    const auto & header = lazy_format->getPort(IOutputFormat::PortKind::Extremes).getHeader();
    return header.cloneWithColumns(extremes.detachColumns());
}

BlockStreamProfileInfo & PullingAsyncPipelineExecutor::getProfileInfo()
{
    static BlockStreamProfileInfo profile_info;
    return lazy_format ? lazy_format->getProfileInfo()
                       : profile_info;
}

}
