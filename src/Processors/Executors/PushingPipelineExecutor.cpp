#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/ReadProgressCallback.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

class PushingSource : public ISource
{
public:
    explicit PushingSource(const Block & header, std::atomic_bool & input_wait_flag_)
        : ISource(header)
        , input_wait_flag(input_wait_flag_)
    {}

    String getName() const override { return "PushingSource"; }

    void setData(Chunk chunk)
    {
        input_wait_flag = false;
        data = std::move(chunk);
    }

protected:

    Status prepare() override
    {
        auto status = ISource::prepare();
        if (status == Status::Ready)
            input_wait_flag = true;

        return status;
    }

    Chunk generate() override
    {
        return std::move(data);
    }

private:
    Chunk data;
    std::atomic_bool & input_wait_flag;
};


PushingPipelineExecutor::PushingPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    if (!pipeline.pushing())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for PushingPipelineExecutor must be pushing");

    pushing_source = std::make_shared<PushingSource>(pipeline.input->getHeader(), input_wait_flag);
    connect(pushing_source->getPort(), *pipeline.input);
    pipeline.processors->emplace_back(pushing_source);
}

PushingPipelineExecutor::~PushingPipelineExecutor()
{
    /// It must be finalized explicitly. Otherwise we cancel it assuming it's due to an exception.
    chassert(finished || std::uncaught_exceptions() || std::current_exception());
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("PushingPipelineExecutor");
    }
}

const Block & PushingPipelineExecutor::getHeader() const
{
    return pushing_source->getPort().getHeader();
}

[[noreturn]] static void throwOnExecutionStatus(PipelineExecutor::ExecutionStatus status)
{
    if (status == PipelineExecutor::ExecutionStatus::CancelledByTimeout
        || status == PipelineExecutor::ExecutionStatus::CancelledByUser)
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Pipeline for PushingPipelineExecutor was finished before all data was inserted");
}

void PushingPipelineExecutor::start()
{
    if (started)
        return;

    started = true;
    executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);
    executor->setReadProgressCallback(pipeline.getReadProgressCallback());

    if (!executor->executeStep(&input_wait_flag))
        throwOnExecutionStatus(executor->getExecutionStatus());
}

void PushingPipelineExecutor::push(Chunk chunk)
{
    if (!started)
        start();

    pushing_source->setData(std::move(chunk));

    if (!executor->executeStep(&input_wait_flag))
        throwOnExecutionStatus(executor->getExecutionStatus());
}

void PushingPipelineExecutor::push(Block block)
{
    push(Chunk(block.getColumns(), block.rows()));
}

void PushingPipelineExecutor::finish()
{
    if (finished)
        return;
    finished = true;

    if (executor)
        executor->executeStep();
}

void PushingPipelineExecutor::cancel()
{
    /// Cancel execution if it wasn't finished.
    if (executor && !finished)
    {
        finished = true;
        executor->cancel();
    }
}

}
