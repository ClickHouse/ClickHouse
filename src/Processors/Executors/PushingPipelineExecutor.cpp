#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/Chain.h>
#include <Processors/Sinks/ExceptionHandlingSink.h>


namespace DB
{

class PushingSource : public ISource
{
public:
    explicit PushingSource(const Block & header, std::atomic_bool & need_data_flag_)
        : ISource(header)
        , need_data_flag(need_data_flag_)
    {}

    String getName() const override { return "PullingOutputFormat"; }

    void setData(Chunk chunk) { data = std::move(chunk); }

protected:

    Status prepare() override
    {
        auto status = ISource::prepare();
        if (status == Status::Ready)
            need_data_flag = true;

        return status;
    }

    Chunk generate() override
    {
        return std::move(data);
    }

private:
    Chunk data;
    std::atomic_bool & need_data_flag;
};


PushingPipelineExecutor::PushingPipelineExecutor(Chain & chain_) : chain(chain_)
{
    pushing_source = std::make_shared<PushingSource>(chain.getInputHeader(), need_data_flag);
    auto sink = std::make_shared<ExceptionHandlingSink>(chain.getOutputHeader());
    connect(pushing_source->getPort(), chain.getInputPort());
    connect(chain.getOutputPort(), sink->getInputPort());
}

PushingPipelineExecutor::~PushingPipelineExecutor()
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

const Block & PushingPipelineExecutor::getHeader() const
{
    return pushing_source->getPort().getHeader();
}


void PushingPipelineExecutor::start()
{
    if (started)
        return;

    started = true;
    executor = std::make_shared<PipelineExecutor>(*processors);

    if (!executor->executeStep(&need_data_flag))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Pipeline for PushingPipelineExecutor was finished before all data was inserted");
}

void PushingPipelineExecutor::push(Chunk chunk)
{
    if (!started)
        start();

    pushing_source->setData(std::move(chunk));

    if (!executor->executeStep(&need_data_flag))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Pipeline for PushingPipelineExecutor was finished before all data was inserted");
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
