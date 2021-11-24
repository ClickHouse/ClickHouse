#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <iostream>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class PushingSource : public ISource
{
public:
    explicit PushingSource(const Block & header, std::atomic_bool & need_data_flag_)
        : ISource(header)
        , need_data_flag(need_data_flag_)
    {}

    String getName() const override { return "PushingSource"; }

    void setData(Chunk chunk)
    {
        need_data_flag = false;
        data = std::move(chunk);
    }

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


PushingPipelineExecutor::PushingPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    if (!pipeline.pushing())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for PushingPipelineExecutor must be pushing");

    pushing_source = std::make_shared<PushingSource>(pipeline.input->getHeader(), need_data_flag);
    connect(pushing_source->getPort(), *pipeline.input);
    pipeline.processors.emplace_back(pushing_source);
}

PushingPipelineExecutor::~PushingPipelineExecutor()
{
    try
    {
        finish();
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


void PushingPipelineExecutor::start()
{
    if (started)
        return;

    started = true;
    executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);

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
