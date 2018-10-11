#pragma once

#include <Processors/Executors/SequentialPipelineExecutor.h>
#include <Common/EventCounter.h>

namespace DB
{

class SequentialTransformExecutor
{
public:
    SequentialTransformExecutor(const Processors & processors, InputPort & pipeline_input, OutputPort & pipeline_output)
        : IProcessor({pipeline_input.getHeader()}, {pipeline_output.getHeader()})
        , executor(processors)
        , input(pipeline_output.getHeader())
        , output(pipeline_input.getHeader())
        , pipeline_input(pipeline_input)
        , pipeline_output(pipeline_output)
    {
        connect(output, pipeline_input);
        connect(pipeline_output, input);
    }

    void execute(Block & block)
    {
        output.push(std::move(block));

        auto status = executor.prepare();
        while (status != IProcessor::Status::NeedData)
        {
            if (status == IProcessor::Status::Ready)
                executor.work();
            else if (status == IProcessor::Status::Async)
            {
                EventCounter watch;
                executor.schedule(watch);
                watch.wait();
            }
            else
                throw Exception("Unexpected status for SequentialTransformExecutor: " + std::to_string(int(status)),
                                ErrorCodes::LOGICAL_ERROR);
        }

        block = input.pull();
    }

private:
    SequentialPipelineExecutor executor;
    InputPort input;
    OutputPort output;
    InputPort & pipeline_input;
    OutputPort & pipeline_output;
};

}
