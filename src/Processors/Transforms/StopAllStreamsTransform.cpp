#include <Processors/Transforms/StopAllStreamsTransform.h>

#include <Processors/Port.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static const SharedHeader & signalHeader()
{
    static const SharedHeader header = std::make_shared<const Block>();
    return header;
}

StopAllStreamsTransform::StopAllStreamsTransform(SharedHeader header)
    : IProcessor({header, signalHeader()}, {header, signalHeader()})
{
}

IProcessor::Status StopAllStreamsTransform::prepare()
{
    auto & input = inputs.front();
    auto & signal_input = inputs.back();
    auto & output = outputs.front();
    auto & signal_output = outputs.back();

    /// This processor finishes the output only when it finishes itself, so here the consumer stopped reading.
    if (output.isFinished())
        signal_pending = true;

    if (signal_pending || signal_input.isFinished() || signal_input.hasData())
    {
        input.close();
        output.finish();
        signal_input.close();

        if (signal_pending && !signal_output.isFinished())
        {
            if (!signal_output.canPush())
                return Status::PortFull;

            signal_output.push(Chunk());
        }

        signal_output.finish();
        return Status::Finished;
    }

    signal_input.setNeeded();

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// The stream ended, and the consumer asks for more after processing all its data, so it did not stop
    /// on the last chunk: finish without telling the other streams.
    if (input.isFinished())
    {
        output.finish();
        signal_input.close();
        signal_output.finish();
        return Status::Finished;
    }

    if (input.hasData())
    {
        output.push(input.pull(true));
        return Status::PortFull;
    }

    input.setNeeded();
    return Status::NeedData;
}

StopAllStreamsSignal::StopAllStreamsSignal(size_t num_streams)
    : IProcessor(InputPorts(num_streams, signalHeader()), OutputPorts(num_streams, signalHeader()))
{
}

IProcessor::Status StopAllStreamsSignal::prepare()
{
    bool all_finished = true;
    bool stop = false;
    for (auto & input : inputs)
    {
        if (input.hasData())
        {
            stop = true;
            break;
        }

        if (!input.isFinished())
        {
            input.setNeeded();
            all_finished = false;
        }
    }

    if (!stop && !all_finished)
        return Status::NeedData;

    for (auto & input : inputs)
        input.close();
    for (auto & output : outputs)
        output.finish();
    return Status::Finished;
}

void addStopAllStreamsTransforms(std::initializer_list<QueryPipelineBuilder *> pipelines)
{
    size_t num_streams = 0;
    QueryPipelineBuilder * last_pipeline = nullptr;
    for (auto * pipeline : pipelines)
    {
        num_streams += pipeline->getNumStreams();
        if (pipeline->getNumStreams())
            last_pipeline = pipeline;
    }

    if (num_streams <= 1)
        return;

    auto signal = std::make_shared<StopAllStreamsSignal>(num_streams);
    auto signal_input = signal->getInputs().begin();
    auto signal_output = signal->getOutputs().begin();

    for (auto * pipeline : pipelines)
    {
        if (!pipeline->getNumStreams())
            continue;

        /// The signal processor is added with the last pipeline, when all its ports are connected. It is
        /// connected to the processors of the other pipelines too, which the port check does not allow.
        pipeline->transform(
            [&](const OutputPortRawPtrs & ports)
            {
                Processors processors;
                for (auto * port : ports)
                {
                    auto transform = std::make_shared<StopAllStreamsTransform>(port->getSharedHeader());
                    connect(*port, transform->getInputs().front());
                    connect(transform->getOutputs().back(), *signal_input++);
                    connect(*signal_output++, transform->getInputs().back());
                    processors.push_back(std::move(transform));
                }

                if (pipeline == last_pipeline)
                    processors.push_back(signal);
                return processors;
            },
            /* check_ports= */ pipelines.size() == 1);
    }
}

}
