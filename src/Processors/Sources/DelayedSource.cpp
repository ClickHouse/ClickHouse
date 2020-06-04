#include <Processors/Sources/DelayedSource.h>
#include "NullSource.h"

namespace DB
{

DelayedSource::DelayedSource(const Block & header, Creator processors_creator)
    : IProcessor({}, OutputPorts(3, header))
    , creator(std::move(processors_creator))
{
}

IProcessor::Status DelayedSource::prepare()
{
    /// At first, wait for main input is needed and expand pipeline.
    if (inputs.empty())
    {
        auto & first_output = outputs.front();

        /// If main port was finished before callback was called, stop execution.
        if (first_output.isFinished())
        {
            for (auto & output : outputs)
                output.finish();

            return Status::Finished;
        }

        if (!first_output.isNeeded())
            return Status::PortFull;

        /// Call creator callback to get processors.
        if (processors.empty())
            return Status::Ready;

        return Status::ExpandPipeline;
    }

    /// Process ports in order: main, totals, extremes
    auto output = outputs.begin();
    for (auto input = inputs.begin(); input != inputs.end(); ++input, ++output)
    {
        if (output->isFinished())
        {
            input->close();
            continue;
        }

        if (!output->isNeeded())
            return Status::PortFull;

        if (input->isFinished())
        {
            output->finish();
            continue;
        }

        input->setNeeded();
        if (!input->hasData())
            return Status::PortFull;

        output->pushData(input->pullData(true));
        return Status::PortFull;
    }

    return Status::Finished;
}

void DelayedSource::work()
{
    auto pipe = creator();

    main_output = &pipe.getPort();
    totals_output = pipe.getTotalsPort();
    extremes_output = pipe.getExtremesPort();

    processors = std::move(pipe).detachProcessors();

    if (!totals_output)
    {
        processors.emplace_back(std::make_shared<NullSource>(main_output->getHeader()));
        totals_output = &processors.back()->getOutputs().back();
    }

    if (!extremes_output)
    {
        processors.emplace_back(std::make_shared<NullSource>(main_output->getHeader()));
        extremes_output = &processors.back()->getOutputs().back();
    }
}

Processors DelayedSource::expandPipeline()
{
    /// Add new inputs. They must have the same header as output.
    for (const auto & output : {main_output, totals_output, extremes_output})
    {
        inputs.emplace_back(outputs.front().getHeader(), this);
        /// Connect checks that header is same for ports.
        connect(*output, inputs.back());
        inputs.back().setNeeded();
    }

    /// Executor will check that all processors are connected.
    return std::move(processors);
}

Pipe createDelayedPipe(const Block & header, DelayedSource::Creator processors_creator)
{
    auto source = std::make_shared<DelayedSource>(header, std::move(processors_creator));

    Pipe pipe(&source->getPort(DelayedSource::Main));
    pipe.setTotalsPort(&source->getPort(DelayedSource::Totals));
    pipe.setExtremesPort(&source->getPort(DelayedSource::Extremes));

    pipe.addProcessors({std::move(source)});
    return pipe;
}

}
