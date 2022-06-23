#include <Processors/Sources/DelayedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/NullSink.h>
#include <Processors/ResizeProcessor.h>

namespace DB
{

DelayedSource::DelayedSource(const Block & header, Creator processors_creator, bool add_totals_port, bool add_extremes_port)
    : IProcessor({}, OutputPorts(1 + (add_totals_port ? 1 : 0) + (add_extremes_port ? 1 : 0), header))
    , creator(std::move(processors_creator))
{
    auto output = outputs.begin();

    main = &*output;
    ++output;

    if (add_totals_port)
    {
        totals = &*output;
        ++output;
    }

    if (add_extremes_port)
    {
        extremes = &*output;
        ++output;
    }
}

IProcessor::Status DelayedSource::prepare()
{
    /// At first, wait for main output is needed and expand pipeline.
    if (inputs.empty())
    {
        auto & first_output = outputs.front();

        /// If main output port was finished before callback was called, stop execution.
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

        if (!output->canPush())
            return Status::PortFull;

        if (input->isFinished())
        {
            output->finish();
            continue;
        }

        input->setNeeded();
        if (!input->hasData())
            return Status::NeedData;

        output->pushData(input->pullData(true));
        return Status::PortFull;
    }

    return Status::Finished;
}

/// Fix port from returned pipe. Create source_port if created or drop if source_port is null.
void synchronizePorts(OutputPort *& pipe_port, OutputPort * source_port, const Block & header, Processors & processors)
{
    if (source_port)
    {
        /// Need port in DelayedSource. Create NullSource.
        if (!pipe_port)
        {
            processors.emplace_back(std::make_shared<NullSource>(header));
            pipe_port = &processors.back()->getOutputs().back();
        }
    }
    else
    {
        /// Has port in pipe, but don't need it. Create NullSink.
        if (pipe_port)
        {
            auto sink = std::make_shared<NullSink>(header);
            connect(*pipe_port, sink->getPort());
            processors.emplace_back(std::move(sink));
            pipe_port = nullptr;
        }
    }
}

void DelayedSource::work()
{
    auto pipe = creator();
    const auto & header = main->getHeader();

    if (pipe.empty())
    {
        auto source = std::make_shared<NullSource>(header);
        main_output = &source->getPort();
        processors.emplace_back(std::move(source));
        return;
    }

    pipe.resize(1);

    main_output = pipe.getOutputPort(0);
    totals_output = pipe.getTotalsPort();
    extremes_output = pipe.getExtremesPort();

    processors = Pipe::detachProcessors(std::move(pipe));

    synchronizePorts(totals_output, totals, header, processors);
    synchronizePorts(extremes_output, extremes, header, processors);
}

Processors DelayedSource::expandPipeline()
{
    /// Add new inputs. They must have the same header as output.
    for (const auto & output : {main_output, totals_output, extremes_output})
    {
        if (!output)
            continue;

        inputs.emplace_back(outputs.front().getHeader(), this);
        /// Connect checks that header is same for ports.
        connect(*output, inputs.back());
        inputs.back().setNeeded();
    }

    /// Executor will check that all processors are connected.
    return std::move(processors);
}

Pipe createDelayedPipe(const Block & header, DelayedSource::Creator processors_creator, bool add_totals_port, bool add_extremes_port)
{
    auto source = std::make_shared<DelayedSource>(header, std::move(processors_creator), add_totals_port, add_extremes_port);

    auto * main = &source->getPort();
    auto * totals = source->getTotalsPort();
    auto * extremes = source->getExtremesPort();

    return Pipe(std::move(source), main, totals, extremes);
}

}
