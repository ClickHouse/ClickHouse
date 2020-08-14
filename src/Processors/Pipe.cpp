#include <Processors/Pipe.h>
#include <IO/WriteHelpers.h>
#include <Processors/Sources/SourceFromInputStream.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void checkSingleInput(const IProcessor & transform)
{
    if (transform.getInputs().size() != 1)
        throw Exception("Processor for pipe should have single input, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);
}

static void checkMultipleInputs(const IProcessor & transform, size_t num_inputs)
{
    if (transform.getInputs().size() != num_inputs)
        throw Exception("Processor for pipe should have " + toString(num_inputs) + " inputs, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);
}

static void checkSingleOutput(const IProcessor & transform)
{
    if (transform.getOutputs().size() != 1)
        throw Exception("Processor for pipe should have single output, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
}

static void checkSimpleTransform(const IProcessor & transform)
{
    checkSingleInput(transform);
    checkSingleOutput(transform);
}

static void checkSource(const IProcessor & source)
{
    if (!source.getInputs().empty())
        throw Exception("Source for pipe shouldn't have any input, but " + source.getName() + " has " +
                        toString(source.getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (source.getOutputs().empty())
        throw Exception("Source for pipe should have single output, but it doesn't have any",
                        ErrorCodes::LOGICAL_ERROR);

    if (source.getOutputs().size() > 1)
        throw Exception("Source for pipe should have single or two outputs, but " + source.getName() + " has " +
                        toString(source.getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
}


Pipe::Pipe(ProcessorPtr source)
{
    if (auto * source_from_input_stream = typeid_cast<SourceFromInputStream *>(source.get()))
    {
        totals = source_from_input_stream->getTotalsPort();
        extremes = source_from_input_stream->getExtremesPort();
    }
    else if (source->getOutputs().size() != 1)
        checkSource(*source);

    output_port = &source->getOutputs().front();

    processors.emplace_back(std::move(source));
    max_parallel_streams = 1;
}

Pipe::Pipe(Processors processors_, OutputPort * output_port_, OutputPort * totals_, OutputPort * extremes_)
    : processors(std::move(processors_)), output_port(output_port_), totals(totals_), extremes(extremes_)
{
}

Pipe::Pipe(Pipes && pipes, ProcessorPtr transform)
{
    checkSingleOutput(*transform);
    checkMultipleInputs(*transform, pipes.size());

    auto it = transform->getInputs().begin();

    for (auto & pipe : pipes)
    {
        connect(*pipe.output_port, *it);
        ++it;

        max_parallel_streams += pipe.max_parallel_streams;
        processors.insert(processors.end(), pipe.processors.begin(), pipe.processors.end());

        std::move(pipe.table_locks.begin(), pipe.table_locks.end(), std::back_inserter(table_locks));
        std::move(pipe.interpreter_context.begin(), pipe.interpreter_context.end(), std::back_inserter(interpreter_context));
        std::move(pipe.storage_holders.begin(), pipe.storage_holders.end(), std::back_inserter(storage_holders));
    }

    output_port = &transform->getOutputs().front();
    processors.emplace_back(std::move(transform));
}

Pipe::Pipe(OutputPort * port) : output_port(port)
{
}

void Pipe::addProcessors(const Processors & processors_)
{
    processors.insert(processors.end(), processors_.begin(), processors_.end());
}

void Pipe::addSimpleTransform(ProcessorPtr transform)
{
    checkSimpleTransform(*transform);
    connect(*output_port, transform->getInputs().front());
    output_port = &transform->getOutputs().front();
    processors.emplace_back(std::move(transform));
}

void Pipe::setLimits(const ISourceWithProgress::LocalLimits & limits)
{
    for (auto & processor : processors)
    {
        if (auto * source_with_progress = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source_with_progress->setLimits(limits);
    }
}

void Pipe::setQuota(const std::shared_ptr<const EnabledQuota> & quota)
{
    for (auto & processor : processors)
    {
        if (auto * source_with_progress = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source_with_progress->setQuota(quota);
    }
}

void Pipe::pinSources(size_t executor_number)
{
    for (auto & processor : processors)
    {
        if (auto * source = dynamic_cast<ISource *>(processor.get()))
            source->setStream(executor_number);
    }
}

void Pipe::enableQuota()
{
    for (auto & processor : processors)
    {
        if (auto * source = dynamic_cast<ISource *>(processor.get()))
            source->enableQuota();
    }
}

}
