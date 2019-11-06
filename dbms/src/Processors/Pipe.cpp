#include <Processors/Pipe.h>
#include <IO/WriteHelpers.h>

namespace DB
{

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

    if (source.getOutputs().size() != 1)
        throw Exception("Source for pipe should have single output, but " + source.getName() + " has " +
                        toString(source.getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
}


Pipe::Pipe(ProcessorPtr source)
{
    checkSource(*source);
    output_port = &source->getOutputs().front();
    processors.emplace_back(std::move(source));
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

        processors.insert(processors.end(), pipe.processors.begin(), pipe.processors.end());
    }

    output_port = &transform->getOutputs().front();
    processors.emplace_back(std::move(transform));
}

void Pipe::addSimpleTransform(ProcessorPtr transform)
{
    checkSimpleTransform(*transform);
    connect(*output_port, transform->getInputs().front());
    output_port = &transform->getOutputs().front();
    processors.emplace_back(std::move(transform));
}

}
