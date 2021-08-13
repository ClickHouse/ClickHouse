#include <Processors/Drain.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void checkSink(const IProcessor & sink)
{
    if (!sink.getOutputs().empty())
        throw Exception("Sink for drain shouldn't have any output, but " + sink.getName() + " has " +
                        toString(sink.getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);

    if (sink.getInputs().empty())
        throw Exception("Sink for drain should have single input, but it doesn't have any",
                        ErrorCodes::LOGICAL_ERROR);

    if (sink.getInputs().size() > 1)
        throw Exception("Sink for drain should have single input, but " + sink.getName() + " has " +
                        toString(sink.getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (sink.getInputs().front().isConnected())
        throw Exception("Sink for drain has connected input.", ErrorCodes::LOGICAL_ERROR);
}

static void checkTransform(const IProcessor & transform)
{
    if (transform.getInputs().size() != 1)
        throw Exception("Transform for drain should have single input, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (transform.getOutputs().size() != 1)
        throw Exception("Transform for drain should have single output, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);

    if (transform.getInputs().front().isConnected())
        throw Exception("Transform for drain has connected input.", ErrorCodes::LOGICAL_ERROR);

    if (transform.getOutputs().front().isConnected())
        throw Exception("Transform for drain has connected input.", ErrorCodes::LOGICAL_ERROR);
}

void checkInitialized(const Processors & processors)
{
    if (processors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Drain is not initialized");
}

Drain::Drain(ProcessorPtr processor)
{
    checkSink(*processor);
    processors.emplace_back(std::move(processor));
}

void Drain::addTransform(ProcessorPtr processor)
{
    checkInitialized(processors);
    checkTransform(*processor);
    connect(processor->getOutputs().front(), processors.back()->getInputs().front());
    processors.emplace_back(std::move(processor));
}

InputPort & Drain::getPort() const
{
    checkInitialized(processors);
    return processors.back()->getInputs().front();
}

}
