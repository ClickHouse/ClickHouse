#include <IO/WriteHelpers.h>
#include <QueryPipeline/Chain.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void checkSingleInput(const IProcessor & transform)
{
    if (transform.getInputs().size() != 1)
        throw Exception("Transform for chain should have single input, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (transform.getInputs().front().isConnected())
        throw Exception("Transform for chain has connected input.", ErrorCodes::LOGICAL_ERROR);
}

static void checkSingleOutput(const IProcessor & transform)
{
    if (transform.getOutputs().size() != 1)
        throw Exception("Transform for chain should have single output, "
                        "but " + transform.getName() + " has " +
                        toString(transform.getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);

    if (transform.getOutputs().front().isConnected())
        throw Exception("Transform for chain has connected input.", ErrorCodes::LOGICAL_ERROR);
}

static void checkTransform(const IProcessor & transform)
{
    checkSingleInput(transform);
    checkSingleOutput(transform);
}

static void checkInitialized(const std::list<ProcessorPtr> & processors)
{
    if (processors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Drain is not initialized");
}

Chain::Chain(ProcessorPtr processor)
{
    checkTransform(*processor);
    processors.emplace_back(std::move(processor));
}

Chain::Chain(std::list<ProcessorPtr> processors_) : processors(std::move(processors_))
{
    if (processors.empty())
        return;

    checkSingleInput(*processors.front());
    checkSingleOutput(*processors.back());

    for (const auto & processor : processors)
    {
        for (const auto & input : processor->getInputs())
            if (&input != &getInputPort() && !input.isConnected())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Cannot initialize chain because there is a not connected input for {}",
                                processor->getName());

        for (const auto & output : processor->getOutputs())
            if (&output != &getOutputPort() && !output.isConnected())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Cannot initialize chain because there is a not connected output for {}",
                                processor->getName());
    }
}

void Chain::addSource(ProcessorPtr processor)
{
    checkTransform(*processor);

    if (!processors.empty())
        connect(processor->getOutputs().front(), getInputPort());

    processors.emplace_front(std::move(processor));
}

void Chain::addSink(ProcessorPtr processor)
{
    checkTransform(*processor);

    if (!processors.empty())
        connect(getOutputPort(), processor->getInputs().front());

    processors.emplace_front(std::move(processor));
}

IProcessor & Chain::getSource()
{
    checkInitialized(processors);
    return *processors.front();
}

IProcessor & Chain::getSink()
{
    checkInitialized(processors);
    return *processors.back();
}

InputPort & Chain::getInputPort() const
{
    checkInitialized(processors);
    return processors.front()->getInputs().front();
}

OutputPort & Chain::getOutputPort() const
{
    checkInitialized(processors);
    return processors.back()->getOutputs().front();
}

void Chain::reset()
{
    Chain to_remove = std::move(*this);
    *this = Chain();
}

}
