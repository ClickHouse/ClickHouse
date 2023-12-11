#include <IO/WriteHelpers.h>
#include <QueryPipeline/Chain.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void checkSingleInput(const IProcessor & transform)
{
    if (transform.getInputs().size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Transform for chain should have single input, but {} has {} inputs",
            transform.getName(),
            transform.getInputs().size());

    if (transform.getInputs().front().isConnected())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Transform for chain has connected input");
}

static void checkSingleOutput(const IProcessor & transform)
{
    if (transform.getOutputs().size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Transform for chain should have single output, but {} has {} outputs",
            transform.getName(),
            transform.getOutputs().size());

    if (transform.getOutputs().front().isConnected())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Transform for chain has connected output");
}

static void checkTransform(const IProcessor & transform)
{
    checkSingleInput(transform);
    checkSingleOutput(transform);
}

static void checkInitialized(const std::list<ProcessorPtr> & processors)
{
    if (processors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chain is not initialized");
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
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot initialize chain because there is a disconnected input for {}",
                    processor->getName());

        for (const auto & output : processor->getOutputs())
            if (&output != &getOutputPort() && !output.isConnected())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot initialize chain because there is a disconnected output for {}",
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

    processors.emplace_back(std::move(processor));
}

void Chain::pushBackChain(Chain chain)
{
    if (processors.empty()) {
        *this = std::move(chain);
        return;
    }

    connect(getOutputPort(), chain.getInputPort());
    processors.splice(processors.end(), std::move(chain.processors));
    attachResources(chain.detachResources());
    num_threads += chain.num_threads;
}

void Chain::pushFrontChain(Chain chain) {
    if (processors.empty()) {
        *this = std::move(chain);
        return;
    }

    connect(chain.getOutputPort(), getInputPort());
    processors.splice(processors.begin(), std::move(chain.processors));
    attachResources(chain.detachResources());
    num_threads += chain.num_threads;
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

void Chain::setRuntimeData(ThreadStatus * thread_status_, std::atomic_uint64_t * elapsed_counter_ms_)
{
    for (auto& processor : processors) {
        if (auto * casted = dynamic_cast<ExceptionKeepingTransform*>(processor.get())) {
            casted->setRuntimeData(thread_status_, elapsed_counter_ms_);
        }
    }
}

void Chain::reset()
{
    Chain to_remove = std::move(*this);
    *this = Chain();
}

}
