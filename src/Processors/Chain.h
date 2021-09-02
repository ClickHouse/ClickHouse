#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

class Chain
{
public:
    Chain() = default;
    Chain(Chain &&) = default;
    Chain(const Chain &) = delete;

    Chain & operator=(Chain &&) = default;
    Chain & operator=(const Chain &) = delete;

    explicit Chain(ProcessorPtr processor);
    explicit Chain(std::list<ProcessorPtr> processors);

    bool empty() const { return processors.empty(); }

    void addSource(ProcessorPtr processor);
    void addSink(ProcessorPtr processor);

    IProcessor & getSource();
    IProcessor & getSink();

    InputPort & getInputPort() const;
    OutputPort & getOutputPort() const;

    const Block & getInputHeader() const { return getInputPort().getHeader(); }
    const Block & getOutputHeader() const { return getOutputPort().getHeader(); }

    static std::list<ProcessorPtr> getProcessors(Chain chain) { return std::move(chain.processors); }

private:
    /// -> source -> transform -> ... -> transform -> sink ->
    ///  ^        ->           ->     ->           ->       ^
    ///  input port                               output port
    std::list<ProcessorPtr> processors;
};

}
