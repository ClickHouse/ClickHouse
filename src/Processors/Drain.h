#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

class Drain
{
public:
    Drain() = default;
    explicit Drain(ProcessorPtr processor);

    void addTransform(ProcessorPtr processor);

    InputPort & getPort() const;
    const Block & getHeader() const { return getPort().getHeader(); }

private:
    Processors processors;
};

}
