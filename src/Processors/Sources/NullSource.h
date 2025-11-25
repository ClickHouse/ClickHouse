#pragma once
#include <Processors/IProcessor.h>
#include <Processors/ISource.h>


namespace DB
{

class NullSource : public ISource
{
public:
    explicit NullSource(SharedHeader header) : ISource(std::move(header)) {}
    String getName() const override { return "NullSource"; }

    ProcessorPtr clone() const override
    {
        return std::make_shared<NullSource>(getOutputs().front().getSharedHeader());
    }

protected:
    Chunk generate() override { return Chunk(); }
};

}
