#pragma once
#include <Processors/IProcessor.h>
#include <Processors/ISink.h>

namespace DB
{

class NullSink : public IProcessor
{
public:
    explicit NullSink(Block header) : IProcessor({std::move(header)}, {}) {}
    String getName() const override { return "NullSink"; }

    Status prepare() override
    {
        inputs.front().close();
        return Status::Finished;
    }

    InputPort & getPort() { return inputs.front(); }
};

class EmptySink : public ISink
{
public:
    explicit EmptySink(Block header) : ISink(std::move(header)) {}
    String getName() const override { return "EmptySink"; }

protected:
    void consume(Chunk) override {}
};

}
