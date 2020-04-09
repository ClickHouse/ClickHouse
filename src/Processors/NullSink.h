#pragma once
#include <Processors/IProcessor.h>
#include <Processors/ISink.h>

namespace DB
{

/// Sink which closes input port and reads nothing.
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

/// Sink which reads everything and do nothing with it.
class EmptySink : public ISink
{
public:
    explicit EmptySink(Block header) : ISink(std::move(header)) {}
    String getName() const override { return "EmptySink"; }

protected:
    void consume(Chunk) override {}
};

}
