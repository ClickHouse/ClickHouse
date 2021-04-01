#pragma once
#include <Processors/IProcessor.h>
#include <Processors/ISink.h>

namespace DB
{

/// Sink which closes input port and reads nothing.
class NullSink : public ISink
{
public:
    explicit NullSink(Block header) : ISink(std::move(header)) {}
    String getName() const override { return "NullSink"; }

    Status prepare() override
    {
        input.close();
        return Status::Finished;
    }
protected:
    void consume(Chunk) override {}
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
