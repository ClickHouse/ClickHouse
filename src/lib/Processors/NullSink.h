#pragma once
#include <Processors/IProcessor.h>

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

}
