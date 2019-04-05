#pragma once
#include <Processors/ISink.h>

namespace DB
{

class NullSink : public ISink
{
public:
    explicit NullSink(Block header) : ISink(std::move(header)) {}
    String getName() const override { return "NullSink"; }

protected:
    void consume(Chunk) override {}
};

}
