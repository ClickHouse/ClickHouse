#pragma once
#include <Processors/ISource.h>


namespace DB
{

class NullSource : public ISource
{
public:
    explicit NullSource(SharedHeader header) : ISource(std::move(header)) {}
    String getName() const override { return "NullSource"; }

protected:
    Chunk generate() override { return Chunk(); }
};

}
