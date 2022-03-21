#pragma once
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{

class NullOutputFormat final : public IOutputFormat
{
public:
    explicit NullOutputFormat(const Block & header) : IOutputFormat(header, empty_buffer) {}

    String getName() const override { return "Null"; }

protected:
    void consume(Chunk) override {}

private:
    static WriteBuffer empty_buffer;
};

}
