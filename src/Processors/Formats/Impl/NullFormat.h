#pragma once
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{

class WriteBufferFromPointer;

class NullOutputFormat final : public IOutputFormat
{
public:
    explicit NullOutputFormat(const Block & header);

    String getName() const override { return "Null"; }

protected:
    void consume(Chunk) override {}

private:
    static WriteBufferFromPointer empty_buffer;
};

}
