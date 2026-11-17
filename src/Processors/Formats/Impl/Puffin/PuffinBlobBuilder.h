#pragma once

#include <memory>

namespace DB
{

class Block;
class Chunk;
class PuffinWriter;

class IPuffinBlobBuilder
{
public:
    virtual ~IPuffinBlobBuilder() = default;

    virtual void consume(const Chunk & chunk) = 0;
    virtual void write(PuffinWriter & writer) = 0;
    virtual void clear() = 0;
};

using PuffinBlobBuilderPtr = std::unique_ptr<IPuffinBlobBuilder>;

}
