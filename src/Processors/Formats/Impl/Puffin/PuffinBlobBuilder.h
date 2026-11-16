#pragma once

#include <memory>

namespace DB
{

class Block;
class Chunk;
class PuffinWriter;
struct FormatSettings;

class IPuffinBlobBuilder
{
public:
    virtual ~IPuffinBlobBuilder() = default;

    virtual void consume(const Chunk & chunk) = 0;
    virtual void write(PuffinWriter & writer) = 0;
    virtual void reset() = 0;
};

using PuffinBlobBuilderPtr = std::unique_ptr<IPuffinBlobBuilder>;

PuffinBlobBuilderPtr createPuffinBlobBuilder(const Block & header, const FormatSettings & format_settings);

}
