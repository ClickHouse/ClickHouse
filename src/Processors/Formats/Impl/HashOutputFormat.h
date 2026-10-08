#pragma once

#include <Common/SipHash.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Chunk.h>
#include <Core/Block_fwd.h>

namespace DB
{

class WriteBuffer;

/// Computes a single hash value from all columns and rows of the input.
class HashOutputFormat final  : public IOutputFormat
{
public:
    HashOutputFormat(WriteBuffer & out_, SharedHeader header_);
    String getName() const override;

private:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;

    SipHash hash;
};

}
