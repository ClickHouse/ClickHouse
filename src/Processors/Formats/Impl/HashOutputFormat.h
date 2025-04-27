#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <IO/HashingWriteBuffer.h>

namespace DB
{

class HashOutputFormat final : public IOutputFormat
{
public:
    HashOutputFormat(const Block & header, WriteBuffer & out, const String & algorithm);
    String getName() const override;
    String getHash();

protected:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;

private:
    HashingWriteBuffer hashing_buffer;
};

class FormatFactory;
void registerOutputFormatHash(FormatFactory & factory);

}
