#pragma once

#include <DataStreams/IBlockOutputStream.h>

namespace DB
{

class AddingVersionsBlockOutputStream : public IBlockOutputStream
{
public:
    AddingVersionsBlockOutputStream(size_t & version_, const BlockOutputStreamPtr & output_)
        : version(version_), output(output_)
    {
    }

    Block getHeader() const override;

    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

private:
    size_t & version;
    BlockOutputStreamPtr output;

    std::atomic<size_t> written_rows{0}, written_bytes{0};

public:
    size_t getWrittenRows() { return written_rows; }
    size_t getWrittenBytes() { return written_bytes; }
};

}
