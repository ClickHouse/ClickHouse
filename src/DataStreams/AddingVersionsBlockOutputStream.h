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
};

}
