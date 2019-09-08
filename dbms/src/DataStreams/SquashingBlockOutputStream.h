#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/SquashingTransform.h>


namespace DB
{

/** Merging consecutive blocks of stream to specified minimum size.
  */
class SquashingBlockOutputStream : public IBlockOutputStream
{
public:
    SquashingBlockOutputStream(BlockOutputStreamPtr dst, Block header_, size_t min_block_size_rows, size_t min_block_size_bytes, bool disable_flush_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;
    void writePrefix() override;
    void writeSuffix() override;

private:
    BlockOutputStreamPtr output;
    Block header;

    SquashingTransform transform;
    bool all_written = false;

    void finalize();

    bool disable_flush;
};

}
