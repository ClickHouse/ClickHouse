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
    SquashingBlockOutputStream(BlockOutputStreamPtr & dst, const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;
    void writePrefix() override;
    void writeSuffix() override;

    /// Don't write blocks less than specified size even when flush method was called by user.
    void disableFlush() { disable_flush = true; }

private:
    BlockOutputStreamPtr output;
    Block header;

    SquashingTransform transform;
    bool all_written = false;

    void finalize();

    bool disable_flush = false;
};

}
