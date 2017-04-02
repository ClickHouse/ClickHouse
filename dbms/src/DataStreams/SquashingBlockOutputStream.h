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
    SquashingBlockOutputStream(BlockOutputStreamPtr & dst, size_t min_block_size_rows, size_t min_block_size_bytes);

    void write(const Block & block) override;

    void flush() override;
    void writePrefix() override;
    void writeSuffix() override;

private:
    BlockOutputStreamPtr output;

    SquashingTransform transform;
    bool all_written = false;

    void finalize();
};

}
