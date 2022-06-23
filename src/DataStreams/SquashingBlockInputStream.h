#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/SquashingTransform.h>


namespace DB
{

/** Merging consecutive blocks of stream to specified minimum size.
  */
class SquashingBlockInputStream : public IBlockInputStream
{
public:
    SquashingBlockInputStream(const BlockInputStreamPtr & src, size_t min_block_size_rows, size_t min_block_size_bytes,
                              bool reserve_memory = false);

    String getName() const override { return "Squashing"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    Block header;
    SquashingTransform transform;
    bool all_read = false;
};

}
