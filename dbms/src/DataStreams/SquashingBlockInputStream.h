#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingTransform.h>


namespace DB
{

/** Merging consecutive blocks of stream to specified minimum size.
  */
class SquashingBlockInputStream : public IProfilingBlockInputStream
{
public:
    SquashingBlockInputStream(BlockInputStreamPtr & src, size_t min_block_size_rows, size_t min_block_size_bytes);

    String getName() const override { return "Squashing"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Squashing(" << children.at(0)->getID() << ")";
        return res.str();
    }

protected:
    Block readImpl() override;

private:
    SquashingTransform transform;
    bool all_read = false;
};

}
