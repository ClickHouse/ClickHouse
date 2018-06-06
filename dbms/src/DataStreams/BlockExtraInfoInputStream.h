#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

/** Adds to one stream additional block information that is specified
  * as the constructor parameter.
  */
class BlockExtraInfoInputStream : public IProfilingBlockInputStream
{
public:
    BlockExtraInfoInputStream(const BlockInputStreamPtr & input, const BlockExtraInfo & block_extra_info_)
        : block_extra_info(block_extra_info_)
    {
        children.push_back(input);
    }

    BlockExtraInfo getBlockExtraInfo() const override
    {
        return block_extra_info;
    }

    String getName() const override { return "BlockExtraInfoInput"; }

    Block getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override
    {
        return children.back()->read();
    }

private:
    BlockExtraInfo block_extra_info;
};

}
