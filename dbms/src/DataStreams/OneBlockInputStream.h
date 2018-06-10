#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** A stream of blocks from which you can read one block.
  * Also see BlocksListBlockInputStream.
  */
class OneBlockInputStream : public IProfilingBlockInputStream
{
public:
    OneBlockInputStream(const Block & block_) : block(block_) {}

    String getName() const override { return "One"; }

    Block getHeader() const override
    {
        Block res;
        for (const auto & elem : block)
            res.insert({ elem.column->cloneEmpty(), elem.type, elem.name });
        return res;
    }

protected:
    Block readImpl() override
    {
        if (has_been_read)
            return Block();

        has_been_read = true;
        return block;
    }

private:
    Block block;
    bool has_been_read = false;
};

}
