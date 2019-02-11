#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace DB
{


/** Combines several sources into one.
  * Unlike UnionBlockInputStream, it does this sequentially.
  * Blocks of different sources are not interleaved with each other.
  */
class ConcatBlockInputStream : public IBlockInputStream
{
public:
    ConcatBlockInputStream(BlockInputStreams inputs_)
    {
        children.insert(children.end(), inputs_.begin(), inputs_.end());
        current_stream = children.begin();
    }

    String getName() const override { return "Concat"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

    /// We call readSuffix prematurely by ourself. Suppress default behaviour.
    void readSuffix() override {}

protected:
    Block readImpl() override
    {
        Block res;

        while (current_stream != children.end())
        {
            res = (*current_stream)->read();

            if (res)
                break;
            else
            {
                (*current_stream)->readSuffix();
                ++current_stream;
            }
        }

        return res;
    }

private:
    BlockInputStreams::iterator current_stream;
};

}
