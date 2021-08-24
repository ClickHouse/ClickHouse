#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

/** A stream of blocks from which you can read the next block from an explicitly provided list.
  * Also see OneBlockInputStream.
  */
class BlocksListBlockInputStream : public IBlockInputStream
{
public:
    /// Acquires the ownership of the block list.
    BlocksListBlockInputStream(BlocksList && list_)
        : list(std::move(list_)), it(list.begin()), end(list.end()) {}

    /// Uses a list of blocks lying somewhere else.
    BlocksListBlockInputStream(BlocksList::iterator & begin_, BlocksList::iterator & end_)
        : it(begin_), end(end_) {}

    String getName() const override { return "BlocksList"; }

protected:
    Block getHeader() const override { return list.empty() ? Block() : *list.begin(); }

    Block readImpl() override
    {
        if (it == end)
            return Block();

        Block res = *it;
        ++it;
        return res;
    }

private:
    BlocksList list;
    BlocksList::iterator it;
    const BlocksList::iterator end;
};

}
