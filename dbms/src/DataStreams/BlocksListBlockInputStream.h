#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** A stream of blocks from which you can read the next block from an explicitly provided list.
  * Also see OneBlockInputStream.
  */
class BlocksListBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// Acquires the ownership of the block list.
    BlocksListBlockInputStream(BlocksList && list_)
        : list(std::move(list_)), it(list.begin()), end(list.end()) {}

    /// Uses a list of blocks lying somewhere else.
    BlocksListBlockInputStream(BlocksList::iterator & begin_, BlocksList::iterator & end_)
        : it(begin_), end(end_) {}

    String getName() const override { return "BlocksList"; }

    String getID() const override
    {
        std::stringstream res;
        res << this;
        return res.str();
    }

protected:
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
