#pragma once

#include <Processors/ISource.h>


namespace DB
{

/** A stream of blocks from which you can read the next block from an explicitly provided list.
  * Also see SourceFromSingleChunk.
  */
class BlocksListSource : public ISource
{
public:
    /// Acquires the ownership of the block list.
    explicit BlocksListSource(BlocksList && list_)
        : ISource(list_.empty() ? Block() : list_.front().cloneEmpty())
        , list(std::move(list_)), it(list.begin()), end(list.end()) {}

    /// Uses a list of blocks lying somewhere else.
    BlocksListSource(BlocksList::iterator & begin_, BlocksList::iterator & end_)
        : ISource(begin_ == end_ ? Block() : begin_->cloneEmpty())
        , it(begin_), end(end_) {}

    String getName() const override { return "BlocksListSource"; }

protected:

    Chunk generate() override
    {
        if (it == end)
            return {};

        Block res = *it;
        ++it;

        size_t num_rows = res.rows();
        return Chunk(res.getColumns(), num_rows);
    }

private:
    BlocksList list;
    BlocksList::iterator it;
    const BlocksList::iterator end;
};

}
