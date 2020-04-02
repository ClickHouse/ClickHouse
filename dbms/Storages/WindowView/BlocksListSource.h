#pragma once

#include <Processors/Sources/SourceWithProgress.h>


namespace DB
{
using BlocksListPtr = std::shared_ptr<BlocksList>;

/** A stream of blocks from a shared list of blocks
  */
class BlocksListSource : public SourceWithProgress
{
public:
    BlocksListSource(const BlocksListPtr & blocks_ptr_, Block header)
        : SourceWithProgress(std::move(header))
        , blocks(blocks_ptr_), it(blocks_ptr_->begin()), end(blocks_ptr_->end()) {}

    String getName() const override { return "BlocksList"; }

protected:
    Chunk generate() override
    {
        if (it == end)
            return {};

        Block res = *it;
        ++it;
        return Chunk(res.getColumns(), res.rows());
    }

private:
    BlocksListPtr blocks;
    BlocksList::iterator it;
    const BlocksList::iterator end;
};

}
