#pragma once

#include <Processors/ISource.h>


namespace DB
{

/// Source that generates chunks with constant columns and
/// size up to max_block_size with total rows total_num_rows.
class ConstChunkGenerator : public ISource
{
public:
    ConstChunkGenerator(Block header, size_t total_num_rows, size_t max_block_size_)
        : ISource(std::move(header))
        , remaining_rows(total_num_rows), max_block_size(max_block_size_)
    {
    }

    String getName() const override { return "ConstChunkGenerator"; }

protected:
    Chunk generate() override
    {
        if (!remaining_rows)
            return {};

        size_t num_rows = std::min(max_block_size, remaining_rows);
        remaining_rows -= num_rows;
        return cloneConstWithDefault(Chunk{getPort().getHeader().getColumns(), 0}, num_rows);
    }

private:
    size_t remaining_rows;
    size_t max_block_size;
};

}
