#pragma once

#include <Processors/Sources/SourceWithProgress.h>

namespace DB
{
class DictionarySourceBase : public SourceWithProgress
{
protected:
    DictionarySourceBase(const Block & header, size_t rows_count_, size_t max_block_size_);

    virtual Block getBlock(size_t start, size_t length) const = 0;

private:
    const size_t rows_count;
    const size_t max_block_size;
    size_t next_row = 0;

    Chunk generate() override;
};

}
