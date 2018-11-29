#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class DictionaryBlockInputStreamBase : public IProfilingBlockInputStream
{
protected:
    DictionaryBlockInputStreamBase(size_t rows_count, size_t max_block_size);

    virtual Block getBlock(size_t start, size_t length) const = 0;

    Block getHeader() const override;

private:
    const size_t rows_count;
    const size_t max_block_size;
    size_t next_row = 0;

    Block readImpl() override;
};

}
