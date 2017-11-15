#pragma once
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class DictionaryBlockInputStreamBase : public IProfilingBlockInputStream
{
protected:
    Block block;

    DictionaryBlockInputStreamBase(size_t rows_count, size_t max_block_size);

    String getID() const override;

    virtual Block getBlock(size_t start, size_t length) const = 0;

private:
    const size_t rows_count;
    const size_t max_block_size;
    size_t next_row;

    Block readImpl() override;
    void readPrefixImpl() override { next_row = 0; }
};

}
