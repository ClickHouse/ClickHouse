#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class Context;

/** Позволяет создать IBlockInputStream или IBlockOutputStream по названию формата.
  * Замечание: формат и сжатие - независимые вещи.
  */
class FormatFactory
{
public:
    BlockInputStreamPtr getInput(const String & name, ReadBuffer & buf,
        const Block & sample, const Context & context, size_t max_block_size) const;

    BlockOutputStreamPtr getOutput(const String & name, WriteBuffer & buf,
        const Block & sample, const Context & context) const;
};

}
