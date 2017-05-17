#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_EMPTY_BLOCK_OUTPUT_STREAM;
}

/** When trying to write blocks to this stream of blocks, throws an exception.
  * Used where, in general, you need to pass a stream of blocks, but in some cases, it should not be used.
  */
class EmptyBlockOutputStream : public IBlockOutputStream
{
public:
    void write(const Block & block) override
    {
        throw Exception("Cannot write to EmptyBlockOutputStream", ErrorCodes::CANNOT_WRITE_TO_EMPTY_BLOCK_OUTPUT_STREAM);
    }
};

}
