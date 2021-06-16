#include "Lz4InflatingReadBuffer.h"

namespace DB
{
namespace ErrorCodes
{
extern const int LZ4_DECODER_FAILED;
}

Lz4InflatingReadBuffer::Lz4InflatingReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment), in(std::move(in_))
{
    /// TODO: Implementation
}

Lz4InflatingReadBuffer::~Lz4InflatingReadBuffer()
{
   /// TODO: Implementation
}

bool Lz4InflatingReadBuffer::nextImpl()
{
    /// TODO: Implementation
    return true;
}

}
