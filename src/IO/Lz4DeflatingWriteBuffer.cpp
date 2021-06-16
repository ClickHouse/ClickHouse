#include "Lz4DeflatingWriteBuffer.h"
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LZ4_ENCODER_FAILED;
}

Lz4DeflatingWriteBuffer::Lz4DeflatingWriteBuffer(
    std::unique_ptr<WriteBuffer> out_, int  /*compression_level*/, size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment), out(std::move(out_))
{
}

Lz4DeflatingWriteBuffer::~Lz4DeflatingWriteBuffer()
{
    /// TODO: Implementation
}

void Lz4DeflatingWriteBuffer::nextImpl()
{
    /// TODO: Implementation
}

void Lz4DeflatingWriteBuffer::finish()
{
    /// TODO: Implementation
}

void Lz4DeflatingWriteBuffer::finishImpl()
{
    /// TODO: Implementation
}

}
