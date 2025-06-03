#include "config.h"

#if USE_SNAPPY
#include <cstring>

#include <snappy.h>

#include <Common/ErrorCodes.h>
#include "SnappyWriteBuffer.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int SNAPPY_COMPRESS_FAILED;
}

SnappyWriteBuffer::SnappyWriteBuffer(std::unique_ptr<WriteBuffer> out_, size_t buf_size, char * existing_memory, size_t alignment)
    : SnappyWriteBuffer(*out_, buf_size, existing_memory, alignment)
{
    out_holder = std::move(out_);
}

SnappyWriteBuffer::SnappyWriteBuffer(WriteBuffer & out_, size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment), out(&out_)
{
}

SnappyWriteBuffer::~SnappyWriteBuffer()
{
    finish();
}

void SnappyWriteBuffer::nextImpl()
{
    if (!offset())
    {
        return;
    }

    const char * in_data = reinterpret_cast<const char *>(working_buffer.begin());
    size_t in_available = offset();
    uncompress_buffer.append(in_data, in_available);
}

void SnappyWriteBuffer::finish()
{
    if (finished)
        return;

    try
    {
        finishImpl();
        out->finalize();
        finished = true;
    }
    catch (...)
    {
        /// Do not try to flush next time after exception.
        out->position() = out->buffer().begin();
        finished = true;
        throw;
    }
}

void SnappyWriteBuffer::finishImpl()
{
    next();

    bool success = snappy::Compress(uncompress_buffer.data(), uncompress_buffer.size(), &compress_buffer);
    if (!success)
    {
        throw Exception(ErrorCodes::SNAPPY_COMPRESS_FAILED, "snappy compress failed: ");
    }

    char * in_data = compress_buffer.data();
    size_t in_available = compress_buffer.size();
    char * out_data = nullptr;
    size_t out_capacity = 0;
    size_t len = 0;
    while (in_available > 0)
    {
        out->nextIfAtEnd();
        out_data = out->position();
        out_capacity = out->buffer().end() - out->position();
        len = in_available > out_capacity ? out_capacity : in_available;

        memcpy(out_data, in_data, len);
        in_data += len;
        in_available -= len;
        out->position() += len;
    }
}

}

#endif

