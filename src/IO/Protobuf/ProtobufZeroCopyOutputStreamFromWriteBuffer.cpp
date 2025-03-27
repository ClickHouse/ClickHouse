#include "config.h"

#if USE_PROTOBUF
#include <IO/Protobuf/ProtobufZeroCopyOutputStreamFromWriteBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ProtobufZeroCopyOutputStreamFromWriteBuffer::ProtobufZeroCopyOutputStreamFromWriteBuffer(WriteBuffer & out_) : out(&out_)
{
}

ProtobufZeroCopyOutputStreamFromWriteBuffer::ProtobufZeroCopyOutputStreamFromWriteBuffer(std::unique_ptr<WriteBuffer> out_)
    : ProtobufZeroCopyOutputStreamFromWriteBuffer(*out_)
{
    out_holder = std::move(out_);
}

ProtobufZeroCopyOutputStreamFromWriteBuffer::~ProtobufZeroCopyOutputStreamFromWriteBuffer() = default;

bool ProtobufZeroCopyOutputStreamFromWriteBuffer::Next(void ** data, int * size)
{
    *data = out->position();
    *size = static_cast<int>(out->available());
    out->position() += *size;
    return true;
}

void ProtobufZeroCopyOutputStreamFromWriteBuffer::BackUp(int count)
{
    if (static_cast<Int64>(out->offset()) < count)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ProtobufZeroCopyOutputStreamFromWriteBuffer::BackUp() cannot back up {} bytes (max = {} bytes)",
            count,
            out->offset());

    out->position() -= count;
}

int64_t ProtobufZeroCopyOutputStreamFromWriteBuffer::ByteCount() const
{
    return out->count();
}

void ProtobufZeroCopyOutputStreamFromWriteBuffer::finalize()
{
    out->finalize();
}

}

#endif
