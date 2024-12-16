#include "config.h"

#if USE_PROTOBUF
#include <IO/Protobuf/ProtobufZeroCopyInputStreamFromReadBuffer.h>
#include <IO/ReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ProtobufZeroCopyInputStreamFromReadBuffer::ProtobufZeroCopyInputStreamFromReadBuffer(std::unique_ptr<ReadBuffer> in_) : in(std::move(in_))
{
}

ProtobufZeroCopyInputStreamFromReadBuffer::~ProtobufZeroCopyInputStreamFromReadBuffer() = default;

bool ProtobufZeroCopyInputStreamFromReadBuffer::Next(const void ** data, int * size)
{
    if (in->eof())
        return false;
    *data = in->position();
    *size = static_cast<int>(in->available());
    in->position() += *size;
    return true;
}

void ProtobufZeroCopyInputStreamFromReadBuffer::BackUp(int count)
{
    if (static_cast<Int64>(in->offset()) < count)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ProtobufZeroCopyInputStreamFromReadBuffer::BackUp() cannot back up {} bytes (max = {} bytes)",
            count,
            in->offset());

    in->position() -= count;
}

bool ProtobufZeroCopyInputStreamFromReadBuffer::Skip(int count)
{
    return static_cast<Int64>(in->tryIgnore(count)) == count;
}

int64_t ProtobufZeroCopyInputStreamFromReadBuffer::ByteCount() const
{
    return in->count();
}

}

#endif
