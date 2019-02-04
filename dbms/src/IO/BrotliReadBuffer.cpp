#include "BrotliReadBuffer.h"

namespace DB
{
BrotliReadBuffer::BrotliReadBuffer(ReadBuffer &in_, size_t buf_size, char *existing_memory, size_t alignment)
        : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment)
        , in(in_)
        , eof(false)
{
    bstate_ = BrotliDecoderCreateInstance(NULL,NULL,NULL);
}

BrotliReadBuffer::~BrotliReadBuffer()
{
    BrotliDecoderDestroyInstance(bstate_);
}

bool BrotliReadBuffer::nextImpl()
{
    if (eof)
        return false;

    auto ptr_in = reinterpret_cast<const uint8_t *>(in.position());
    size_t size_in = in.buffer().end() - in.position();

    auto ptr_out = reinterpret_cast<uint8_t *>(internal_buffer.begin());
    size_t size_out = internal_buffer.size();

    BrotliDecoderDecompressStream(bstate_, &size_in, &ptr_in, &size_out, &ptr_out, nullptr);

    in.position() = in.buffer().end() - size_in;
    working_buffer.resize(internal_buffer.size() - size_out);

    if (in.eof()) {
        eof = true;
        return working_buffer.size() != 0;
    }

    return true;
}
}