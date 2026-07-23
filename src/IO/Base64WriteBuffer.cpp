#include <IO/Base64WriteBuffer.h>

#if USE_BASE64

#include <algorithm>

namespace DB
{

Base64WriteBuffer::Base64WriteBuffer(WriteBuffer & out_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , out(out_)
{
    base64_stream_encode_init(&state, 0);
}

Base64WriteBuffer::~Base64WriteBuffer()
{
    if (!finalized && !canceled)
        cancel();
}

void Base64WriteBuffer::nextImpl()
{
    if (!offset())
        return;

    /// base64 emits 4 output bytes per 3 input bytes. Encode through a fixed local buffer and forward with
    /// `out.write`, which copes with any destination buffer size. Encoding straight into `out`'s buffer would
    /// assume it can expose at least four contiguous bytes, which `WriteBuffer` does not guarantee.
    static constexpr size_t input_chunk = 3072;
    /// base64 of `input_chunk` bytes (plus up to two carried over) is at most input_chunk / 3 * 4 bytes;
    /// the extra bytes are the slack libbase64 asks callers to leave past that size.
    char encoded[input_chunk / 3 * 4 + 16];

    const char * src = working_buffer.begin();
    size_t left = offset();
    while (left > 0)
    {
        const size_t input = std::min(left, input_chunk);
        size_t encoded_size = 0;
        base64_stream_encode(&state, src, input, encoded, &encoded_size);
        out.write(encoded, encoded_size);
        src += input;
        left -= input;
    }
}

void Base64WriteBuffer::finalizeImpl()
{
    next();

    /// Emit the padding for the final partial group, if any. Never finalizes `out`.
    char tail[4];
    size_t encoded = 0;
    base64_stream_encode_final(&state, tail, &encoded);
    if (encoded)
        out.write(tail, encoded);
}

}

#endif
