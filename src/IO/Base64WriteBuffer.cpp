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

    const char * src = working_buffer.begin();
    size_t left = offset();

    /// base64 expands 3 input bytes to 4 output bytes. Feed the input in pieces small enough that the encoded
    /// output of each piece fits into the destination buffer's free space.
    while (left > 0)
    {
        out.nextIfAtEnd();
        const size_t out_available = out.buffer().end() - out.position();
        const size_t max_input = (out_available / 4) * 3;
        if (max_input == 0)
        {
            out.next();
            continue;
        }

        const size_t input = std::min(left, max_input);
        size_t encoded = 0;
        base64_stream_encode(&state, src, input, out.position(), &encoded);
        out.position() += encoded;

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
