#include <IO/Base64WriteBuffer.h>

#if USE_SIMDUTF

#include <simdutf.h>

#include <algorithm>
#include <cstring>

namespace DB
{

Base64WriteBuffer::Base64WriteBuffer(WriteBuffer & out_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , out(out_)
{
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
    /// The input chunk is a multiple of 3 so each `binary_to_base64` call encodes only whole groups (no
    /// padding) until the final group, which is emitted from `carry` in `finalizeImpl`.
    static constexpr size_t input_chunk = 3072;
    /// Up to two carried-over bytes are prepended, so stage at most `input_chunk + 2` bytes per call.
    char input[input_chunk + 2];
    char encoded[(input_chunk + 2) / 3 * 4 + 4];

    const char * src = working_buffer.begin();
    size_t left = offset();
    while (left > 0)
    {
        size_t staged = carry_size;
        memcpy(input, carry, carry_size);

        const size_t take = std::min(left, input_chunk);
        memcpy(input + staged, src, take);
        staged += take;
        src += take;
        left -= take;

        const size_t aligned = staged / 3 * 3;
        if (aligned)
        {
            const size_t encoded_size = simdutf::binary_to_base64(input, aligned, encoded, simdutf::base64_default);
            out.write(encoded, encoded_size);
        }

        carry_size = staged - aligned;
        memcpy(carry, input + aligned, carry_size);
    }
}

void Base64WriteBuffer::finalizeImpl()
{
    next();

    /// Emit the padded final group for the carried 1-2 bytes, if any. Never finalizes `out`.
    if (carry_size)
    {
        char tail[4];
        const size_t encoded_size
            = simdutf::binary_to_base64(reinterpret_cast<const char *>(carry), carry_size, tail, simdutf::base64_default);
        out.write(tail, encoded_size);
        carry_size = 0;
    }
}

}

#endif
