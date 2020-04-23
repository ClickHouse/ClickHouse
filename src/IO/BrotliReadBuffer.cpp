#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_BROTLI
#    include <brotli/decode.h>
#    include "BrotliReadBuffer.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BROTLI_READ_FAILED;
}


class BrotliReadBuffer::BrotliStateWrapper
{
public:
    BrotliStateWrapper()
        : state(BrotliDecoderCreateInstance(nullptr, nullptr, nullptr))
        , result(BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT)
    {
    }

    ~BrotliStateWrapper()
    {
        BrotliDecoderDestroyInstance(state);
    }

    BrotliDecoderState * state;
    BrotliDecoderResult result;
};

BrotliReadBuffer::BrotliReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t buf_size, char *existing_memory, size_t alignment)
        : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment)
        , in(std::move(in_))
        , brotli(std::make_unique<BrotliStateWrapper>())
        , in_available(0)
        , in_data(nullptr)
        , out_capacity(0)
        , out_data(nullptr)
        , eof(false)
{
}

BrotliReadBuffer::~BrotliReadBuffer() = default;

bool BrotliReadBuffer::nextImpl()
{
    if (eof)
        return false;

    if (!in_available)
    {
        in->nextIfAtEnd();
        in_available = in->buffer().end() - in->position();
        in_data = reinterpret_cast<uint8_t *>(in->position());
    }

    if (brotli->result == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT && (!in_available || in->eof()))
    {
        throw Exception("brotli decode error", ErrorCodes::BROTLI_READ_FAILED);
    }

    out_capacity = internal_buffer.size();
    out_data = reinterpret_cast<uint8_t *>(internal_buffer.begin());

    brotli->result = BrotliDecoderDecompressStream(brotli->state, &in_available, &in_data, &out_capacity, &out_data, nullptr);

    in->position() = in->buffer().end() - in_available;
    working_buffer.resize(internal_buffer.size() - out_capacity);

    if (brotli->result == BROTLI_DECODER_RESULT_SUCCESS)
    {
        if (in->eof())
        {
            eof = true;
            return working_buffer.size() != 0;
        }
        else
        {
            throw Exception("brotli decode error", ErrorCodes::BROTLI_READ_FAILED);
        }
    }

    if (brotli->result == BROTLI_DECODER_RESULT_ERROR)
    {
        throw Exception("brotli decode error", ErrorCodes::BROTLI_READ_FAILED);
    }

    return true;
}
}

#endif
