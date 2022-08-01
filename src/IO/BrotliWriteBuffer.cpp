#include <Common/config.h>

#if USE_BROTLI
#    include <IO/BrotliWriteBuffer.h>
#    include <brotli/encode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BROTLI_WRITE_FAILED;
}


class BrotliWriteBuffer::BrotliStateWrapper
{
public:
    BrotliStateWrapper()
    : state(BrotliEncoderCreateInstance(nullptr, nullptr, nullptr))
    {
    }

    ~BrotliStateWrapper()
    {
        BrotliEncoderDestroyInstance(state);
    }

    BrotliEncoderState * state;
};

BrotliWriteBuffer::BrotliWriteBuffer(std::unique_ptr<WriteBuffer> out_, int compression_level, size_t buf_size, char * existing_memory, size_t alignment)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment)
    , brotli(std::make_unique<BrotliStateWrapper>())
    , in_available(0)
    , in_data(nullptr)
    , out_capacity(0)
    , out_data(nullptr)
{
    BrotliEncoderSetParameter(brotli->state, BROTLI_PARAM_QUALITY, static_cast<uint32_t>(compression_level));
    // Set LZ77 window size. According to brotli sources default value is 24 (c/tools/brotli.c:81)
    BrotliEncoderSetParameter(brotli->state, BROTLI_PARAM_LGWIN, 24);
}

BrotliWriteBuffer::~BrotliWriteBuffer() = default;

void BrotliWriteBuffer::nextImpl()
{
    if (!offset())
    {
        return;
    }

    in_data = reinterpret_cast<unsigned char *>(working_buffer.begin());
    in_available = offset();

    try
    {
        do
        {
            out->nextIfAtEnd();
            out_data = reinterpret_cast<unsigned char *>(out->position());
            out_capacity = out->buffer().end() - out->position();

            int result = BrotliEncoderCompressStream(
                    brotli->state,
                    in_available ? BROTLI_OPERATION_PROCESS : BROTLI_OPERATION_FINISH,
                    &in_available,
                    &in_data,
                    &out_capacity,
                    &out_data,
                    nullptr);

            out->position() = out->buffer().end() - out_capacity;

            if (result == 0)
            {
                throw Exception("brotli compress failed", ErrorCodes::BROTLI_WRITE_FAILED);
            }
        }
        while (in_available > 0);
    }
    catch (...)
    {
        /// Do not try to write next time after exception.
        out->position() = out->buffer().begin();
        throw;
    }
}

void BrotliWriteBuffer::finalizeBefore()
{
    next();

    while (true)
    {
        out->nextIfAtEnd();
        out_data = reinterpret_cast<unsigned char *>(out->position());
        out_capacity = out->buffer().end() - out->position();

        int result = BrotliEncoderCompressStream(
                brotli->state,
                BROTLI_OPERATION_FINISH,
                &in_available,
                &in_data,
                &out_capacity,
                &out_data,
                nullptr);

        out->position() = out->buffer().end() - out_capacity;

        if (BrotliEncoderIsFinished(brotli->state))
        {
            return;
        }

        if (result == 0)
        {
            throw Exception("brotli compress failed", ErrorCodes::BROTLI_WRITE_FAILED);
        }
    }
}

}

#endif
