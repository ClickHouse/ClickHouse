#include <Common/config.h>
#if USE_BROTLI

#include <IO/BrotliWriteBuffer.h>
#include <brotli/encode.h>

namespace DB
{

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

public:
    BrotliEncoderState * state;
};

BrotliWriteBuffer::BrotliWriteBuffer(WriteBuffer & out_, int compression_level, size_t buf_size, char * existing_memory, size_t alignment)
        : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment)
        , brotli(new BrotliStateWrapper())
        , in_available(0)
        , in_data(nullptr)
        , out_capacity(0)
        , out_data(nullptr)
        , out(out_)
{
    BrotliEncoderSetParameter(brotli->state, BROTLI_PARAM_QUALITY, static_cast<uint32_t>(compression_level));
    BrotliEncoderSetParameter(brotli->state, BROTLI_PARAM_LGWIN, 24);
}

BrotliWriteBuffer::~BrotliWriteBuffer()
{
    next();

    in_data = reinterpret_cast<unsigned char *>(working_buffer.end());
    in_available = 0;

    while (true)
    {
        out.nextIfAtEnd();
        out_data = reinterpret_cast<unsigned char *>(out.position());
        out_capacity = out.buffer().end() - out.position();

        BrotliEncoderCompressStream(
                brotli->state,
                BROTLI_OPERATION_FINISH,
                &in_available,
                &in_data,
                &out_capacity,
                &out_data,
                nullptr);

        out.position() = out.buffer().end() - out_capacity;

        finished = true;
        return;
    }
}

void BrotliWriteBuffer::nextImpl()
{
    if (!offset())
    {
        return;
    }

    in_data = reinterpret_cast<unsigned char *>(working_buffer.begin());
    in_available = offset();

    do
    {
        out.nextIfAtEnd();
        out_data = reinterpret_cast<unsigned char *>(out.position());
        out_capacity = out.buffer().end() - out.position();

        int result = BrotliEncoderCompressStream(
                brotli->state,
                in_available ? BROTLI_OPERATION_PROCESS : BROTLI_OPERATION_FINISH,
                &in_available,
                &in_data,
                &out_capacity,
                &out_data,
                nullptr);

        out.position() = out.buffer().end() - out_capacity;

        if (result == 0)
        {
            throw Exception(std::string("brotli compress failed: "), ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);
        }
    }
    while (in_available > 0 || out_capacity == 0);
}

}

#endif