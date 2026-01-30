#include "config.h"

#if USE_BROTLI
#    include <IO/BrotliWriteBuffer.h>
#    include <brotli/encode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BROTLI_WRITE_FAILED;
}


BrotliWriteBuffer::BrotliStateWrapper::BrotliStateWrapper()
: state(BrotliEncoderCreateInstance(nullptr, nullptr, nullptr))
{
}

BrotliWriteBuffer::BrotliStateWrapper::~BrotliStateWrapper()
{
    BrotliEncoderDestroyInstance(state);
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
            const auto * in_data_ptr = in_data;
            out->nextIfAtEnd();
            out_data = reinterpret_cast<unsigned char *>(out->position());
            out_capacity = out->buffer().end() - out->position();

            int result = BrotliEncoderCompressStream(
                    brotli->state,
                    BROTLI_OPERATION_PROCESS,
                    &in_available,
                    &in_data,
                    &out_capacity,
                    &out_data,
                    nullptr);
            total_in += in_data - in_data_ptr;

            out->position() = out->buffer().end() - out_capacity;

            if (result == 0)
            {
                throw Exception(ErrorCodes::BROTLI_WRITE_FAILED, "brotli compress failed");
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

    /// Don't write out if no data was ever compressed
    if (!compress_empty && total_in == 0)
        return;

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
            throw Exception(ErrorCodes::BROTLI_WRITE_FAILED, "brotli compress failed");
        }
    }
}

}

#endif
