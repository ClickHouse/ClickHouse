#include "config.h"

#if USE_BZIP2
#    include <IO/Bzip2WriteBuffer.h>
#    include <bzlib.h>

#include <Common/MemoryTracker.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BZIP2_STREAM_ENCODER_FAILED;
}

Bzip2WriteBuffer::~Bzip2WriteBuffer()
{
    BZ2_bzCompressEnd(&stream);
}

void Bzip2WriteBuffer::nextImpl()
{
    if (!offset())
    {
        return;
    }

    stream.next_in = working_buffer.begin();
    stream.avail_in = static_cast<unsigned>(offset());

    try
    {
        do
        {
            out->nextIfAtEnd();
            stream.next_out = out->position();
            stream.avail_out = static_cast<unsigned>(out->buffer().end() - out->position());

            int ret = BZ2_bzCompress(&stream, BZ_RUN);

            out->position() = out->buffer().end() - stream.avail_out;

            if (ret != BZ_RUN_OK)
                throw Exception(
                    ErrorCodes::BZIP2_STREAM_ENCODER_FAILED,
                    "bzip2 stream encoder failed: error code: {}",
                    ret);

        }
        while (stream.avail_in > 0);

        total_in += offset();
    }
    catch (...)
    {
        /// Do not try to write next time after exception.
        out->position() = out->buffer().begin();
        throw;
    }
}

void Bzip2WriteBuffer::finalizeBefore()
{
    next();

    /// Don't write out if no data was ever compressed
    if (!compress_empty && total_in == 0)
        return;

    do
    {
        out->nextIfAtEnd();
        stream.next_out = out->position();
        stream.avail_out = static_cast<unsigned>(out->buffer().end() - out->position());

        int ret = BZ2_bzCompress(&stream, BZ_FINISH);

        out->position() = out->buffer().end() - stream.avail_out;

        if (ret == BZ_STREAM_END)
            break;

        if (ret != BZ_FINISH_OK)
            throw Exception(
                ErrorCodes::BZIP2_STREAM_ENCODER_FAILED,
                "bzip2 stream encoder failed: error code: {}",
                ret);
    }
    while (true);
}

}

#endif
