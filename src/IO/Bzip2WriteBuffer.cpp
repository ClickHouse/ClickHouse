#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_BZIP2
#    include <IO/Bzip2WriteBuffer.h>
#    include <bzlib.h> // Y_IGNORE

#include <Common/MemoryTracker.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BZIP2_STREAM_ENCODER_FAILED;
}


class Bzip2WriteBuffer::Bzip2StateWrapper
{
public:
    explicit Bzip2StateWrapper(int compression_level)
    {
        memset(&stream, 0, sizeof(stream));

        int ret = BZ2_bzCompressInit(&stream, compression_level, 0, 0);

        if (ret != BZ_OK)
            throw Exception(
                ErrorCodes::BZIP2_STREAM_ENCODER_FAILED,
                "bzip2 stream encoder init failed: error code: {}",
                ret);
    }

    ~Bzip2StateWrapper()
    {
        BZ2_bzCompressEnd(&stream);
    }

    bz_stream stream;
};

Bzip2WriteBuffer::Bzip2WriteBuffer(std::unique_ptr<WriteBuffer> out_, int compression_level, size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment)
    , bz(std::make_unique<Bzip2StateWrapper>(compression_level))
    , out(std::move(out_))
{
}

Bzip2WriteBuffer::~Bzip2WriteBuffer()
{
    /// FIXME move final flush into the caller
    MemoryTracker::LockExceptionInThread lock(VariableContext::Global);
    finish();
}

void Bzip2WriteBuffer::nextImpl()
{
    if (!offset())
    {
        return;
    }

    bz->stream.next_in = working_buffer.begin();
    bz->stream.avail_in = offset();

    try
    {
        do
        {
            out->nextIfAtEnd();
            bz->stream.next_out = out->position();
            bz->stream.avail_out = out->buffer().end() - out->position();

            int ret = BZ2_bzCompress(&bz->stream, BZ_RUN);

            out->position() = out->buffer().end() - bz->stream.avail_out;

            if (ret != BZ_RUN_OK)
                throw Exception(
                    ErrorCodes::BZIP2_STREAM_ENCODER_FAILED,
                    "bzip2 stream encoder failed: error code: {}",
                    ret);

        }
        while (bz->stream.avail_in > 0);
    }
    catch (...)
    {
        /// Do not try to write next time after exception.
        out->position() = out->buffer().begin();
        throw;
    }
}

void Bzip2WriteBuffer::finish()
{
    if (finished)
        return;

    try
    {
        finishImpl();
        out->finalize();
        finished = true;
    }
    catch (...)
    {
        /// Do not try to flush next time after exception.
        out->position() = out->buffer().begin();
        finished = true;
        throw;
    }
}

void Bzip2WriteBuffer::finishImpl()
{
    next();

    out->nextIfAtEnd();
    bz->stream.next_out = out->position();
    bz->stream.avail_out = out->buffer().end() - out->position();

    int ret = BZ2_bzCompress(&bz->stream, BZ_FINISH);

    out->position() = out->buffer().end() - bz->stream.avail_out;

    if (ret != BZ_STREAM_END && ret != BZ_FINISH_OK)
        throw Exception(
            ErrorCodes::BZIP2_STREAM_ENCODER_FAILED,
            "bzip2 stream encoder failed: error code: {}",
            ret);
}

}

#endif
