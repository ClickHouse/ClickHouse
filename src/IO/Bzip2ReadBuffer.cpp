#include <Common/config.h>

#if USE_BZIP2
#    include <IO/Bzip2ReadBuffer.h>
#    include <bzlib.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BZIP2_STREAM_DECODER_FAILED;
    extern const int UNEXPECTED_END_OF_FILE;
}


class Bzip2ReadBuffer::Bzip2StateWrapper
{
public:
    Bzip2StateWrapper()
    {
        memset(&stream, 0, sizeof(stream));

        int ret = BZ2_bzDecompressInit(&stream, 0, 0);

        if (ret != BZ_OK)
            throw Exception(
                ErrorCodes::BZIP2_STREAM_DECODER_FAILED,
                "bzip2 stream encoder init failed: error code: {}",
                ret);
    }

    ~Bzip2StateWrapper()
    {
        BZ2_bzDecompressEnd(&stream);
    }

    bz_stream stream;
};

Bzip2ReadBuffer::Bzip2ReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t buf_size, char *existing_memory, size_t alignment)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
    , bz(std::make_unique<Bzip2StateWrapper>())
    , eof_flag(false)
{
}

Bzip2ReadBuffer::~Bzip2ReadBuffer() = default;

bool Bzip2ReadBuffer::nextImpl()
{
    if (eof_flag)
        return false;

    int ret;
    do
    {
        if (!bz->stream.avail_in)
        {
            in->nextIfAtEnd();
            bz->stream.avail_in = in->buffer().end() - in->position();
            bz->stream.next_in = in->position();
        }

        bz->stream.avail_out = internal_buffer.size();
        bz->stream.next_out = internal_buffer.begin();

        ret = BZ2_bzDecompress(&bz->stream);

        in->position() = in->buffer().end() - bz->stream.avail_in;
    }
    while (bz->stream.avail_out == internal_buffer.size() && ret == BZ_OK && !in->eof());

    working_buffer.resize(internal_buffer.size() - bz->stream.avail_out);

    if (ret == BZ_STREAM_END)
    {
        if (in->eof())
        {
            eof_flag = true;
            return !working_buffer.empty();
        }
        else
        {
            throw Exception(
                ErrorCodes::BZIP2_STREAM_DECODER_FAILED,
                "bzip2 decoder finished, but input stream has not exceeded: error code: {}", ret);
        }
    }

    if (ret != BZ_OK)
        throw Exception(
            ErrorCodes::BZIP2_STREAM_DECODER_FAILED,
            "bzip2 stream decoder failed: error code: {}",
            ret);

    if (in->eof())
    {
        eof_flag = true;
        throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "Unexpected end of bzip2 archive");
    }

    return true;
}
}

#endif
