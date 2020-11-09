#include <IO/LZMAInflatingReadBuffer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LZMA_STREAM_DECODER_FAILED;
}
LZMAInflatingReadBuffer::LZMAInflatingReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment), in(std::move(in_)), eof(false)
{
    // FL2_createDStreamMt(number of threads)
    lstr = FL2_createDStreamMt(2);
    /* size_t res = */ FL2_initDStream(lstr);
    /*
    lstr = LZMA_STREAM_INIT;
    lstr.allocator = nullptr;
    lstr.next_in = nullptr;
    lstr.avail_in = 0;
    lstr.next_out = nullptr;
    lstr.avail_out = 0;

    // 500 mb
    uint64_t memlimit = 500 << 20;

    lzma_ret ret = lzma_stream_decoder(&lstr, memlimit, LZMA_CONCATENATED);
    // lzma does not provide api for converting error code to string unlike zlib
    if (ret != LZMA_OK)
        throw Exception(
            std::string("lzma_stream_decoder initialization failed: error code: ") + std::to_string(ret)
                + "; lzma version: " + LZMA_VERSION_STRING,
            ErrorCodes::LZMA_STREAM_DECODER_FAILED);
            */
}

LZMAInflatingReadBuffer::~LZMAInflatingReadBuffer()
{
    //lzma_end(&lstr);
}

bool LZMAInflatingReadBuffer::nextImpl()
{
    /*
    if (eof)
        return false;

    if (!lstr.avail_in)
    {
        in->nextIfAtEnd();
        lstr.next_in = reinterpret_cast<unsigned char *>(in->position());
        lstr.avail_in = in->buffer().end() - in->position();
    }
    lstr.next_out = reinterpret_cast<unsigned char *>(internal_buffer.begin());
    lstr.avail_out = internal_buffer.size();

    lzma_ret ret = lzma_code(&lstr, LZMA_RUN);

    in->position() = in->buffer().end() - lstr.avail_in;
    working_buffer.resize(internal_buffer.size() - lstr.avail_out);

    if (ret == LZMA_STREAM_END)
    {
        if (in->eof())
        {
            eof = true;
            return working_buffer.size() != 0;
        }
        else
        {
            throw Exception(
                ErrorCodes::LZMA_STREAM_DECODER_FAILED,
                "lzma decoder finished, but stream is still alive: error code: {}; lzma version: {}",
                ret,
                LZMA_VERSION_STRING);
        }
    }

    if (ret != LZMA_OK)
        throw Exception(
            ErrorCodes::LZMA_STREAM_DECODER_FAILED,
            "lzma_stream_decoder failed: error code: error codeL {}; lzma version: {}",
            ret,
            LZMA_VERSION_STRING);

    return true;
    */
    return true;
}
}
