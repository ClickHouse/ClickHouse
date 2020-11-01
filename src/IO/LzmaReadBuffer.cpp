#include <IO/LzmaReadBuffer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LZMA_STREAM_DECODER_FAILED;
}
LzmaReadBuffer::LzmaReadBuffer(
    std::unique_ptr<ReadBuffer> in_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = nullptr, size_t alignment = 0)
    : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment), in(std::move(in_))
{
    lstr = LZMA_STREAM_INIT;
    lstr.allocator = nullptr;
    lstr.next_in = nullptr;
    lstr.avail_in = 0;
    lstr.next_out = nullptr;
    lstr.avail_out = 0;

    // 500 mb
    uint64_t memlimit = 500 << 30;

    lzma_ret ret = lzma_stream_decoder(&lstr, memlimit, LZMA_CONCATENATED | LZMA_IGNORE_CHECK);
    // lzma does not provide api for converting error code to string unlike zlib
    if (ret != LZMA_OK)
        throw Exception(
            std::string("lzma_stream_decoder failed: error code: ") + std::to_string(ret) + "; lzma version: " + LZMA_VERSION_STRING,
            ErrorCodes::LZMA_STREAM_DECODER_FAILED);
}

LzmaReadBuffer::~LzmaReadBuffer()
{
    lzma_end(&lstr);
}

bool LzmaReadBuffer::nextImpl()
{
    if (eof)
    {
        return false;
    }

    if (!lstr.avail_in)
    {
        in->nextIfAtEnd();
        lstr.next_in = reinterpret_cast<unsigned char *>(in->position());
        lstr.avail_in = in->buffer().end() - in->position();
    }
    lstr.next_out = reinterpret_cast<unsigned char *>(internal_buffer.begin());
    lstr.avail_out = internal_buffer.size();

    lzma_ret ret = lzma_code(&lstr, LZMA_FINISH);

    in->position() = in->buffer().end() - lstr.avail_in;

    if (ret == LZMA_STREAM_END)
    {
        if (in->eof())
        {
            eof = true;
            return working_buffer.size() != 0;
        }
    }

    if (ret != LZMA_OK)
        throw Exception(
            std::string("lzma_stream_decoder failed: error code: ") + std::to_string(ret) + "; lzma version: " + LZMA_VERSION_STRING,
            ErrorCodes::LZMA_STREAM_DECODER_FAILED);

    return true
}
}