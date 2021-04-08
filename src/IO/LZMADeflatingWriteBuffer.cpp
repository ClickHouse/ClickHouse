#include <IO/LZMADeflatingWriteBuffer.h>

#if !defined(ARCADIA_BUILD)

namespace DB
{
namespace ErrorCodes
{
    extern const int LZMA_STREAM_ENCODER_FAILED;
}

LZMADeflatingWriteBuffer::LZMADeflatingWriteBuffer(
    std::unique_ptr<WriteBuffer> out_, int compression_level, size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment), out(std::move(out_))
{

    lstr = LZMA_STREAM_INIT;
    lstr.allocator = nullptr;
    lstr.next_in = nullptr;
    lstr.avail_in = 0;
    lstr.next_out = nullptr;
    lstr.avail_out = 0;

    // options for further compression
    lzma_options_lzma opt_lzma2;
    if (lzma_lzma_preset(&opt_lzma2, compression_level))
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma preset failed: lzma version: {}", LZMA_VERSION_STRING);


    // LZMA_FILTER_X86 -
    // LZMA2 - codec for *.xz files compression; LZMA is not suitable for this purpose
    // VLI - variable length integer (in *.xz most integers encoded as VLI)
    // LZMA_VLI_UNKNOWN (UINT64_MAX) - VLI value to denote that the value is unknown
    lzma_filter filters[] = {
        {.id = LZMA_FILTER_X86, .options = nullptr},
        {.id = LZMA_FILTER_LZMA2, .options = &opt_lzma2},
        {.id = LZMA_VLI_UNKNOWN, .options = nullptr},
    };
    lzma_ret ret = lzma_stream_encoder(&lstr, filters, LZMA_CHECK_CRC64);

    if (ret != LZMA_OK)
        throw Exception(
            ErrorCodes::LZMA_STREAM_ENCODER_FAILED,
            "lzma stream encoder init failed: error code: {} lzma version: {}",
            ret,
            LZMA_VERSION_STRING);
}

LZMADeflatingWriteBuffer::~LZMADeflatingWriteBuffer()
{
    try
    {
        finish();

        lzma_end(&lstr);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void LZMADeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    lstr.next_in = reinterpret_cast<unsigned char *>(working_buffer.begin());
    lstr.avail_in = offset();

    lzma_action action = LZMA_RUN;
    do
    {
        out->nextIfAtEnd();
        lstr.next_out = reinterpret_cast<unsigned char *>(out->position());
        lstr.avail_out = out->buffer().end() - out->position();

        lzma_ret ret = lzma_code(&lstr, action);
        out->position() = out->buffer().end() - lstr.avail_out;

        if (ret == LZMA_STREAM_END)
            return;

        if (ret != LZMA_OK)
            throw Exception(
                ErrorCodes::LZMA_STREAM_ENCODER_FAILED,
                "lzma stream encoding failed: error code: {}; lzma_version: {}",
                ret,
                LZMA_VERSION_STRING);

    } while (lstr.avail_in > 0 || lstr.avail_out == 0);
}


void LZMADeflatingWriteBuffer::finish()
{
    if (finished)
        return;

    next();

    do
    {
        out->nextIfAtEnd();
        lstr.next_out = reinterpret_cast<unsigned char *>(out->position());
        lstr.avail_out = out->buffer().end() - out->position();

        lzma_ret ret = lzma_code(&lstr, LZMA_FINISH);
        out->position() = out->buffer().end() - lstr.avail_out;

        if (ret == LZMA_STREAM_END)
        {
            finished = true;
            return;
        }

        if (ret != LZMA_OK)
            throw Exception(
                ErrorCodes::LZMA_STREAM_ENCODER_FAILED,
                "lzma stream encoding failed: error code: {}; lzma version: {}",
                ret,
                LZMA_VERSION_STRING);

    } while (lstr.avail_out == 0);
}
}

#endif
