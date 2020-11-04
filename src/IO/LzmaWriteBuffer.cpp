#include <IO/LzmaWriteBuffer.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LZMA_STREAM_ENCODER_FAILED;
}

LzmaWriteBuffer::LzmaWriteBuffer(
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
        throw Exception(
            std::string("lzma preset failed: ") + "; lzma version: " + LZMA_VERSION_STRING, ErrorCodes::LZMA_STREAM_ENCODER_FAILED);


    lzma_filter filters[] = {
        {.id = LZMA_FILTER_X86, .options = nullptr},
        {.id = LZMA_FILTER_LZMA2, .options = &opt_lzma2},
        {.id = LZMA_VLI_UNKNOWN, .options = nullptr},
    };
    lzma_ret ret = lzma_stream_encoder(&lstr, filters, LZMA_CHECK_CRC64);

    if (ret != LZMA_OK)
        throw Exception(
            std::string("lzma stream encoder init failed: ") + std::to_string(ret) + "; lzma version: " + LZMA_VERSION_STRING,
            ErrorCodes::LZMA_STREAM_ENCODER_FAILED);
}

LzmaWriteBuffer::~LzmaWriteBuffer()
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

void LzmaWriteBuffer::nextImpl()
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
                std::string("lzma stream encoding failed: ") + "; lzma version: " + LZMA_VERSION_STRING,
                ErrorCodes::LZMA_STREAM_ENCODER_FAILED);

    } while (lstr.avail_in > 0 || lstr.avail_out == 0);
}


void LzmaWriteBuffer::finish()
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
                std::string("lzma stream encoding failed: ") + "; lzma version: " + LZMA_VERSION_STRING,
                ErrorCodes::LZMA_STREAM_ENCODER_FAILED);

    } while (lstr.avail_out == 0);
}
}
