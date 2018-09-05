#include <IO/ZlibDeflatingWriteBuffer.h>


namespace DB
{

ZlibDeflatingWriteBuffer::ZlibDeflatingWriteBuffer(
        WriteBuffer & out_,
        ZlibCompressionMethod compression_method,
        int compression_level,
        size_t buf_size,
        char * existing_memory,
        size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment)
    , out(out_)
{
    zstr.zalloc    = Z_NULL;
    zstr.zfree     = Z_NULL;
    zstr.opaque    = Z_NULL;
    zstr.next_in   = nullptr;
    zstr.avail_in  = 0;
    zstr.next_out  = nullptr;
    zstr.avail_out = 0;

    int window_bits = 15;
    if (compression_method == ZlibCompressionMethod::Gzip)
    {
        window_bits += 16;
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    int rc = deflateInit2(&zstr, compression_level, Z_DEFLATED, window_bits, 8, Z_DEFAULT_STRATEGY);
#pragma GCC diagnostic pop

    if (rc != Z_OK)
        throw Exception(std::string("deflateInit2 failed: ") + zError(rc) + "; zlib version: " + ZLIB_VERSION, ErrorCodes::ZLIB_DEFLATE_FAILED);
}

ZlibDeflatingWriteBuffer::~ZlibDeflatingWriteBuffer()
{
    try
    {
        finish();

        int rc = deflateEnd(&zstr);
        if (rc != Z_OK)
            throw Exception(std::string("deflateEnd failed: ") + zError(rc), ErrorCodes::ZLIB_DEFLATE_FAILED);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ZlibDeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    zstr.next_in = reinterpret_cast<unsigned char *>(working_buffer.begin());
    zstr.avail_in = offset();

    do
    {
        out.nextIfAtEnd();
        zstr.next_out = reinterpret_cast<unsigned char *>(out.position());
        zstr.avail_out = out.buffer().end() - out.position();

        int rc = deflate(&zstr, Z_NO_FLUSH);
        out.position() = out.buffer().end() - zstr.avail_out;

        if (rc != Z_OK)
            throw Exception(std::string("deflate failed: ") + zError(rc), ErrorCodes::ZLIB_DEFLATE_FAILED);
    }
    while (zstr.avail_in > 0 || zstr.avail_out == 0);
}

void ZlibDeflatingWriteBuffer::finish()
{
    if (finished)
        return;

    next();

    while (true)
    {
        out.nextIfAtEnd();
        zstr.next_out = reinterpret_cast<unsigned char *>(out.position());
        zstr.avail_out = out.buffer().end() - out.position();

        int rc = deflate(&zstr, Z_FINISH);
        out.position() = out.buffer().end() - zstr.avail_out;

        if (rc == Z_STREAM_END)
            return;
        if (rc != Z_OK)
            throw Exception(std::string("deflate finish failed: ") + zError(rc), ErrorCodes::ZLIB_DEFLATE_FAILED);
    }

    finished = true;
}

}
