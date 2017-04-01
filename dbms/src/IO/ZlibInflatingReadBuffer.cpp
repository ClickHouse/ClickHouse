#include <IO/ZlibInflatingReadBuffer.h>


namespace DB
{

ZlibInflatingReadBuffer::ZlibInflatingReadBuffer(
        ReadBuffer & in_,
        ZlibCompressionMethod compression_method,
        size_t buf_size,
        char * existing_memory,
        size_t alignment)
    : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment)
    , in(in_)
    , eof(false)
{
    zstr.zalloc    = Z_NULL;
    zstr.zfree     = Z_NULL;
    zstr.opaque    = Z_NULL;
    zstr.next_in   = 0;
    zstr.avail_in  = 0;
    zstr.next_out  = 0;
    zstr.avail_out = 0;

    int window_bits = 15;
    if (compression_method == ZlibCompressionMethod::Gzip)
    {
        window_bits += 16;
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    int rc = inflateInit2(&zstr, window_bits);
#pragma GCC diagnostic pop

    if (rc != Z_OK)
        throw Exception(std::string("inflateInit2 failed: ") + zError(rc), ErrorCodes::ZLIB_INFLATE_FAILED);
}

ZlibInflatingReadBuffer::~ZlibInflatingReadBuffer()
{
    inflateEnd(&zstr);
}

bool ZlibInflatingReadBuffer::nextImpl()
{
    if (eof)
        return false;

    if (!zstr.avail_in)
    {
        in.nextIfAtEnd();
        zstr.next_in = reinterpret_cast<unsigned char *>(in.position());
        zstr.avail_in = in.buffer().end() - in.position();
    }
    zstr.next_out = reinterpret_cast<unsigned char *>(internal_buffer.begin());
    zstr.avail_out = internal_buffer.size();

    int rc = inflate(&zstr, Z_NO_FLUSH);

    in.position() = in.buffer().end() - zstr.avail_in;
    working_buffer.resize(internal_buffer.size() - zstr.avail_out);

    if (rc == Z_STREAM_END)
    {
        if (in.eof())
        {
            eof = true;
            return working_buffer.size() != 0;
        }
        else
        {
            int rc = inflateReset(&zstr);
            if (rc != Z_OK)
                throw Exception(std::string("inflateReset failed: ") + zError(rc), ErrorCodes::ZLIB_INFLATE_FAILED);
            return true;
        }
    }
    if (rc != Z_OK)
        throw Exception(std::string("inflate failed: ") + zError(rc), ErrorCodes::ZLIB_INFLATE_FAILED);

    return true;
}

}
