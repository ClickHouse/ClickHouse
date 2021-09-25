#include <IO/ZlibInflatingReadBuffer.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ZLIB_INFLATE_FAILED;
}

ZlibInflatingReadBuffer::ZlibInflatingReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        CompressionMethod compression_method,
        size_t buf_size,
        char * existing_memory,
        size_t alignment)
    : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment)
    , in(std::move(in_))
    , eof(false)
{
    zstr.zalloc = nullptr;
    zstr.zfree = nullptr;
    zstr.opaque = nullptr;
    zstr.next_in = nullptr;
    zstr.avail_in = 0;
    zstr.next_out = nullptr;
    zstr.avail_out = 0;

    int window_bits = 15;
    if (compression_method == CompressionMethod::Gzip)
    {
        window_bits += 16;
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    int rc = inflateInit2(&zstr, window_bits);
#pragma GCC diagnostic pop

    if (rc != Z_OK)
        throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflateInit2 failed: {}; zlib version: {}.", zError(rc), ZLIB_VERSION);
}

ZlibInflatingReadBuffer::~ZlibInflatingReadBuffer()
{
    inflateEnd(&zstr);
}

bool ZlibInflatingReadBuffer::nextImpl()
{
    /// Need do-while loop to prevent situation, when
    /// eof was not reached, but working buffer became empty (when nothing was decompressed in current iteration)
    /// (this happens with compression algorithms, same idea is implemented in ZstdInflatingReadBuffer)
    do
    {
        /// if we already found eof, we shouldn't do anything
        if (eof)
            return false;

        /// if there is no available bytes in zstr, move ptr to next available data
        if (!zstr.avail_in)
        {
            in->nextIfAtEnd();
            zstr.next_in = reinterpret_cast<unsigned char *>(in->position());
            zstr.avail_in = in->buffer().end() - in->position();
        }
        /// init output bytes (place, where decompressed data will be)
        zstr.next_out = reinterpret_cast<unsigned char *>(internal_buffer.begin());
        zstr.avail_out = internal_buffer.size();

        int rc = inflate(&zstr, Z_NO_FLUSH);

        /// move in stream on place, where reading stopped
        in->position() = in->buffer().end() - zstr.avail_in;
        /// change size of working buffer (it's size equal to internal_buffer size without unused uncompressed values)
        working_buffer.resize(internal_buffer.size() - zstr.avail_out);

        /// If end was reached, it can be end of file or end of part (for example, chunk)
        if (rc == Z_STREAM_END)
        {
            /// if it is end of file, remember this and return
            /// * true if we can work with working buffer (we still have something to read, so next must return true)
            /// * false if there is no data in working buffer
            if (in->eof())
            {
                eof = true;
                return !working_buffer.empty();
            }
            /// If it is not end of file, we need to reset zstr and return true, because we still have some data to read
            else
            {
                rc = inflateReset(&zstr);
                if (rc != Z_OK)
                    throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflateReset failed: {}", zError(rc));
                return true;
            }
        }
        /// If it is not end and not OK, something went wrong, throw exception
        if (rc != Z_OK)
            throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflateReset failed: {}", zError(rc));
    }
    while (working_buffer.empty());

    /// if code reach this section, working buffer is not empty, so we have some data to process
    return true;
}

}
