#include <IO/ZlibDeflatingWriteBuffer.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ZLIB_DEFLATE_FAILED;
}

void ZlibDeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    zstr.next_in = reinterpret_cast<unsigned char *>(working_buffer.begin());
    zstr.avail_in = static_cast<unsigned>(offset());

    try
    {
        do
        {
            out->nextIfAtEnd();
            zstr.next_out = reinterpret_cast<unsigned char *>(out->position());
            zstr.avail_out = static_cast<unsigned>(out->buffer().end() - out->position());

            int rc = deflate(&zstr, Z_NO_FLUSH);
            out->position() = out->buffer().end() - zstr.avail_out;

            if (rc != Z_OK)
                throw Exception(ErrorCodes::ZLIB_DEFLATE_FAILED, "deflate failed: {}", zError(rc));
        }
        while (zstr.avail_in > 0 || zstr.avail_out == 0);
    }
    catch (...)
    {
        /// Do not try to write next time after exception.
        out->position() = out->buffer().begin();
        throw;
    }
}

ZlibDeflatingWriteBuffer::~ZlibDeflatingWriteBuffer()
{
    /// It is OK to call deflateEnd() twice (one from the finalizeAfter() that does the proper error checking)
    deflateEnd(&zstr);
}

void ZlibDeflatingWriteBuffer::finalizeBefore()
{
    next();

    /// Don't write out if no data was ever compressed
    if (!compress_empty && zstr.total_out == 0)
        return;

    /// https://github.com/zlib-ng/zlib-ng/issues/494
    do
    {
        out->nextIfAtEnd();
        zstr.next_out = reinterpret_cast<unsigned char *>(out->position());
        zstr.avail_out = static_cast<unsigned>(out->buffer().end() - out->position());

        int rc = deflate(&zstr, Z_FULL_FLUSH);
        out->position() = out->buffer().end() - zstr.avail_out;

        if (rc != Z_OK)
            throw Exception(ErrorCodes::ZLIB_DEFLATE_FAILED, "deflate failed: {}", zError(rc));
    }
    while (zstr.avail_out == 0);

    while (true)
    {
        out->nextIfAtEnd();
        zstr.next_out = reinterpret_cast<unsigned char *>(out->position());
        zstr.avail_out = static_cast<unsigned>(out->buffer().end() - out->position());

        int rc = deflate(&zstr, Z_FINISH);
        out->position() = out->buffer().end() - zstr.avail_out;

        if (rc == Z_STREAM_END)
        {
            return;
        }

        if (rc != Z_OK)
            throw Exception(ErrorCodes::ZLIB_DEFLATE_FAILED, "deflate finalizeImpl() failed: {}", zError(rc));
    }
}

void ZlibDeflatingWriteBuffer::finalizeAfter()
{
    try
    {
        int rc = deflateEnd(&zstr);
        if (rc != Z_OK)
            throw Exception(ErrorCodes::ZLIB_DEFLATE_FAILED, "deflateEnd failed: {}", zError(rc));
    }
    catch (...)
    {
        /// It is OK not to terminate under an error from deflateEnd()
        /// since all data already written to the stream.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
