#include <IO/HTTPChunkedWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/hex.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_SMALL_BUFFER_SIZE;
    extern const int TOO_LARGE_ARRAY_SIZE;
}


static constexpr size_t chunk_header_size = 10; // The size of FFFFFFFF\r\n
static constexpr size_t chunk_footer_size = 2; // The size of \r\n


HTTPChunkedWriteBuffer::HTTPChunkedWriteBuffer(WriteBuffer & out_)
    : WriteBuffer(nullptr, 0), out(out_)
{
    setWorkingBuffer();
}

void HTTPChunkedWriteBuffer::setWorkingBuffer()
{
    char * out_buffer_ptr = out.internalBuffer().begin();
    size_t out_buffer_size = out.internalBuffer().size();

    if (out_buffer_size < (chunk_header_size + chunk_footer_size) * 2)
        throw Exception("Too small buffer size in HTTPChunkedWriteBuffer", ErrorCodes::TOO_SMALL_BUFFER_SIZE);
    else if (out_buffer_size > 0xFFFFFFFF)
        throw Exception("Too large buffer size in HTTPChunkedWriteBuffer", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

    working_buffer = Buffer(
        out_buffer_ptr + chunk_header_size,
        out_buffer_ptr + out_buffer_size - chunk_footer_size);
}

void HTTPChunkedWriteBuffer::writeChunkHeader(size_t chunk_size)
{
    char * chunk_header_pos = buffer().begin() - chunk_size;
    writeHexByteUppercase((chunk_size >> 24) & 0xFF, &chunk_header_pos[0]);
    writeHexByteUppercase((chunk_size >> 16) & 0xFF, &chunk_header_pos[2]);
    writeHexByteUppercase((chunk_size >> 8) & 0xFF, &chunk_header_pos[4]);
    writeHexByteUppercase(chunk_size & 0xFF, &chunk_header_pos[6]);
    chunk_header_pos[8] = '\r';
    chunk_header_pos[9] = '\n';
}

void HTTPChunkedWriteBuffer::writeChunkFooter()
{
    pos[0] = '\r';
    pos[1] = '\n';
    pos += 2;
}

void HTTPChunkedWriteBuffer::nextImpl()
{
    size_t chunk_size = offset();
    writeChunkHeader(chunk_size);
    writeChunkFooter();
    out.position() = pos;
    out.next();
    setWorkingBuffer();
}

void HTTPChunkedWriteBuffer::finalize()
{
    writeCString("0\r\n\r\n", out);
}

}


