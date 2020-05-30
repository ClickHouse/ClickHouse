#include <IO/HTTPChunkedReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>
#include <Common/Exception.h>
#include <common/find_symbols.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int CORRUPTED_DATA;
}

static constexpr size_t max_chunk_size = 0xFFFFFFFF;

static void skipToCarriageReturnOrEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\r'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }
    }
}

HTTPChunkedReadBuffer::HTTPChunkedReadBuffer(ReadBuffer & in_)
    : in(in_)
{
}

size_t HTTPChunkedReadBuffer::readChunkHeader()
{
    if (in.eof())
        throw Exception("Unexpected end of file while reading chunk header of HTTP chunked data", ErrorCodes::UNEXPECTED_END_OF_FILE);

    if (!isHexDigit(*in.position()))
        throw Exception("Unexpected data instead of HTTP chunk header", ErrorCodes::CORRUPTED_DATA);

    size_t res = 0;
    do
    {
        res *= 16;
        res += unhex(*in.position());
        ++in.position();

        if (res > max_chunk_size)
            throw Exception("Too large chunk size in HTTPChunkedReadBuffer: " + std::to_string(res), ErrorCodes::TOO_LARGE_ARRAY_SIZE);
    } while (!in.eof() && isHexDigit(*in.position()));

    if (in.eof())
        throw Exception("Unexpected end of file while reading chunk header of HTTP chunked data", ErrorCodes::UNEXPECTED_END_OF_FILE);

    /// Chunk extensions. We must skip all unknown extensions (and we don't know any).
    if (*in.position() == ';')
        skipToCarriageReturnOrEOF(in);
    else if (in.eof())
        throw Exception("Unexpected end of file while reading chunk header of HTTP chunked data", ErrorCodes::UNEXPECTED_END_OF_FILE);

    assertString("\r\n", in);
    return res;
}

void HTTPChunkedReadBuffer::readChunkFooter()
{
    assertString("\r\n", in);
}

bool HTTPChunkedReadBuffer::nextImpl()
{
    /// The footer of previous chunk.
    if (in.count())
        readChunkFooter();

    size_t chunk_size = readChunkHeader();
    if (0 == chunk_size)
    {
        readChunkFooter();
        return false;
    }

    if (in.available() >= chunk_size)
    {
        /// Zero-copy read from input.
        working_buffer = Buffer(in.position(), in.position() + chunk_size);
        in.position() += chunk_size;
    }
    else
    {
        /// Chunk is not completely in buffer, copy it to scratch space.
        memory.resize(chunk_size);
        in.readStrict(memory.data(), chunk_size);
        working_buffer = Buffer(memory.data(), memory.data() + chunk_size);
    }

    /// We postpone reading the footer to the next iteration, because it may not be completely in buffer
    /// but we need to keep the current data in buffer available.
    return true;
}

}

