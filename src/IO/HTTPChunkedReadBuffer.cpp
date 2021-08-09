#include <IO/HTTPChunkedReadBuffer.h>

#include <IO/ReadHelpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>
#include <common/arithmeticOverflow.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int CORRUPTED_DATA;
}

size_t HTTPChunkedReadBuffer::readChunkHeader()
{
    if (in->eof())
        throw Exception("Unexpected end of file while reading chunk header of HTTP chunked data", ErrorCodes::UNEXPECTED_END_OF_FILE);

    if (!isHexDigit(*in->position()))
        throw Exception("Unexpected data instead of HTTP chunk header", ErrorCodes::CORRUPTED_DATA);

    size_t res = 0;
    do
    {
        if (common::mulOverflow(res, 16ul, res) || common::addOverflow<size_t>(res, unhex(*in->position()), res))
            throw Exception("Chunk size is out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        ++in->position();
    } while (!in->eof() && isHexDigit(*in->position()));

    /// NOTE: If we want to read any chunk extensions, it should be done here.

    skipToCarriageReturnOrEOF(*in);

    if (in->eof())
        throw Exception("Unexpected end of file while reading chunk header of HTTP chunked data", ErrorCodes::UNEXPECTED_END_OF_FILE);

    assertString("\n", *in);
    return res;
}

void HTTPChunkedReadBuffer::readChunkFooter()
{
    assertString("\r\n", *in);
}

bool HTTPChunkedReadBuffer::nextImpl()
{
    if (!in)
        return false;

    /// The footer of previous chunk.
    if (count())
        readChunkFooter();

    size_t chunk_size = readChunkHeader();
    if (0 == chunk_size)
    {
        readChunkFooter();
        in.reset();  // prevent double-eof situation.
        return false;
    }

    if (in->available() >= chunk_size)
    {
        /// Zero-copy read from input.
        working_buffer = Buffer(in->position(), in->position() + chunk_size);
        in->position() += chunk_size;
    }
    else
    {
        /// Chunk is not completely in buffer, copy it to scratch space.
        memory.resize(chunk_size);
        in->readStrict(memory.data(), chunk_size);
        working_buffer = Buffer(memory.data(), memory.data() + chunk_size);
    }

    /// NOTE: We postpone reading the footer to the next iteration, because it may not be completely in buffer,
    ///       but we need to keep the current data in buffer available.

    return true;
}

}
