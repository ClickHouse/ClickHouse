#include <IO/HTTP/HTTPClientIO.h>

#include <Common/Exception.h>

#include <Poco/Ascii.h>
#include <Poco/Net/NetException.h>
#include <Poco/NumberParser.h>
#include <Poco/String.h>

#include <base/hex.h>

#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The same limits `Poco::Net::HTTPResponse` applies when it parses a response line.
constexpr size_t max_version_length = 8;
constexpr size_t max_status_length = 3;
constexpr size_t max_reason_length = 512;

/// The same limit `Poco::Net::HTTPChunkedStreamBuf` applies to a chunk header line.
constexpr size_t max_chunk_length_line = 4096;

/// A single system call cannot transfer more than `INT_MAX` bytes through the `Poco` socket
/// interface; a request for more is split into several.
constexpr size_t max_transfer_at_once = 1UL << 30;

/// The chunk header is `<size in hex>\r\n`, at most 16 hex digits for a 64 bit size.
constexpr size_t chunk_header_reserve = 18;
constexpr size_t chunk_footer_size = 2;

/// The scratch buffer a response is read into when the caller provided no memory: in the
/// external buffer mode the memory arrives with `set()` before every read, but the body of an
/// error response is drained before the caller ever sees the buffer. The same size the old
/// `std::iostream` path used for it.
constexpr size_t scratch_buffer_size = 8192;

/// A port of `Poco::Net::MessageHeader::read`, reading the bytes from the session instead of an
/// `std::istream`. Keeps the behaviour of the original, including the limits, the folding of
/// continuation lines and the exceptions it throws.
/// Returns the character the header block was terminated with.
int readHTTPMessageHeader(Poco::Net::HTTPClientSession & session, Poco::Net::HTTPResponse & response)
{
    static const int eof = std::char_traits<char>::eof();

    const int field_limit = response.getFieldLimit();
    const size_t name_length_limit = static_cast<size_t>(response.getNameLengthLimit());
    const size_t value_length_limit = static_cast<size_t>(response.getValueLengthLimit());

    std::string name;
    std::string value;
    name.reserve(32);
    value.reserve(64);

    int ch = session.getCharRaw();
    int fields = 0;
    while (ch != eof && ch != '\r' && ch != '\n')
    {
        if (field_limit > 0 && fields == field_limit)
            throw Poco::Net::MessageException("Too many header fields");

        name.clear();
        value.clear();

        while (ch != eof && ch != ':' && ch != '\n' && name.length() < name_length_limit)
        {
            name += static_cast<char>(ch);
            ch = session.getCharRaw();
        }
        if (ch == '\n')
        {
            ch = session.getCharRaw();
            continue; /// ignore invalid header lines
        }
        if (ch != ':')
            throw Poco::Net::MessageException("Field name too long/no colon found");
        if (ch != eof)
            ch = session.getCharRaw(); /// ':'

        while (ch != eof && Poco::Ascii::isSpace(ch) && ch != '\r' && ch != '\n')
            ch = session.getCharRaw();
        while (ch != eof && ch != '\r' && ch != '\n' && value.length() < value_length_limit)
        {
            value += static_cast<char>(ch);
            ch = session.getCharRaw();
        }
        if (ch == '\r')
            ch = session.getCharRaw();
        if (ch == '\n')
            ch = session.getCharRaw();
        else if (ch != eof)
            throw Poco::Net::MessageException("Field value too long/no CRLF found");

        while (ch == ' ' || ch == '\t') /// folding
        {
            while (ch != eof && ch != '\r' && ch != '\n' && value.length() < value_length_limit)
            {
                value += static_cast<char>(ch);
                ch = session.getCharRaw();
            }
            if (ch == '\r')
                ch = session.getCharRaw();
            if (ch == '\n')
                ch = session.getCharRaw();
            else if (ch != eof)
                throw Poco::Net::MessageException("Folded field value too long/no CRLF found");
        }

        Poco::trimRightInPlace(value);
        response.add(name, Poco::Net::MessageHeader::decodeWord(value));
        ++fields;
    }

    return ch;
}

/// A port of `Poco::Net::HTTPResponse::read`, reading the bytes from the session.
void readHTTPResponseHeader(Poco::Net::HTTPClientSession & session, Poco::Net::HTTPResponse & response)
{
    static const int eof = std::char_traits<char>::eof();

    std::string version;
    std::string status;
    std::string reason;

    int ch = session.getCharRaw();
    if (ch == eof)
        throw Poco::Net::NoMessageException();
    while (Poco::Ascii::isSpace(ch))
        ch = session.getCharRaw();
    if (ch == eof)
        throw Poco::Net::MessageException("No HTTP response header");
    while (!Poco::Ascii::isSpace(ch) && ch != eof && version.length() < max_version_length)
    {
        version += static_cast<char>(ch);
        ch = session.getCharRaw();
    }
    if (!Poco::Ascii::isSpace(ch))
        throw Poco::Net::MessageException("Invalid HTTP version string");
    while (Poco::Ascii::isSpace(ch))
        ch = session.getCharRaw();
    while (!Poco::Ascii::isSpace(ch) && ch != eof && status.length() < max_status_length)
    {
        status += static_cast<char>(ch);
        ch = session.getCharRaw();
    }
    if (!Poco::Ascii::isSpace(ch))
        throw Poco::Net::MessageException("Invalid HTTP status code");
    while (Poco::Ascii::isSpace(ch) && ch != '\r' && ch != '\n' && ch != eof)
        ch = session.getCharRaw();
    while (ch != '\r' && ch != '\n' && ch != eof && reason.length() < max_reason_length)
    {
        reason += static_cast<char>(ch);
        ch = session.getCharRaw();
    }
    if (!Poco::Ascii::isSpace(ch))
        throw Poco::Net::MessageException("HTTP reason string too long");
    if (ch == '\r')
        ch = session.getCharRaw();
    if (ch != '\n')
        throw Poco::Net::MessageException("Unterminated HTTP response line");

    /// `readHTTPMessageHeader` stops on the first character of the empty line that ends the
    /// header block; the rest of that line is consumed here, as `Poco` does.
    ch = readHTTPMessageHeader(session, response);
    while (ch != '\n' && ch != eof)
        ch = session.getCharRaw();

    try
    {
        response.setVersion(version);
        response.setStatus(status);
        response.setReason(reason);
    }
    catch (const Poco::SyntaxException & e)
    {
        throw Poco::SyntaxException(
            e.message(), ", while reading HTTP response: version='" + version + "', status='" + status + "', reason='" + reason + "'");
    }
}

}


HTTPRequestBodyWriteBuffer::HTTPRequestBodyWriteBuffer(
    Poco::Net::HTTPClientSession & session_, Poco::Net::HTTPClientSession::BodyInfo body_info_, size_t buf_size)
    : WriteBuffer(nullptr, 0)
    , session(session_)
    , encoding(body_info_.encoding)
    , content_length(body_info_.content_length)
    , memory(buf_size + chunk_header_reserve + chunk_footer_size)
{
    /// The chunk header is written in front of the data and the trailer right after it, so that
    /// a chunk leaves as a single `send` without copying the payload anywhere.
    BufferBase::set(memory.data() + chunk_header_reserve, buf_size, 0);
}

HTTPRequestBodyWriteBuffer::~HTTPRequestBodyWriteBuffer()
{
    /// A body that was not written to the end leaves the connection unusable: the session keeps
    /// `requestBodyComplete` false, and the pool drops the connection instead of handing it to
    /// the next request.
    cancel();
}

void HTTPRequestBodyWriteBuffer::nextImpl()
{
    const size_t size = offset();
    if (!size)
        return;

    switch (encoding)
    {
        case Poco::Net::HTTPClientSession::BodyEncoding::NoBody:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Cannot write {} bytes of a body to an HTTP request that does not have one", size);

        case Poco::Net::HTTPClientSession::BodyEncoding::ContentLength:
            if (bytes_written + size > content_length)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "HTTP request body is larger than its Content-Length: {} bytes to write of {}",
                    bytes_written + size,
                    content_length);
            session.writeAllRaw(working_buffer.begin(), static_cast<std::streamsize>(size));
            break;

        case Poco::Net::HTTPClientSession::BodyEncoding::UntilEOF:
            session.writeAllRaw(working_buffer.begin(), static_cast<std::streamsize>(size));
            break;

        case Poco::Net::HTTPClientSession::BodyEncoding::Chunked:
        {
            char * const data = working_buffer.begin();

            /// `<size in hex>\r\n<data>\r\n`, written backwards into the space reserved in front
            /// of the data, so that the whole chunk goes out with a single write.
            char * header = data - chunk_footer_size;
            memcpy(header, "\r\n", chunk_footer_size);
            for (size_t rest = size; rest != 0; rest /= 16)
                *--header = hexDigitLowercase(rest % 16);
            chassert(header >= memory.data());

            memcpy(data + size, "\r\n", chunk_footer_size);

            session.writeAllRaw(header, static_cast<std::streamsize>(data + size + chunk_footer_size - header));
            break;
        }
    }

    bytes_written += size;
}

void HTTPRequestBodyWriteBuffer::finalizeImpl()
{
    next();

    if (encoding == Poco::Net::HTTPClientSession::BodyEncoding::Chunked)
        session.writeAllRaw("0\r\n\r\n", 5);

    if (encoding == Poco::Net::HTTPClientSession::BodyEncoding::ContentLength && bytes_written != content_length)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "HTTP request body is smaller than its Content-Length: {} bytes written of {}",
            bytes_written,
            content_length);

    session.setRequestBodyComplete(true);
}


HTTPResponseReadBuffer::HTTPResponseReadBuffer(
    Poco::Net::HTTPClientSession & session_,
    HTTPSessionPtr session_holder_,
    Poco::Net::HTTPClientSession::BodyInfo body_info_,
    size_t buf_size)
    : BufferWithOwnMemory<ReadBuffer>(
          body_info_.encoding == Poco::Net::HTTPClientSession::BodyEncoding::NoBody ? 0 : buf_size)
    , session(&session_)
    , session_holder(std::move(session_holder_))
    , encoding(body_info_.encoding)
    , content_length(body_info_.content_length)
{
    /// A response without a body, or with an empty one, is complete as soon as its headers have
    /// been read; a caller is not required to read from a buffer that has nothing in it.
    if (encoding == Poco::Net::HTTPClientSession::BodyEncoding::NoBody
        || (encoding == Poco::Net::HTTPClientSession::BodyEncoding::ContentLength && content_length == 0))
        markComplete();
}

HTTPResponseReadBuffer::~HTTPResponseReadBuffer()
{
    /// A chunked body consumed up to the last payload byte but not past the terminating chunk is
    /// still a complete response: the terminator is on the wire right behind the payload. Reading
    /// it here lets the pool reuse the connection, which is what the old iostream path did on
    /// teardown with `HTTPChunkedInputStream::isComplete(true)`. When the next thing on the wire
    /// is a real chunk instead, the response was abandoned midway and stays incomplete.
    if (session && !response_complete && encoding == Poco::Net::HTTPClientSession::BodyEncoding::Chunked && !chunked_eof
        && chunk_left == 0)
    {
        try
        {
            chunk_left = readChunkLength();
            if (chunk_left == 0)
            {
                skipCRLF();
                chunked_eof = true;
                markComplete();
            }
        }
        catch (...) /// NOLINT(bugprone-empty-catch) Ok: the response merely stays incomplete and the connection is dropped
        {
        }
    }
}

void HTTPResponseReadBuffer::detachSession()
{
    session = nullptr;
    session_holder.reset();
}

bool HTTPResponseReadBuffer::nextImpl()
{
    if (response_complete)
        return false;

    /// A response with a body is read into this buffer's own memory, or into the memory the
    /// caller substituted with `set()`. Reading before any was provided - draining the body of
    /// an error response before it reaches the caller of the external buffer mode - falls back
    /// to a small scratch buffer.
    if (internal_buffer.empty())
    {
        memory.resize(scratch_buffer_size);
        internal_buffer = Buffer(memory.data(), memory.data() + memory.size());
    }

    /// A single read returns only the data available on the socket at the moment; reading to
    /// the end of the buffer keeps the blocks handed downstream - to the filesystem cache, to
    /// the prefetcher - as large as the buffer, not as large as one `recv`.
    size_t bytes = 0;
    while (bytes < internal_buffer.size())
    {
        const size_t bytes_read_now = readBody(internal_buffer.begin() + bytes, internal_buffer.size() - bytes);
        if (!bytes_read_now)
        {
            markComplete();
            break;
        }
        bytes += bytes_read_now;
    }

    if (!bytes)
        return false;

    working_buffer = internal_buffer;
    working_buffer.resize(bytes);
    return true;
}

size_t HTTPResponseReadBuffer::readBig(char * to, size_t n)
{
    size_t copied = 0;

    /// Whatever is already buffered has to be handed out first.
    if (const size_t buffered = available())
    {
        copied = std::min(buffered, n);
        memcpy(to, position(), copied);
        position() += copied;
    }

    /// The rest is read from the socket into the caller's memory, with no buffer in between.
    while (copied < n)
    {
        const size_t bytes_read_now = readBody(to + copied, n - copied);
        if (!bytes_read_now)
        {
            markComplete();
            break;
        }

        copied += bytes_read_now;
        bytes += bytes_read_now;
    }

    return copied;
}

size_t HTTPResponseReadBuffer::readBody(char * to, size_t n)
{
    if (!session || !n)
        return 0;

    n = std::min(n, max_transfer_at_once);

    switch (encoding)
    {
        case Poco::Net::HTTPClientSession::BodyEncoding::NoBody:
            return 0;

        case Poco::Net::HTTPClientSession::BodyEncoding::ContentLength:
        {
            if (bytes_read >= content_length)
                return 0;

            const int bytes = session->readRaw(to, static_cast<std::streamsize>(std::min<UInt64>(n, content_length - bytes_read)));
            if (bytes <= 0)
                throw Poco::Net::MessageException("Unexpected EOF");

            bytes_read += bytes;
            /// The connection is ready for the next request as soon as the last byte of the body
            /// has been read; there is no need to wait for a read that returns nothing.
            if (bytes_read == content_length)
                markComplete();
            return bytes;
        }

        case Poco::Net::HTTPClientSession::BodyEncoding::UntilEOF:
        {
            const int bytes = session->readRaw(to, static_cast<std::streamsize>(n));
            return bytes > 0 ? static_cast<size_t>(bytes) : 0;
        }

        case Poco::Net::HTTPClientSession::BodyEncoding::Chunked:
            return readChunked(to, n);
    }
}

size_t HTTPResponseReadBuffer::readChunked(char * to, size_t n)
{
    if (chunked_eof)
        return 0;

    if (chunk_left == 0)
    {
        chunk_left = readChunkLength();

        if (chunk_left == 0)
        {
            /// The terminating chunk. Trailers are not supported, the same as in `Poco`.
            skipCRLF();
            chunked_eof = true;
            return 0;
        }
    }

    const int bytes = session->readRaw(to, static_cast<std::streamsize>(std::min<UInt64>(n, chunk_left)));
    if (bytes <= 0)
        throw Poco::Net::MessageException("Unexpected EOF");

    chunk_left -= bytes;
    if (chunk_left == 0)
        skipCRLF();

    return bytes;
}

UInt64 HTTPResponseReadBuffer::readChunkLength()
{
    static const int eof = std::char_traits<char>::eof();

    std::string line;
    while (line.size() < max_chunk_length_line)
    {
        const int ch = session->getCharRaw();
        if (ch == eof)
            throw Poco::Net::MessageException("Unexpected EOF");
        line += static_cast<char>(ch);
        if (ch == '\n')
            break;
    }

    if (line.size() >= 2 && line[line.size() - 2] == '\r' && line[line.size() - 1] == '\n')
        line.resize(line.size() - 2);
    else
        throw Poco::Net::MessageException("Malformed chunked encoding");

    /// Chunk extensions are not interpreted, the same as in `Poco`.
    if (const size_t pos = line.find(';'); pos != std::string::npos)
        line.resize(pos);

    unsigned chunk_length = 0;
    if (!Poco::NumberParser::tryParseHex(line, chunk_length))
        throw Poco::Net::MessageException("Invalid chunk length");

    return chunk_length;
}

void HTTPResponseReadBuffer::skipCRLF()
{
    static const int eof = std::char_traits<char>::eof();

    const int c1 = session->getCharRaw();
    const int c2 = session->getCharRaw();
    if (c1 == eof || c2 == eof)
        throw Poco::Net::MessageException("Unexpected EOF");
    if (c1 != '\r' || c2 != '\n')
        throw Poco::Net::MessageException("Malformed chunked encoding");
}

void HTTPResponseReadBuffer::markComplete()
{
    if (response_complete)
        return;

    response_complete = true;
    if (session)
        session->setResponseBodyComplete(true);
}


std::unique_ptr<HTTPRequestBodyWriteBuffer> sendHTTPRequest(
    Poco::Net::HTTPClientSession & session,
    Poco::Net::HTTPRequest & request,
    size_t buf_size,
    UInt64 * connect_time,
    UInt64 * first_byte_time)
{
    const auto body_info = session.sendRequestHeaders(request, connect_time, first_byte_time);

    /// A request that carries no body, or a body shorter than the buffer, needs no more memory
    /// than that. One byte is always kept, so that a write to a request without a body reaches
    /// `nextImpl` and fails there instead of spinning on an empty buffer.
    if (body_info.encoding == Poco::Net::HTTPClientSession::BodyEncoding::NoBody)
        buf_size = 1;
    else if (body_info.encoding == Poco::Net::HTTPClientSession::BodyEncoding::ContentLength)
        buf_size = std::max<UInt64>(std::min<UInt64>(buf_size, body_info.content_length), 1);

    return std::make_unique<HTTPRequestBodyWriteBuffer>(session, body_info, buf_size);
}

static std::unique_ptr<HTTPResponseReadBuffer> receiveHTTPResponseImpl(
    HTTPSessionPtr session_holder, Poco::Net::HTTPClientSession & session, Poco::Net::HTTPResponse & response, size_t buf_size)
{
    do
    {
        response.clear();
        readHTTPResponseHeader(session, response);
    } while (response.getStatus() == Poco::Net::HTTPResponse::HTTP_CONTINUE);

    const auto body_info = session.onResponseHeadersReceived(response);

    /// A body shorter than the buffer needs no more memory than its own length.
    if (body_info.encoding == Poco::Net::HTTPClientSession::BodyEncoding::ContentLength)
        buf_size = std::min<UInt64>(buf_size, body_info.content_length);

    return std::make_unique<HTTPResponseReadBuffer>(session, std::move(session_holder), body_info, buf_size);
}

std::unique_ptr<HTTPResponseReadBuffer>
receiveHTTPResponse(Poco::Net::HTTPClientSession & session, Poco::Net::HTTPResponse & response, size_t buf_size)
{
    return receiveHTTPResponseImpl(nullptr, session, response, buf_size);
}

std::unique_ptr<HTTPResponseReadBuffer>
receiveHTTPResponse(HTTPSessionPtr session, Poco::Net::HTTPResponse & response, size_t buf_size)
{
    auto & session_ref = *session;
    return receiveHTTPResponseImpl(std::move(session), session_ref, response, buf_size);
}

}
