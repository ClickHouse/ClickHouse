#include <IO/HTTP/HTTPClientIO.h>
#include <IO/ScopedNonBlocking.h>

#include <Common/Exception.h>
#include <Common/Hex.h>
#include <Common/Stopwatch.h>

#include <Poco/Ascii.h>
#include <Poco/Net/HTTPBasicStreamBuf.h>
#include <Poco/Net/NetException.h>
#include <Poco/NumberParser.h>
#include <Poco/String.h>

#include <cerrno>
#include <limits>
#include <cstring>
#include <poll.h>


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

void readHTTPResponseHeaderOrReset(Poco::Net::HTTPClientSession & session, Poco::Net::HTTPResponse & response)
{
    try
    {
        response.clear();
        readHTTPResponseHeader(session, response);
    }
    catch (const Poco::Exception &)
    {
        /// Same cleanup as the `Poco::Net::HTTPClientSession::receiveResponse` this replaces: a
        /// half-parsed response header leaves the socket at an unknown offset, so the session must
        /// not be kept alive for a retry. Callers that reuse one session, such as `HTTPAuthClient`,
        /// would otherwise write the next request on top of the leftover bytes.
        session.reset();
        if (const auto * network_exception = session.networkException())
            network_exception->rethrow();
        throw;
    }
}

std::string_view trimHTTPSpaces(std::string_view value)
{
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
        value.remove_prefix(1);
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t'))
        value.remove_suffix(1);
    return value;
}

void parseHTTPStatusLine(std::string_view line, Poco::Net::HTTPResponse & response)
{
    const auto version_end = line.find(' ');
    if (version_end > max_version_length)
        throw Poco::Net::MessageException("Invalid HTTP version string");
    auto rest = trimHTTPSpaces(line.substr(version_end));
    const auto status_end = std::min(rest.find(' '), rest.size());
    if (status_end == 0 || status_end > max_status_length)
        throw Poco::Net::MessageException("Invalid HTTP status code");
    const auto reason = trimHTTPSpaces(rest.substr(status_end));
    if (reason.size() > max_reason_length)
        throw Poco::Net::MessageException("HTTP reason string too long");

    const std::string version(line.substr(0, version_end));
    const std::string status(rest.substr(0, status_end));
    try
    {
        response.setVersion(version);
        response.setStatus(status);
        response.setReason(std::string(reason));
    }
    catch (const Poco::SyntaxException & e)
    {
        throw Poco::SyntaxException(e.message(), ", while reading HTTP response: version='" + version + "', status='" + status + "'");
    }
}

/// Parses the complete lines of a response header that are in the read buffer of the session into
/// `started`, consuming exactly them: the bytes after the header belong to the response body.
void parseBufferedHTTPHeaderLines(Poco::Net::HTTPClientSession & session, Poco::Net::HTTPClientSession::StartedResponse & started)
{
    while (!started.complete)
    {
        const char * begin = session.bufferedData();
        const auto size = static_cast<size_t>(session.buffered());
        const auto * end_of_line = static_cast<const char *>(std::memchr(begin, '\n', size));
        if (!end_of_line)
            return;

        std::string_view line(begin, end_of_line - begin);
        if (!line.empty() && line.back() == '\r')
            line.remove_suffix(1);

        if (!started.status_line_read)
        {
            /// Empty lines before the status line are allowed.
            if (!line.empty())
            {
                parseHTTPStatusLine(line, started.response);
                started.status_line_read = true;
            }
        }
        else if (line.empty())
        {
            started.complete = true;
        }
        else if (line.front() == ' ' || line.front() == '\t')
        {
            /// An obsolete folded line continues the value of the previous field.
            if (started.last_field_name.empty())
                throw Poco::Net::MessageException("Folded HTTP header line without a field");
            const auto value = started.response.get(started.last_field_name) + " " + std::string(trimHTTPSpaces(line));
            started.response.set(started.last_field_name, value);
        }
        else
        {
            const auto colon = line.find(':');
            if (colon == std::string_view::npos)
                throw Poco::Net::MessageException("Field name in HTTP header line without a colon");
            started.last_field_name = std::string(trimHTTPSpaces(line.substr(0, colon)));
            started.response.add(started.last_field_name, std::string(trimHTTPSpaces(line.substr(colon + 1))));
        }
        session.consumeBuffered(static_cast<int>(end_of_line - begin + 1));
    }
}

}


HTTPRequestBodyWriteBuffer::HTTPRequestBodyWriteBuffer(
    Poco::Net::HTTPClientSession & session_, Poco::Net::HTTPClientSession::BodyInfo body_info_, size_t buf_size)
    : WriteBuffer(nullptr, 0)
    , session(session_)
    , encoding(body_info_.encoding)
    , content_length(body_info_.content_length)
    , memory(std::min(buf_size, max_transfer_at_once) + chunk_header_reserve + chunk_footer_size)
{
    /// Every flush leaves as a single `writeAllRaw`, whose length `Poco` narrows down to `int`,
    /// so the buffer is capped the same way the read side caps a single transfer.
    buf_size = std::min(buf_size, max_transfer_at_once);

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

bool HTTPRequestBodyWriteBuffer::checkResponse()
{
    while (session.buffered() > 0)
    {
        auto & started = session.startResponse();
        parseBufferedHTTPHeaderLines(session, started);
        if (!started.status_line_read)
            break;

        const auto status = started.response.getStatus();
        if (status >= Poco::Net::HTTPResponse::HTTP_MULTIPLE_CHOICES)
            return true;

        if (status >= Poco::Net::HTTPResponse::HTTP_OK)
        {
            /// The rest of a successful response comes after the body: stop reading it now.
            if (started.complete)
                response_header_complete = true;
            break;
        }

        /// An informational response, such as a late `100 Continue`: the server wants the body.
        if (!started.complete)
            break;
        session.takeStartedResponse();
    }

    if (!response_header_complete && static_cast<size_t>(session.buffered()) >= Poco::Net::HTTP_DEFAULT_BUFFER_SIZE)
        throw Poco::Net::MessageException("HTTP response header line is too long");
    return false;
}

void HTTPRequestBodyWriteBuffer::writeBody(const char * data, size_t size)
{
    /// The server has answered with an error: drop the rest of the body.
    if (stopped)
        return;

    auto & socket = session.socket();
    const Int64 send_timeout_ms = session.getSendTimeout().totalMilliseconds();

    /// Set when the server closed the connection without a response.
    bool peer_closed = false;

    while (size > 0)
    {
        /// Wait for the socket to accept data and watch for a response at the same time, so that a
        /// response arrives before any write can fail. A failed write would lose it over TLS:
        /// OpenSSL allows no read after a failed `SSL_write`. The response is parsed as it arrives:
        /// an error stops the body, a success is finished after the body.
        const bool watch_response = !peer_closed && !response_header_complete;
        const Int16 events = watch_response ? (POLLIN | POLLOUT) : POLLOUT;
        pollfd fd{.fd = socket.impl()->sockfd(), .events = events, .revents = 0};

        /// The send timeout bounds the whole wait, also when signals interrupt `poll`.
        const Stopwatch watch;
        int res = 0;
        while (true)
        {
            int timeout_ms = -1;
            if (send_timeout_ms > 0)
            {
                const Int64 remaining_ms = send_timeout_ms - static_cast<Int64>(watch.elapsedMilliseconds());
                if (remaining_ms <= 0)
                {
                    res = 0;
                    break;
                }
                timeout_ms = static_cast<int>(std::min<Int64>(remaining_ms, std::numeric_limits<int>::max()));
            }
            res = ::poll(&fd, 1, timeout_ms);
            if (res >= 0 || errno != EINTR)
                break;
        }

        if (res < 0)
            throw Poco::Net::NetException("Failed to poll the socket while sending the HTTP request body", errno);
        if (res == 0)
            throw Poco::TimeoutException("Timeout while sending the HTTP request body");

        if (watch_response && (fd.revents & (POLLIN | POLLHUP | POLLERR)))
        {
            int received = 0;
            {
                /// For TLS this goes through `SSL_read`: a session ticket or `KeyUpdate` gives no data.
                ScopedNonBlocking non_blocking(*socket.impl());
                received = session.fillBuffer();
            }

            /// The next write reports how the connection ended, as without the watching.
            if (received < 0)
                peer_closed = true;

            if (checkResponse())
            {
                stopped = true;
                return;
            }
        }

        if ((fd.revents & POLLOUT) || peer_closed)
        {
            /// Only the send is non-blocking: a response header is read with the timeouts of the session.
            int sent = 0;
            {
                ScopedNonBlocking non_blocking(*socket.impl());
                sent = session.writeRawNonBlocking(data, static_cast<std::streamsize>(size));
            }
            data += sent;
            size -= static_cast<size_t>(sent);
        }
    }
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
            writeBody(working_buffer.begin(), size);
            break;

        case Poco::Net::HTTPClientSession::BodyEncoding::UntilEOF:
            writeBody(working_buffer.begin(), size);
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

            writeBody(header, static_cast<size_t>(data + size + chunk_footer_size - header));
            break;
        }
    }

    bytes_written += size;
}

void HTTPRequestBodyWriteBuffer::finalizeImpl()
{
    next();

    /// The server has answered with an error, and the rest of the body was dropped: the request stays incomplete.
    if (stopped)
        return;

    if (encoding == Poco::Net::HTTPClientSession::BodyEncoding::Chunked)
    {
        writeBody("0\r\n\r\n", 5);
        /// The error response can arrive while the terminator is being sent.
        if (stopped)
            return;
    }

    if (encoding == Poco::Net::HTTPClientSession::BodyEncoding::ContentLength && bytes_written != content_length)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "HTTP request body is smaller than its Content-Length: {} bytes written of {}",
            bytes_written,
            content_length);

    if (encoding == Poco::Net::HTTPClientSession::BodyEncoding::UntilEOF)
    {
        /// This framing means "the body ends when the connection is closed", so writing its last
        /// byte does not end it: the server keeps waiting for more until the sending side is shut
        /// down. Nothing this buffer does can make such a request complete, and the connection
        /// must not return to the pool - the next borrower would start its request inside the body
        /// of this one, from the server's point of view.
        ///
        /// `HTTPStreamBuf::close`, the counterpart of this function on the `std::iostream` path,
        /// does the same two things: it half-closes a session that is not keep-alive, and
        /// `HTTPOutputStream::isComplete` reports incomplete unconditionally.
        if (!session.getKeepAlive())
            session.socket().shutdownSend();
        return;
    }

    session.setRequestBodyComplete(true);
}


HTTPResponseReadBuffer::HTTPResponseReadBuffer(
    Poco::Net::HTTPClientSession & session_,
    HTTPSessionPtr session_holder_,
    Poco::Net::HTTPClientSession::BodyInfo body_info_,
    size_t buf_size)
    : BufferWithOwnMemory<ReadBuffer>(0)
    , session(&session_)
    , session_holder(std::move(session_holder_))
    , encoding(body_info_.encoding)
    , content_length(body_info_.content_length)
    , own_buffer_size(encoding == Poco::Net::HTTPClientSession::BodyEncoding::NoBody ? 0 : buf_size)
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
    /// caller substituted with `set()`. The own memory is allocated only here, on the first read
    /// that arrives without the caller's memory: the S3 readers never read into it - they pass
    /// their own memory with `set()` or read with `readBig` - so allocating it in advance would
    /// only charge an unused buffer to the query. Reading with no own memory at all - draining
    /// the body of an error response before it reaches the caller of the external buffer mode -
    /// falls back to a small scratch buffer.
    if (internal_buffer.empty())
    {
        memory.resize(own_buffer_size ? own_buffer_size : scratch_buffer_size);
        setPaddedInternalBuffer(memory, 0, memory.size());
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

    setWorkingBufferFromInternal();
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

    /// A body delimited by the end of the connection has been read to the end, but the peer
    /// has closed its side to say so: the connection cannot carry another request and must not
    /// return to the pool, the same as `HTTPInputStream::isComplete` reported on the `std::iostream`
    /// path. The pool only checks that the socket is still open, so reusing it would hand the
    /// next borrower a dead connection.
    if (session && encoding != Poco::Net::HTTPClientSession::BodyEncoding::UntilEOF)
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

static std::unique_ptr<HTTPResponseReadBuffer> makeHTTPResponseReadBuffer(
    HTTPSessionPtr session_holder, Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPResponse & response, size_t buf_size)
{
    const auto body_info = session.onResponseHeadersReceived(response);

    /// A body shorter than the buffer needs no more memory than its own length.
    if (body_info.encoding == Poco::Net::HTTPClientSession::BodyEncoding::ContentLength)
        buf_size = std::min<UInt64>(buf_size, body_info.content_length);

    return std::make_unique<HTTPResponseReadBuffer>(session, std::move(session_holder), body_info, buf_size);
}

static std::unique_ptr<HTTPResponseReadBuffer> receiveHTTPResponseImpl(
    HTTPSessionPtr session_holder, Poco::Net::HTTPClientSession & session, Poco::Net::HTTPResponse & response, size_t buf_size)
{
    /// A response that started to arrive while the request body was being sent: finish it.
    if (auto started = session.takeStartedResponse())
    {
        try
        {
            while (true)
            {
                parseBufferedHTTPHeaderLines(session, *started);
                if (started->complete)
                {
                    if (started->response.getStatus() >= Poco::Net::HTTPResponse::HTTP_OK)
                        break;
                    /// An informational response: the final one follows.
                    started = std::make_unique<Poco::Net::HTTPClientSession::StartedResponse>();
                    continue;
                }
                if (static_cast<size_t>(session.buffered()) >= Poco::Net::HTTP_DEFAULT_BUFFER_SIZE)
                    throw Poco::Net::MessageException("HTTP response header line is too long");
                if (session.fillBuffer() < 0)
                    throw Poco::Net::MessageException("HTTP response header ended prematurely");
            }

            /// The caller's limits apply to the header as a whole, as for a header read in one go.
            const auto & parsed = started->response;
            if (response.getFieldLimit() > 0 && parsed.size() > static_cast<size_t>(response.getFieldLimit()))
                throw Poco::Net::MessageException("Too many header fields");
            response.clear();
            response.setVersion(parsed.getVersion());
            response.setStatusAndReason(parsed.getStatus(), parsed.getReason());
            for (const auto & [name, value] : parsed)
            {
                if (response.getNameLengthLimit() > 0 && name.size() > static_cast<size_t>(response.getNameLengthLimit()))
                    throw Poco::Net::MessageException("Field name is too long");
                if (response.getValueLengthLimit() > 0 && value.size() > static_cast<size_t>(response.getValueLengthLimit()))
                    throw Poco::Net::MessageException("Field value is too long");
                response.add(name, value);
            }
        }
        catch (const Poco::Exception &)
        {
            /// Same cleanup as in `readHTTPResponseHeaderOrReset`.
            session.reset();
            if (const auto * network_exception = session.networkException())
                network_exception->rethrow();
            throw;
        }
        return makeHTTPResponseReadBuffer(std::move(session_holder), session, response, buf_size);
    }

    do
    {
        readHTTPResponseHeaderOrReset(session, response);
    } while (response.getStatus() == Poco::Net::HTTPResponse::HTTP_CONTINUE);

    return makeHTTPResponseReadBuffer(std::move(session_holder), session, response, buf_size);
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
