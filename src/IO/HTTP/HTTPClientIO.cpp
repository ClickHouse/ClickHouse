#include <IO/HTTP/HTTPClientIO.h>
#include <IO/ScopedNonBlocking.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/Hex.h>
#include <Common/Stopwatch.h>

#include <Poco/Net/HTTPResponseParser.h>
#include <Poco/Net/NetException.h>
#include <Poco/NumberParser.h>

#include <cerrno>
#include <limits>
#include <cstring>
#include <poll.h>

#if USE_SILK
#include <IO/SilkFiberStreamSocketImpl.h>
#if USE_SSL
#include <IO/SilkSecureFiberStreamSocketImpl.h>
#endif
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The same limit `Poco::Net::HTTPChunkedStreamBuf` applies to a chunk header line.
constexpr size_t max_chunk_length_line = 4096;

/// A single system call cannot transfer more than `INT_MAX` bytes through the `Poco` socket
/// interface; a request for more is split into several.
constexpr size_t max_transfer_at_once = 1UL << 30;

/// The chunk header is `<size in hex>\r\n`, at most 16 hex digits for a 64 bit size.
constexpr size_t chunk_header_reserve = 18;
constexpr size_t chunk_footer_size = 2;

/// A Silk fiber socket waits by suspending its fiber, and it has no non-blocking mode.
bool isFiberSocket([[maybe_unused]] Poco::Net::SocketImpl & socket_impl)
{
#if USE_SILK
    if (dynamic_cast<Silk::FiberStreamSocketImpl *>(&socket_impl))
        return true;
#if USE_SSL
    if (dynamic_cast<Silk::SecureFiberStreamSocketImpl *>(&socket_impl))
        return true;
#endif
#endif
    return false;
}

/// The scratch buffer a response is read into when the caller provided no memory: in the
/// external buffer mode the memory arrives with `set()` before every read, but the body of an
/// error response is drained before the caller ever sees the buffer. The same size the old
/// `std::iostream` path used for it.
constexpr size_t scratch_buffer_size = 8192;

/// Drops a complete interim `100 Continue`, as `Poco` does: the final response follows it.
/// Returns whether the parser holds a complete final response.
bool finalResponseComplete(Poco::Net::HTTPResponseParser & parser)
{
    if (!parser.complete())
        return false;
    if (parser.response().getStatus() != Poco::Net::HTTPResponse::HTTP_CONTINUE)
        return true;
    parser.reset();
    return false;
}

/// Gives the data the session has buffered to its response parser, up to the end of the final
/// response header. The bytes after it stay buffered for the body.
void feedHTTPResponseParser(Poco::Net::HTTPClientSession & session)
{
    auto & parser = session.responseParser();
    while (session.buffered() > 0)
    {
        session.consumeBuffered(static_cast<int>(parser.feed(session.bufferedData(), static_cast<size_t>(session.buffered()))));
        if (finalResponseComplete(parser))
            return;
    }
}

/// Continues the response parser of the session with blocking reads, until the final response
/// header is complete.
void readHTTPResponseHeader(Poco::Net::HTTPClientSession & session)
{
    auto & parser = session.responseParser();
    feedHTTPResponseParser(session);
    while (!parser.complete())
    {
        /// The parser took all the buffered data, so the buffer has space.
        const int received = session.fillBuffer();
        if (received < 0)
        {
            parser.feedEof();
            /// The end of the data after a `100 Continue`: throws `NoMessageException`.
            if (!finalResponseComplete(parser))
                parser.feedEof();
        }
        else if (received == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A blocking read of an HTTP response header returned no data");
        else
            feedHTTPResponseParser(session);
    }
}

void copyHTTPResponseHeader(const Poco::Net::HTTPResponse & from, Poco::Net::HTTPResponse & to)
{
    to.clear();
    to.setVersion(from.getVersion());
    to.setStatusAndReason(from.getStatus(), from.getReason());
    for (const auto & [name, value] : from)
        to.add(name, value);
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
    feedHTTPResponseParser(session);
    const auto & parser = session.responseParser();
    if (parser.statusLineComplete() && parser.response().getStatus() >= Poco::Net::HTTPResponse::HTTP_MULTIPLE_CHOICES)
        return true;

    /// The rest of a response that is not an error comes after the body: stop reading it now.
    if (parser.complete())
        response_header_complete = true;
    return false;
}

void HTTPRequestBodyWriteBuffer::writeBody(const char * data, size_t size)
{
    /// The server has answered with an error: drop the rest of the body.
    if (stopped)
        return;

    auto & socket = session.socket();

    /// The watching needs a non-blocking socket and a `poll` of its descriptor. A fiber socket has
    /// no non-blocking mode, and a `poll` would block the thread of the fiber scheduler, not the
    /// fiber. Such a body is sent as before the watching: a response is read only after the body.
    if (isFiberSocket(*socket.impl()))
    {
        session.writeAllRaw(data, static_cast<std::streamsize>(size));
        return;
    }

    using Wait = Poco::Net::HTTPSession::Wait;

    /// The send timeout bounds the time spent waiting for the socket without sending anything, as
    /// for a blocking send, also when signals interrupt `poll`. The waits of the send throttler and
    /// of the scheduler are not part of it.
    const Int64 send_timeout_us = session.getSendTimeout().totalMicroseconds();
    Int64 waited_us = 0;

    /// Set when the server closed the connection without a response.
    bool peer_closed = false;

    /// What the next send waits for: a TLS write waits for the socket to become readable during a
    /// renegotiation, while the socket stays writable.
    Wait send_wait = Wait::Write;

    while (size > 0)
    {
        /// Wait for the socket to accept data and watch for a response at the same time, so that a
        /// response arrives before any write can fail. A failed write would lose it over TLS:
        /// OpenSSL allows no read after a failed `SSL_write`. The response is parsed as it arrives:
        /// an error stops the body, a success is finished after the body.
        const bool watch_response = !peer_closed && !response_header_complete;
        Int16 events = send_wait == Wait::Read ? POLLIN : POLLOUT;
        if (watch_response)
            events |= POLLIN;
        pollfd fd{.fd = socket.impl()->sockfd(), .events = events, .revents = 0};

        /// Response bytes that OpenSSL already holds, from a record that `SSL_read` or `SSL_write`
        /// took from the socket, make it readable without `poll`.
        int res = 0;
        if (watch_response && socket.secure() && socket.impl()->available() > 0)
        {
            fd.revents = POLLIN;
            res = 1;
        }
        else
        {
            while (true)
            {
                int timeout_ms = -1;
                if (send_timeout_us > 0)
                {
                    const Int64 remaining_us = send_timeout_us - waited_us;
                    if (remaining_us <= 0)
                    {
                        res = 0;
                        break;
                    }
                    timeout_ms = static_cast<int>(std::min<Int64>((remaining_us + 999) / 1000, std::numeric_limits<int>::max()));
                }
                const Stopwatch watch;
                res = ::poll(&fd, 1, timeout_ms);
                waited_us += static_cast<Int64>(watch.elapsedMicroseconds());
                if (res >= 0 || errno != EINTR)
                    break;
            }
        }

        if (res < 0)
            throw Poco::Net::NetException("Failed to poll the socket while sending the HTTP request body", errno);
        if (res == 0)
            throw Poco::TimeoutException("Timeout while sending the HTTP request body");

        const bool readable = (fd.revents & (POLLIN | POLLHUP | POLLERR)) != 0;

        /// A read is safe also while an `SSL_write` has a record write pending, though the OpenSSL
        /// documentation asks for no other I/O then: OpenSSL delays every write that a read can start
        /// (an alert, a `KeyUpdate`, a session ticket, a renegotiation) until the pending record is
        /// written. See `ssl3_send_alert`, `ssl3_write_bytes` and `ssl3_renegotiate_check`.
        if (watch_response && readable)
        {
            /// Read all that has arrived: `SSL_read` takes a whole record from the socket and keeps
            /// what does not fit into the buffer, and `poll` does not see these bytes.
            ScopedNonBlocking non_blocking(*socket.impl());
            while (true)
            {
                /// For TLS this goes through `SSL_read`: a session ticket or `KeyUpdate` gives no data.
                const int received = session.fillBuffer();

                /// The next write reports how the connection ended, as without the watching.
                if (received < 0)
                    peer_closed = true;

                if (checkResponse())
                {
                    stopped = true;
                    return;
                }
                if (received <= 0 || response_header_complete)
                    break;
            }
        }

        const bool writable = (fd.revents & (POLLOUT | POLLHUP | POLLERR)) != 0;
        const bool can_send = peer_closed || (send_wait == Wait::Read ? readable : writable);
        if (can_send)
        {
            /// Only the send is non-blocking: a response header is read with the timeouts of the session.
            int sent = 0;
            {
                ScopedNonBlocking non_blocking(*socket.impl());
                sent = session.writeRawNonBlocking(data, static_cast<std::streamsize>(size), send_wait);
            }
            if (sent > 0)
            {
                data += sent;
                size -= static_cast<size_t>(sent);
                waited_us = 0;
            }
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
    UInt64 * first_byte_time,
    const Poco::Net::HTTPHeaderLimits & response_header_limits)
{
    const auto body_info = session.sendRequestHeaders(request, connect_time, first_byte_time);
    session.responseParser().reset(response_header_limits);

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
    try
    {
        readHTTPResponseHeader(session);
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
    copyHTTPResponseHeader(session.responseParser().response(), response);

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
