#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPResponseParser.h>

#include <memory>

namespace DB
{

using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

/// Writes the body of an HTTP request directly to the socket of the session.
///
/// The framing - `Content-Length` or chunked transfer encoding - is the one the session chose
/// when the request headers were sent. There is no `std::iostream` in between: the buffer is
/// handed to a single `send` call, and for chunked encoding the chunk header and trailer are
/// written into the space reserved around the buffer, so that a chunk still costs one `send`
/// and the payload is never copied.
///
/// A request that carries a body with neither framing - a `POST`, `PUT` or `PATCH` without
/// `Content-Length` and without chunked encoding - is delimited by the end of the connection, so
/// it can never be finished on a connection that is meant to be reused. Finalizing such a body
/// leaves the request incomplete, which is what makes the pool drop the connection instead of
/// handing it to the next request.
class HTTPRequestBodyWriteBuffer : public WriteBuffer
{
public:
    /// On a plain TCP socket, the body is sent while the connection is watched for a response
    /// (RFC 9112, 9.5). The response is given to the parser of the session as it arrives, and
    /// `receiveHTTPResponse` finishes it: an error status stops the body, and the rest of it is
    /// dropped; a success is finished after the body. A response that breaks the syntax or the
    /// limits makes the write throw. A TLS or a Silk fiber socket sends the body with blocking writes.
    HTTPRequestBodyWriteBuffer(
        Poco::Net::HTTPClientSession & session_, Poco::Net::HTTPClientSession::BodyInfo body_info_, size_t buf_size);

    ~HTTPRequestBodyWriteBuffer() override;

    /// Whether the request has a body at all. Writing to the buffer of a request without a body
    /// (a GET, or a request the server does not expect a body for) is a logical error.
    bool hasBody() const { return encoding != Poco::Net::HTTPClientSession::BodyEncoding::NoBody; }

protected:
    void nextImpl() override;
    void finalizeImpl() override;

private:
    void writeBody(const char * data, size_t size);
    /// Gives the response bytes that have arrived to the parser. Returns true for an error status.
    bool checkResponse();

    Poco::Net::HTTPClientSession & session;
    const Poco::Net::HTTPClientSession::BodyEncoding encoding;
    const UInt64 content_length;
    UInt64 bytes_written = 0;
    /// The server answered with an error before the end of the body.
    bool stopped = false;
    /// A response that is not an error is complete; the rest of it is read after the body.
    bool response_header_complete = false;

    Memory<> memory;
};

/// Reads the body of an HTTP response directly from the socket of the session.
///
/// Applies the framing of the response - fixed length, chunked, or until the connection is
/// closed - while reading the payload straight into this buffer's memory, which the caller can
/// replace with its own with `set()`. Only the framing metadata (chunk headers) goes through the
/// small buffer of the session, exactly like the response headers before it.
///
/// When the body has been read to the end, this is reported back to the session, so that the
/// connection pool can tell a connection that is ready for the next request from one that has
/// to be dropped.
class HTTPResponseReadBuffer final : public BufferWithOwnMemory<ReadBuffer>
{
public:
    HTTPResponseReadBuffer(
        Poco::Net::HTTPClientSession & session_,
        HTTPSessionPtr session_holder_,
        Poco::Net::HTTPClientSession::BodyInfo body_info_,
        size_t buf_size);

    ~HTTPResponseReadBuffer() override;

    /// Stops reading from the session and releases it. The buffer stays usable, but returns no
    /// more data. Used when the session has to be given back before the reader is destroyed.
    void detachSession();

    /// Whether the response body has been read to the end.
    bool isResponseComplete() const { return response_complete; }

    bool supportsExternalBufferMode() const override { return true; }

    /// Reads into the caller's memory directly, so that a large read costs no copy at all.
    size_t readBig(char * to, size_t n) override;

private:
    bool nextImpl() override;

    /// Reads up to `n` bytes of the body into `to`, applying the framing.
    /// Returns 0 at the end of the body.
    size_t readBody(char * to, size_t n);
    size_t readChunked(char * to, size_t n);
    UInt64 readChunkLength();
    void skipCRLF();

    void markComplete();

    Poco::Net::HTTPClientSession * session;
    HTTPSessionPtr session_holder;
    const Poco::Net::HTTPClientSession::BodyEncoding encoding;
    const UInt64 content_length;
    /// The size of the own memory, allocated on the first read that needs it.
    const size_t own_buffer_size;

    UInt64 bytes_read = 0;
    UInt64 chunk_left = 0;
    bool chunked_eof = false;
    bool response_complete = false;
};

/// Sends the request line and the headers of `request`, and returns a buffer for its body.
///
/// The returned buffer has to be finalized before the response is received; for a request
/// without a body, finalizing it only checks that nothing was written. This is the replacement
/// of `Poco::Net::HTTPClientSession::sendRequest`, which returns an `std::ostream` that copies
/// the data through an 8 KiB buffer and sends it in 8 KiB pieces.
///
/// The header of the response is checked against `response_header_limits`. They are given here,
/// because the response is parsed from the moment the body starts to be sent.
std::unique_ptr<HTTPRequestBodyWriteBuffer> sendHTTPRequest(
    Poco::Net::HTTPClientSession & session,
    Poco::Net::HTTPRequest & request,
    size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
    UInt64 * connect_time = nullptr,
    UInt64 * first_byte_time = nullptr,
    const Poco::Net::HTTPHeaderLimits & response_header_limits = {});

/// Reads and parses the response line and the headers into `response`, and returns a buffer for
/// its body. An interim `100 Continue` is skipped, as `Poco` does.
///
/// The header is parsed by the response parser of the session and checked against the limits
/// given to `sendHTTPRequest`; the limits set on `response` are not used.
///
/// This is the replacement of `Poco::Net::HTTPClientSession::receiveResponse`. Throws the same
/// exceptions `Poco` throws: `Poco::Net::NoMessageException` when the peer sent nothing at all
/// (a keep-alive connection closed by the server), `Poco::Net::MessageException` for a
/// malformed response or one that ends before its body does.
std::unique_ptr<HTTPResponseReadBuffer> receiveHTTPResponse(
    Poco::Net::HTTPClientSession & session, Poco::Net::HTTPResponse & response, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

/// Same, but the returned buffer keeps the session alive.
std::unique_ptr<HTTPResponseReadBuffer> receiveHTTPResponse(
    HTTPSessionPtr session, Poco::Net::HTTPResponse & response, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

}
