#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

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
class HTTPRequestBodyWriteBuffer : public WriteBuffer
{
public:
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
    Poco::Net::HTTPClientSession & session;
    const Poco::Net::HTTPClientSession::BodyEncoding encoding;
    const UInt64 content_length;
    UInt64 bytes_written = 0;

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
std::unique_ptr<HTTPRequestBodyWriteBuffer> sendHTTPRequest(
    Poco::Net::HTTPClientSession & session,
    Poco::Net::HTTPRequest & request,
    size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
    UInt64 * connect_time = nullptr,
    UInt64 * first_byte_time = nullptr);

/// Reads and parses the response line and the headers into `response`, and returns a buffer for
/// its body. `1xx` informational responses are skipped, as `Poco` does.
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
