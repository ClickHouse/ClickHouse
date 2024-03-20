#pragma once

#include <IO/WriteBufferFromPocoSocket.h>
#include <Server/HTTP/HTTPResponse.h>

#include <Poco/Net/HTTPServerSession.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/NumberFormatter.h>

#include <memory>


namespace DB
{


class HTTPWriteBufferChunked : public WriteBufferFromPocoSocket
{
    using WriteBufferFromPocoSocket::WriteBufferFromPocoSocket;
protected:
    void nextImpl() override
    {
        if (offset() == 0)
            return;

        std::string chunk_header;
        Poco::NumberFormatter::appendHex(chunk_header, offset());
        chunk_header.append("\r\n", 2);
        socketSendBytes(chunk_header.data(), static_cast<int>(chunk_header.size()));
        WriteBufferFromPocoSocket::nextImpl();
        socketSendBytes("\r\n", 2);
    }

    void finalizeImpl() override
    {
        WriteBufferFromPocoSocket::finalizeImpl();
        socketSendBytes("0\r\n\r\n", 5);
    }
};

class HTTPWriteBufferFixedLength : public WriteBufferFromPocoSocket
{
public:
    explicit HTTPWriteBufferFixedLength(Poco::Net::Socket & socket_, size_t fixed_length_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : WriteBufferFromPocoSocket(socket_, buf_size)
    {
        fixed_length = fixed_length_;
    }
    explicit HTTPWriteBufferFixedLength(Poco::Net::Socket & socket_, size_t fixed_length_, const ProfileEvents::Event & write_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : WriteBufferFromPocoSocket(socket_, write_event_, buf_size)
    {
        fixed_length = fixed_length_;
    }
protected:
    void nextImpl() override
    {
        if (count_length >= fixed_length || offset() == 0)
            return;

        if (count_length + offset() > fixed_length)
            pos -= offset() - (fixed_length - count_length);

        count_length += offset();

        WriteBufferFromPocoSocket::nextImpl();
    }
private:
    size_t fixed_length;
    size_t count_length = 0;
};

/// Universal HTTP buffer, can be switched for different Transfer-Encoding/Content-Length on the fly
/// so it can be used to output HTTP header and then switched to appropriate mode for body
class HTTPWriteBuffer : public WriteBufferFromPocoSocket
{
public:
    explicit HTTPWriteBuffer(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : WriteBufferFromPocoSocket(socket_, buf_size)
    {
    }
    explicit HTTPWriteBuffer(Poco::Net::Socket & socket_, const ProfileEvents::Event & write_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : WriteBufferFromPocoSocket(socket_, write_event_, buf_size)
    {
    }

    void setChunked(size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
    {
        chunked = true;
        resizeIfNeeded(buf_size);
    }

    bool isChunked()
    {
        return chunked;
    }

    void setFixedLength(size_t length)
    {
        chunked = false;
        fixed_length = length;
        count_length = 0;
        resizeIfNeeded(length);
    }

    size_t isFixedLength()
    {
        return chunked ? 0 : fixed_length;
    }

    void setPlain(size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
    {
        chunked = false;
        fixed_length = 0;
        count_length = 0;
        resizeIfNeeded(buf_size);
    }

    bool isPlain()
    {
        return !(isChunked() || isFixedLength());
    }

protected:
    void finalizeImpl() override
    {
        WriteBufferFromPocoSocket::finalizeImpl();
        if (chunked)
            socketSendBytes("0\r\n\r\n", 5);
    }

    void nextImpl() override
    {
        if (chunked)
            return nextImplChunked();

        if (fixed_length)
            return nextImplFixedLength();

        WriteBufferFromPocoSocket::nextImpl();
    }

    void nextImplFixedLength()
    {
        if (count_length >= fixed_length || offset() == 0)
            return;

        if (count_length + offset() > fixed_length)
            pos -= offset() - (fixed_length - count_length);

        count_length += offset();

        WriteBufferFromPocoSocket::nextImpl();
    }

    void nextImplChunked()
    {
        if (offset() == 0)
            return;

        std::string chunk_header;
        Poco::NumberFormatter::appendHex(chunk_header, offset());
        chunk_header.append("\r\n", 2);
        socketSendBytes(chunk_header.data(), static_cast<int>(chunk_header.size()));
        WriteBufferFromPocoSocket::nextImpl();
        socketSendBytes("\r\n", 2);
    }

    void resizeIfNeeded(size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
    {
        if (!buf_size)
            return;

        auto data_size = offset();
        assert(data_size <= buf_size);

        memory.resize(buf_size);
        set(memory.data(), memory.size(), data_size);
    }
private:
    bool chunked = false;
    size_t fixed_length = 0;
    size_t count_length = 0;
};


class HTTPServerRequest;

class HTTPServerResponse : public HTTPResponse
{
public:
    explicit HTTPServerResponse(Poco::Net::HTTPServerSession & session, const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    void sendContinue(); /// Sends a 100 Continue response to the client.

    /// Sends the response header to the client and
    /// returns an output stream for sending the
    /// response body.
    ///
    /// Must not be called after beginSend(), sendFile(), sendBuffer()
    /// or redirect() has been called.
    std::shared_ptr<WriteBufferFromPocoSocket> send();

    /// Sends the response headers to the client
    /// but do not finish headers with \r\n,
    /// allowing to continue sending additional header fields.
    ///
    /// Must not be called after send(), sendFile(), sendBuffer()
    /// or redirect() has been called.
    std::pair<std::shared_ptr<WriteBufferFromPocoSocket>, std::shared_ptr<WriteBufferFromPocoSocket>> beginSend();

    /// Sends the response header to the client, followed
    /// by the contents of the given buffer.
    ///
    /// The Content-Length header of the response is set
    /// to length and chunked transfer encoding is disabled.
    ///
    /// If both the HTTP message header and body (from the
    /// given buffer) fit into one single network packet, the
    /// complete response can be sent in one network packet.
    ///
    /// Must not be called after send(), sendFile()
    /// or redirect() has been called.
    void sendBuffer(const void * pBuffer, std::size_t length); /// FIXME: do we need this one?

    void requireAuthentication(const std::string & realm);
    /// Sets the status code to 401 (Unauthorized)
    /// and sets the "WWW-Authenticate" header field
    /// according to the given realm.

    /// Returns true if the response (header) has been sent.
    bool sent() const { return !!stream; }

    Poco::Net::StreamSocket & getSocket() { return session.socket(); }

    void attachRequest(HTTPServerRequest * request_) { request = request_; }

private:
    Poco::Net::HTTPServerSession & session;
    HTTPServerRequest * request = nullptr;
    ProfileEvents::Event write_event;
    std::shared_ptr<WriteBufferFromPocoSocket> stream;
    std::shared_ptr<WriteBufferFromPocoSocket> header_stream;
};

}
