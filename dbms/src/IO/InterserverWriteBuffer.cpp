#include <IO/InterserverWriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>

#include <Poco/Version.h>
#include <Poco/URI.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_OSTREAM;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

InterserverWriteBuffer::InterserverWriteBuffer(const std::string & host_, int port_,
    const std::string & endpoint_,
    const std::string & path_,
    bool compress_,
    size_t buffer_size_,
    const Poco::Timespan & connection_timeout,
    const Poco::Timespan & send_timeout,
    const Poco::Timespan & receive_timeout)
    : WriteBuffer(nullptr, 0), host(host_), port(port_), path(path_)
{
    std::string encoded_path;
    Poco::URI::encode(path, "&#", encoded_path);

    std::string encoded_endpoint;
    Poco::URI::encode(endpoint_, "&#", encoded_endpoint);

    std::string compress_str = compress_ ? "true" : "false";
    std::string encoded_compress;
    Poco::URI::encode(compress_str, "&#", encoded_compress);

    std::stringstream uri;
    uri << "http://" << host << ":" << port
        << "/?endpoint=" << encoded_endpoint
        << "&compress=" << encoded_compress
        << "&path=" << encoded_path;

    std::string uri_str = Poco::URI(uri.str()).getPathAndQuery();

    session.setHost(host);
    session.setPort(port);
    session.setKeepAlive(true);

    /// set the timeout
#if POCO_CLICKHOUSE_PATCH || POCO_VERSION >= 0x02000000
    session.setTimeout(connection_timeout, send_timeout, receive_timeout);
#else
    session.setTimeout(connection_timeout);
    static_cast <void> (send_timeout);
    static_cast <void> (receive_timeout);
#endif

    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri_str, Poco::Net::HTTPRequest::HTTP_1_1);

    request.setChunkedTransferEncoding(true);

    ostr = &session.sendRequest(request);
    impl = std::make_unique<WriteBufferFromOStream>(*ostr, buffer_size_);
    set(impl->buffer().begin(), impl->buffer().size());
}

InterserverWriteBuffer::~InterserverWriteBuffer()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void InterserverWriteBuffer::nextImpl()
{
    if (!offset() || finalized)
        return;

    /// For correct work with AsynchronousWriteBuffer, which replaces buffers.
    impl->set(buffer().begin(), buffer().size());

    impl->position() = pos;

    impl->next();
}

void InterserverWriteBuffer::finalize()
{
    if (finalized)
        return;

    next();

    finalized = true;
}

void InterserverWriteBuffer::cancel()
{
    finalized = true;
}

}
