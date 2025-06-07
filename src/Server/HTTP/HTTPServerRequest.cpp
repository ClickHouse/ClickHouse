#include <Server/HTTP/HTTPServerRequest.h>

#if USE_SSL
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <Poco/Net/SSLException.h>
#include <Common/Crypto/X509Certificate.h>
#endif

namespace DB
{

HTTPServerRequest::HTTPServerRequest(
    HTTPRequest request_,
    std::unique_ptr<ReadBuffer> body_,
    const Poco::Net::SocketAddress & client_address_,
    const Poco::Net::SocketAddress & server_address_,
    bool secure_,
    Poco::Net::SocketImpl * socket_)
    : HTTPRequest(std::move(request_)), body(std::move(body_)), client_address(client_address_), server_address(server_address_), secure(secure_), socket(socket_)
{
}

bool HTTPServerRequest::checkPeerConnected() const
{
    try
    {
        char b;
        if (!socket->receiveBytes(&b, 1, MSG_DONTWAIT | MSG_PEEK))
            return false;
    }
    catch (Poco::TimeoutException &) // NOLINT(bugprone-empty-catch)
    {
    }
    catch (...)
    {
        return false;
    }

    return true;
}

#if USE_SSL
bool HTTPServerRequest::havePeerCertificate() const
{
    if (!secure)
        return false;

    const Poco::Net::SecureStreamSocketImpl * secure_socket = dynamic_cast<const Poco::Net::SecureStreamSocketImpl *>(socket);
    if (!secure_socket)
        return false;

    return secure_socket->havePeerCertificate();
}

X509Certificate HTTPServerRequest::peerCertificate() const
{
    if (!secure)
        throw Poco::Net::SSLException("No certificate available");

    const Poco::Net::SecureStreamSocketImpl * secure_socket = dynamic_cast<const Poco::Net::SecureStreamSocketImpl *>(socket);
    if (!secure_socket)
        throw Poco::Net::SSLException("No certificate available");

    return X509Certificate(secure_socket->peerCertificate());
}
#endif

}
