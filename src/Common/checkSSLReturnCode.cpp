#include <Common/checkSSLReturnCode.h>
#include "config.h"

#if USE_SSL
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SecureStreamSocketImpl.h>
#endif

namespace DB
{

bool checkSSLWantRead([[maybe_unused]] ssize_t ret)
{
#if USE_SSL
    return ret == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_READ;
#else
    return false;
#endif
}

bool checkSSLWantWrite([[maybe_unused]] ssize_t ret)
{
#if USE_SSL
    return ret == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE;
#else
    return false;
#endif
}

bool secureHandshakePending([[maybe_unused]] const Poco::Net::SocketImpl * socket)
{
#if USE_SSL
    const auto * secure_socket = dynamic_cast<const Poco::Net::SecureStreamSocketImpl *>(socket);
    return secure_socket && secure_socket->needHandshake();
#else
    return false;
#endif
}

}
