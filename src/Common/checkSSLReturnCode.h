#pragma once
#include <sys/types.h>

namespace Poco::Net
{
class SocketImpl;
}

namespace DB
{

/// Check if ret is ERR_SSL_WANT_READ.
bool checkSSLWantRead(ssize_t ret);

/// CHeck if ret is ERR_SSL_WANT_WRITE.
bool checkSSLWantWrite(ssize_t ret);

/// Check if the TLS handshake of the socket has not completed, so the next read or write runs it.
bool secureHandshakePending(const Poco::Net::SocketImpl * socket);

}
