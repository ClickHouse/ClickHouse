#include <Common/checkSSLReturnCode.h>
#include "config.h"

#if USE_SSL
#include <Poco/Net/SecureStreamSocket.h>
#endif

namespace DB
{

bool checkSSLWantRead(ssize_t res)
{
#if USE_SSL
    return res == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_READ;
#else
    return false;
#endif
}

bool checkSSLWantWrite(ssize_t res)
{
#if USE_SSL
    return res == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE;
#else
    return false;
#endif
}

}
