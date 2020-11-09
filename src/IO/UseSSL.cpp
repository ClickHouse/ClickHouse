#include "UseSSL.h"

#include <Common/config.h>

#if USE_POCO_NETSSL
#include <Poco/Net/SSLManager.h>
#endif

namespace DB
{
UseSSL::UseSSL()
{
#if USE_POCO_NETSSL
    Poco::Net::initializeSSL();
#endif
}

UseSSL::~UseSSL()
{
#if USE_POCO_NETSSL
    Poco::Net::uninitializeSSL();
#endif
}
}
