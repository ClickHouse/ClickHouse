#include "UseSSL.h"

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_POCO_NETSSL
#    include <Poco/Net/SSLManager.h>
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
