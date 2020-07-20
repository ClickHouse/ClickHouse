#include "UseSSL.h"

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_SSL
#    include <Poco/Net/SSLManager.h>
#endif

namespace DB
{
UseSSL::UseSSL()
{
#if USE_SSL
    Poco::Net::initializeSSL();
#endif
}

UseSSL::~UseSSL()
{
#if USE_SSL
    Poco::Net::uninitializeSSL();
#endif
}
}
