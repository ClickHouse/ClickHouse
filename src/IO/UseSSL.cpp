#include "UseSSL.h"

#include "config.h"

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
