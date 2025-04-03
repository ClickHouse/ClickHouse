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
    if (ref_count++ == 0)
    {
        OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, nullptr);
    }
#endif
}

UseSSL::~UseSSL()
{
#if USE_SSL
    // if (--ref_count == 0)
    // {
    //
    // }
#endif
}
}
