#pragma once

#include "config.h"

#include <boost/noncopyable.hpp>
#include <atomic>

#if USE_SSL
#    include <openssl/provider.h>
#endif

namespace DB
{
// http://stackoverflow.com/questions/18315472/https-request-in-c-using-poco
struct UseSSL : private boost::noncopyable
{
public:
    static UseSSL & instance()
    {
        static UseSSL instance;
        return instance;
    }

private:
    UseSSL();
    ~UseSSL();

#if USE_SSL
    static std::atomic<uint8_t> ref_count;
    static OSSL_PROVIDER * legacy_provider;
    static OSSL_PROVIDER * default_provider;
#endif
};
}
