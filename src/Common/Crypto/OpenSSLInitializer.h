#pragma once

#include "config.h"

#include <boost/noncopyable.hpp>
#include <atomic>

#if USE_SSL
#    include <openssl/provider.h>
#endif

namespace DB
{

struct OpenSSLInitializer : private boost::noncopyable
{
public:
    static OpenSSLInitializer & instance()
    {
        static OpenSSLInitializer instance;
        return instance;
    }

    static void initialize();

private:
    OpenSSLInitializer();
    ~OpenSSLInitializer();

#if USE_SSL
    static std::atomic<uint8_t> ref_count;
    static OSSL_PROVIDER * legacy_provider;
    static OSSL_PROVIDER * default_provider;
#endif
};

}
