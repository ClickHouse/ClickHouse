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
    static void cleanup();

    bool isFIPSEnabled() const;

private:
    OpenSSLInitializer();
    ~OpenSSLInitializer();

#if USE_SSL
    static std::atomic<bool> initialize_done;
    static std::atomic<bool> cleanup_done;

    static OSSL_PROVIDER * legacy_provider;
    static OSSL_PROVIDER * default_provider;
#endif
};

}
