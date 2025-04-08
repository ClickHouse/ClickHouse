#include "UseSSL.h"

#include "config.h"

#include <Common/Exception.h>

#if USE_SSL
#    include <openssl/provider.h>
#    include <openssl/crypto.h>
#    include <openssl/ssl.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
}

#if USE_SSL
OSSL_PROVIDER * DB::UseSSL::default_provider = nullptr;
OSSL_PROVIDER * DB::UseSSL::legacy_provider = nullptr;
#endif

UseSSL::UseSSL()
{
#if USE_SSL
    if (ref_count++ == 0)
    {
        int basic_init = OPENSSL_init_ssl(OPENSSL_INIT_LOAD_CONFIG, nullptr);
        if (!basic_init)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to initialize OpenSSL.");

        default_provider = OSSL_PROVIDER_load(nullptr, "default");
        if (!default_provider)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to load 'default' provider.");

        legacy_provider = OSSL_PROVIDER_load(nullptr, "legacy");
        if (!legacy_provider)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to load 'legacy' provider.");
    }
#endif
}

UseSSL::~UseSSL()
{
#if USE_SSL
    if (--ref_count == 0)
    {
        if (legacy_provider)
        {
            OSSL_PROVIDER_unload(legacy_provider);
            legacy_provider = nullptr;
        }

        if (default_provider)
        {
            OSSL_PROVIDER_unload(default_provider);
            default_provider = nullptr;
        }
    }
#endif
}
}
