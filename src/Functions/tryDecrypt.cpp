#include <Common/FunctionDocumentation.h>
#include "config.h"

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>


namespace DB
{

namespace
{

struct TryDecryptImpl
{
    static constexpr auto name = "tryDecrypt";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::OpenSSL;
    static constexpr bool use_null_when_decrypt_fail = true;
};

}

REGISTER_FUNCTION(TryDecrypt)
{
    factory.registerFunction<FunctionDecrypt<TryDecryptImpl>>(FunctionDocumentation{
        .description="Similar to `decrypt`, but returns NULL if decryption fails because of using the wrong key."});
}

}

#endif
