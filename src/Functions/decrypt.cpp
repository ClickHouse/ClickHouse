#include "config.h"

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>

namespace
{

struct DecryptImpl
{
    static constexpr auto name = "decrypt";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::OpenSSL;
    static constexpr bool use_null_when_decrypt_fail = false;
};

}

namespace DB
{

REGISTER_FUNCTION(Decrypt)
{
    factory.registerFunction<FunctionDecrypt<DecryptImpl>>();
}

}

#endif
