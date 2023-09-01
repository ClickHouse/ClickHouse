#include <Common/config.h>

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>

namespace
{

struct EncryptImpl
{
    static constexpr auto name = "encrypt";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::OpenSSL;
};

}

namespace DB
{

REGISTER_FUNCTION(Encrypt)
{
    factory.registerFunction<FunctionEncrypt<EncryptImpl>>();
}

}

#endif
