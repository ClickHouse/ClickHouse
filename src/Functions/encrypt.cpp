#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

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

void registerFunctionEncrypt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEncrypt<EncryptImpl>>();
}

}

#endif
