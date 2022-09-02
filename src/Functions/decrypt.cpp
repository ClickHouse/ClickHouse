#include <Common/config.h>

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>

namespace
{

struct DecryptImpl
{
    static constexpr auto name = "decrypt";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::OpenSSL;
};

}

namespace DB
{

void registerFunctionDecrypt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDecrypt<DecryptImpl>>();
}

}

#endif
