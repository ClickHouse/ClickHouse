#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>

namespace
{

struct EncryptMySQLModeImpl
{
    static constexpr auto name = "aes_encrypt_mysql";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::MySQL;
};

}

namespace DB
{

void registerFunctionAESEncryptMysql(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEncrypt<EncryptMySQLModeImpl>>();
}

}

#endif
