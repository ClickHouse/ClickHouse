#include "config.h"

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>

namespace DB
{
namespace
{

struct EncryptMySQLModeImpl
{
    static constexpr auto name = "aes_encrypt_mysql";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::MySQL;
};

}

REGISTER_FUNCTION(AESEncryptMysql)
{
    factory.registerFunction<FunctionEncrypt<EncryptMySQLModeImpl>>();
}

}

#endif
