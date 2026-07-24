#include "config.h"

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>


namespace DB
{

namespace
{

struct DecryptMySQLModeImpl
{
    static constexpr auto name = "aes_decrypt_mysql";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::MySQL;
    static constexpr bool use_null_when_decrypt_fail = false;
};

}

REGISTER_FUNCTION(AESDecryptMysql)
{
    factory.registerFunction<FunctionDecrypt<DecryptMySQLModeImpl>>();
}

}

#endif
