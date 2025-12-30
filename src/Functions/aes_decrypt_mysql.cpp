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
    FunctionDocumentation::Description description = R"(
Decrypts data encrypted by MySQL's [`AES_ENCRYPT`](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-encrypt) function.

Produces the same plaintext as [`decrypt`](#decrypt) for the same inputs.
When `key` or `iv` are longer than they should normally be, `aes_decrypt_mysql` will stick to what MySQL's `aes_decrypt` does which is to 'fold' `key` and ignore the excess bits of `IV`.

Supports the following decryption modes:

- aes-128-ecb, aes-192-ecb, aes-256-ecb
- aes-128-cbc, aes-192-cbc, aes-256-cbc
- aes-128-cfb128
- aes-128-ofb, aes-192-ofb, aes-256-ofb
        )";
    FunctionDocumentation::Syntax syntax = "aes_decrypt_mysql(mode, ciphertext, key[, iv])";
    FunctionDocumentation::Arguments arguments = {
        {"mode", "Decryption mode.", {"String"}},
        {"ciphertext", "Encrypted text that needs to be decrypted.", {"String"}},
        {"key", "Decryption key.", {"String"}},
        {"iv", "Optional. Initialization vector.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the decrypted String.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Decrypt MySQL data",
        R"(
-- Let's decrypt data we've previously encrypted with MySQL:
mysql> SET  block_encryption_mode='aes-256-ofb';
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT aes_encrypt('Secret', '123456789101213141516171819202122', 'iviviviviviviviv123456') as ciphertext;
+------------------------+
| ciphertext             |
+------------------------+
| 0x24E9E4966469         |
+------------------------+
1 row in set (0.00 sec)

SELECT aes_decrypt_mysql('aes-256-ofb', unhex('24E9E4966469'), '123456789101213141516171819202122', 'iviviviviviviviv123456') AS plaintext
        )",
        R"(
┌─plaintext─┐
│ Secret    │
└───────────┘
        )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encryption;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDecrypt<DecryptMySQLModeImpl>>(documentation);
}

}

#endif
