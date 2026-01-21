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
    FunctionDocumentation::Description description = R"(
Encrypts text the same way as MySQL's `AES_ENCRYPT` function does.
The resulting ciphertext can be decrypted with MySQL's `AES_DECRYPT` function.
Produces the same ciphertext as the `encrypt` function for the same inputs.
When `key` or `iv` are longer than they should normally be, `aes_encrypt_mysql` will stick to what MySQL's `aes_encrypt` does which is to 'fold' `key` and ignore the excess bits of `iv`.

The supported encryption modes are:

- aes-128-ecb, aes-192-ecb, aes-256-ecb
- aes-128-cbc, aes-192-cbc, aes-256-cbc
- aes-128-ofb, aes-192-ofb, aes-256-ofb
        )";
    FunctionDocumentation::Syntax syntax = "aes_encrypt_mysql(mode, plaintext, key[, iv])";
    FunctionDocumentation::Arguments arguments = {
        {"mode", "Encryption mode.", {"String"}},
        {"plaintext", "Text that should be encrypted.", {"String"}},
        {"key", "Encryption key. If the key is longer than required by `mode`, MySQL-specific key folding is performed.", {"String"}},
        {"iv", "Optional. Initialization vector. Only the first 16 bytes are taken into account.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Ciphertext binary string.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Equal input comparison",
            R"(
-- Given equal input encrypt and aes_encrypt_mysql produce the same ciphertext:
SELECT encrypt('aes-256-ofb', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv') = aes_encrypt_mysql('aes-256-ofb', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv') AS ciphertexts_equal;
            )",
            R"(
┌─ciphertexts_equal─┐
│                 1 │
└───────────────────┘
              )"
        },
        {
            "Encrypt fails with long key",
            R"(
-- But encrypt fails when key or iv is longer than expected:
SELECT encrypt('aes-256-ofb', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123');
            )",
            R"(
Received exception from server (version 22.6.1):
Code: 36. DB::Exception: Received from localhost:9000. DB::Exception: Invalid key size: 33 expected 32: While processing encrypt('aes-256-ofb', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123').
            )"
        },
        {
            "MySQL compatibility",
            R"(
-- aes_encrypt_mysql produces MySQL-compatible output:
SELECT hex(aes_encrypt_mysql('aes-256-ofb', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123')) AS ciphertext;
            )",
            R"(
┌─ciphertext───┐
│ 24E9E4966469 │
└──────────────┘
            )"
        },
        {
            "Longer IV produces the same result",
            R"(
-- Notice how supplying even longer IV produces the same result
SELECT hex(aes_encrypt_mysql('aes-256-ofb', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123456')) AS ciphertext
            )",
            R"(
┌─ciphertext───┐
│ 24E9E4966469 │
└──────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encryption;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionEncrypt<EncryptMySQLModeImpl>>(documentation);
}

}

#endif
