#include "config.h"

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>

namespace DB
{

namespace
{

struct DecryptImpl
{
    static constexpr auto name = "decrypt";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::OpenSSL;
    static constexpr bool use_null_when_decrypt_fail = false;
};

}

REGISTER_FUNCTION(Decrypt)
{
    FunctionDocumentation::Description description = R"(
This function decrypts an AES-encrypted binary string using the following modes:

- aes-128-ecb, aes-192-ecb, aes-256-ecb
- aes-128-cbc, aes-192-cbc, aes-256-cbc
- aes-128-ofb, aes-192-ofb, aes-256-ofb
- aes-128-gcm, aes-192-gcm, aes-256-gcm
- aes-128-ctr, aes-192-ctr, aes-256-ctr
- aes-128-cfb, aes-128-cfb1, aes-128-cfb8
        )";
    FunctionDocumentation::Syntax syntax = "decrypt(mode, ciphertext, key[, iv, aad])";
    FunctionDocumentation::Arguments arguments = {
        {"mode", "Decryption mode.", {"String"}},
        {"ciphertext", "Encrypted text that should be decrypted.", {"String"}},
        {"key", "Decryption key.", {"String"}},
        {"iv", "Initialization vector. Required for `-gcm` modes, optional for others.", {"String"}},
        {"aad", "Additional authenticated data. Won't decrypt if this value is incorrect. Works only in `-gcm` modes, for others throws an exception.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns decrypted plaintext.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Correctly decrypting encrypted data",
            R"(
-- Re-using the table from the encrypt function example
SELECT comment, hex(secret) FROM encryption_test;
            )",
            R"(
┌─comment──────────────┬─hex(secret)──────────────────────────────────┐
│ aes-256-gcm          │ A8A3CCBC6426CFEEB60E4EAE03D3E94204C1B09E0254 │
│ aes-256-gcm with AAD │ A8A3CCBC6426D9A1017A0A932322F1852260A4AD6837 │
└──────────────────────┴──────────────────────────────────────────────┘
┌─comment──────────────────────────┬─hex(secret)──────────────────────┐
│ aes-256-ofb no IV                │ B4972BDC4459                     │
│ aes-256-ofb no IV, different key │ 2FF57C092DC9                     │
│ aes-256-ofb with IV              │ 5E6CB398F653                     │
│ aes-256-cbc no IV                │ 1BC0629A92450D9E73A00E7D02CF4142 │
└──────────────────────────────────┴──────────────────────────────────┘
            )"
        },
        {
            "Incorrectly decrypting encrypted data",
            R"(
SELECT comment, decrypt('aes-256-cfb128', secret, '12345678910121314151617181920212') AS plaintext FROM encryption_test
            )",
            R"(
-- Notice how only a portion of the data was properly decrypted, and the rest is gibberish since either `mode`, `key`, or `iv` were different upon encryption.
┌─comment──────────────┬─plaintext──┐
│ aes-256-gcm          │ OQ�E
                             �t�7T�\���\�   │
│ aes-256-gcm with AAD │ OQ�E
                             �\��si����;�o�� │
└──────────────────────┴────────────┘
┌─comment──────────────────────────┬─plaintext─┐
│ aes-256-ofb no IV                │ Secret    │
│ aes-256-ofb no IV, different key │ �4�
                                        �         │
│ aes-256-ofb with IV              │ ���6�~        │
│aes-256-cbc no IV                │ �2*4�h3c�4w��@
└──────────────────────────────────┴───────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encryption;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDecrypt<DecryptImpl>>(documentation);
}

}

#endif
