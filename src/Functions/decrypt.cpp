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
CREATE TABLE encryption_test
(
    `comment` String,
    `secret` String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO encryption_test VALUES
('aes-256-ofb no IV', encrypt('aes-256-ofb', 'Secret', '12345678910121314151617181920212')),
('aes-256-ofb no IV, different key', encrypt('aes-256-ofb', 'Secret', 'keykeykeykeykeykeykeykeykeykeyke')),
('aes-256-ofb with IV', encrypt('aes-256-ofb', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv')),
('aes-256-cbc no IV', encrypt('aes-256-cbc', 'Secret', '12345678910121314151617181920212'));

SELECT comment, hex(secret) FROM encryption_test;
            )",
            R"(
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
SELECT comment, hex(decrypt('aes-256-cfb8', secret, '12345678910121314151617181920212')) AS plaintext FROM encryption_test
            )",
            R"DOCS_MD(
aes-256-ofb no IV	53A69A28E815
aes-256-ofb no IV, different key	C86BD2404D8A
aes-256-ofb with IV	B9658159142A
aes-256-cbc no IV	FC552E5096F1455997B2732FB31DCFEB
            )DOCS_MD"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encryption;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDecrypt<DecryptImpl>>(documentation);
}

}

#endif
