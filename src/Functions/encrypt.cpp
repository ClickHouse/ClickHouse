#include "config.h"

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>

namespace DB
{

namespace
{

struct EncryptImpl
{
    static constexpr auto name = "encrypt";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::OpenSSL;
};

}

REGISTER_FUNCTION(Encrypt)
{
    FunctionDocumentation::Description description = R"(
Encrypts plaintext into ciphertext using AES in one of the following modes:

- aes-128-ecb, aes-192-ecb, aes-256-ecb
- aes-128-cbc, aes-192-cbc, aes-256-cbc
- aes-128-ofb, aes-192-ofb, aes-256-ofb
- aes-128-gcm, aes-192-gcm, aes-256-gcm
- aes-128-ctr, aes-192-ctr, aes-256-ctr
- aes-128-cfb, aes-128-cfb1, aes-128-cfb8
        )";
    FunctionDocumentation::Syntax syntax = "encrypt(mode, plaintext, key[, iv, aad])";
    FunctionDocumentation::Arguments arguments = {
        {"mode", "Encryption mode.", {"String"}},
        {"plaintext", "Text that should be encrypted.", {"String"}},
        {"key", "Encryption key.", {"String"}},
        {"iv", "Initialization vector. Required for `-gcm` modes, optional for others.", {"String"}},
        {"aad", "Additional authenticated data. It isn't encrypted, but it affects decryption. Works only in `-gcm` modes, for others it throws an exception.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns binary string ciphertext.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Example encryption",
            R"(
CREATE TABLE encryption_test
(
    `comment` String,
    `secret` String
)
ENGINE = MergeTree;

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
            "Example with GCM mode",
            R"(
INSERT INTO encryption_test VALUES
('aes-256-gcm', encrypt('aes-256-gcm', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv')),

('aes-256-gcm with AAD', encrypt('aes-256-gcm', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv', 'aad'));

SELECT comment, hex(secret) FROM encryption_test WHERE comment LIKE '%gcm%';
            )",
            R"(
┌─comment──────────────┬─hex(secret)──────────────────────────────────┐
│ aes-256-gcm          │ A8A3CCBC6426CFEEB60E4EAE03D3E94204C1B09E0254 │
│ aes-256-gcm with AAD │ A8A3CCBC6426D9A1017A0A932322F1852260A4AD6837 │
└──────────────────────┴──────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encryption;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionEncrypt<EncryptImpl>>(documentation);
}

}

#endif
