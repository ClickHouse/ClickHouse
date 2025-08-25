#include <Common/FunctionDocumentation.h>
#include "config.h"

#if USE_SSL

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsAES.h>


namespace DB
{

namespace
{

struct TryDecryptImpl
{
    static constexpr auto name = "tryDecrypt";
    static constexpr auto compatibility_mode = OpenSSLDetails::CompatibilityMode::OpenSSL;
    static constexpr bool use_null_when_decrypt_fail = true;
};

}

REGISTER_FUNCTION(TryDecrypt)
{
    FunctionDocumentation::Description description = R"(
Similar to the `decrypt` function, but returns `NULL` if decryption fails when using the wrong key.
        )";
    FunctionDocumentation::Syntax syntax = "tryDecrypt(mode, ciphertext, key[, iv, aad])";
    FunctionDocumentation::Arguments arguments = {
        {"mode", "Decryption mode.", {"String"}},
        {"ciphertext", "Encrypted text that should be decrypted.", {"String"}},
        {"key", "Decryption key.", {"String"}},
        {"iv", "Optional. Initialization vector. Required for `-gcm` modes, optional for other modes.", {"String"}},
        {"aad", "Optional. Additional authenticated data. Won't decrypt if this value is incorrect. Works only in `-gcm` modes, for other modes throws an exception.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the decrypted String, or `NULL` if decryption fails.", {"Nullable(String)"}};
    FunctionDocumentation::Examples examples = {
        {
            "Create table and insert data",
            R"(
-- Let's create a table where user_id is the unique user id, encrypted is an encrypted string field, iv is an initial vector for decrypt/encrypt.
-- Assume that users know their id and the key to decrypt the encrypted field:
CREATE TABLE decrypt_null
(
    dt DateTime,
    user_id UInt32,
    encrypted String,
    iv String
)
ENGINE = MergeTree;

-- Insert some data:
INSERT INTO decrypt_null VALUES
('2022-08-02 00:00:00', 1, encrypt('aes-256-gcm', 'value1', 'keykeykeykeykeykeykeykeykeykey01', 'iv1'), 'iv1'),
('2022-09-02 00:00:00', 2, encrypt('aes-256-gcm', 'value2', 'keykeykeykeykeykeykeykeykeykey02', 'iv2'), 'iv2'),
('2022-09-02 00:00:01', 3, encrypt('aes-256-gcm', 'value3', 'keykeykeykeykeykeykeykeykeykey03', 'iv3'), 'iv3');

-- Try decrypt with one key
SELECT
    dt,
    user_id,
    tryDecrypt('aes-256-gcm', encrypted, 'keykeykeykeykeykeykeykeykeykey02', iv) AS value
FROM decrypt_null
ORDER BY user_id ASC
            )",
            R"(
┌──────────────────dt─┬─user_id─┬─value──┐
│ 2022-08-02 00:00:00 │       1 │ ᴺᵁᴸᴸ   │
│ 2022-09-02 00:00:00 │       2 │ value2 │
│ 2022-09-02 00:00:01 │       3 │ ᴺᵁᴸᴸ   │
└─────────────────────┴─────────┴────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encryption;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDecrypt<TryDecryptImpl>>(documentation);
}

}

#endif
