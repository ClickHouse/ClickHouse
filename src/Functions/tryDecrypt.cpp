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
    factory.registerFunction<FunctionDecrypt<TryDecryptImpl>>(FunctionDocumentation{
        .description="Similar to `decrypt`, but returns NULL if decryption fails because of using the wrong key.",
        .syntax="tryDecrypt('mode', 'ciphertext', 'key' [, iv, aad])",
        .arguments={
            {"mode", "Decryption mode. [String](/sql-reference/data-types/string)."},
            {"ciphertext", "Encrypted text that needs to be decrypted. [String](/sql-reference/data-types/string)."},
            {"key", "Decryption key. [String](/sql-reference/data-types/string)."},
            {"iv", "Initialization vector. Required for `-gcm` modes, Optional for others. [String](/sql-reference/data-types/string)."},
            {"aad", "Additional authenticated data. Won't decrypt if this value is incorrect. Works only in `-gcm` modes, for others would throw an exception. [String](/sql-reference/data-types/string)."}
        },
        .returned_value="Decrypted String, or `NULL` if decryption fails. [String](/sql-reference/data-types/string).",
        .examples={{
            "Example",
            R"(
Let's create a table where `user_id` is the unique user id, `encrypted` is an encrypted string field, `iv` is an initial vector for decrypt/encrypt. Assume that users know their id and the key to decrypt the encrypted field:

```sql
CREATE TABLE decrypt_null (
  dt DateTime,
  user_id UInt32,
  encrypted String,
  iv String
) ENGINE = Memory;
```

Insert some data:

```sql
INSERT INTO decrypt_null VALUES
    ('2022-08-02 00:00:00', 1, encrypt('aes-256-gcm', 'value1', 'keykeykeykeykeykeykeykeykeykey01', 'iv1'), 'iv1'),
    ('2022-09-02 00:00:00', 2, encrypt('aes-256-gcm', 'value2', 'keykeykeykeykeykeykeykeykeykey02', 'iv2'), 'iv2'),
    ('2022-09-02 00:00:01', 3, encrypt('aes-256-gcm', 'value3', 'keykeykeykeykeykeykeykeykeykey03', 'iv3'), 'iv3');
```

```sql
SELECT
    dt,
    user_id,
    tryDecrypt('aes-256-gcm', encrypted, 'keykeykeykeykeykeykeykeykeykey02', iv) AS value
FROM decrypt_null
ORDER BY user_id ASC
```
            )",
            R"(
```response
┌──────────────────dt─┬─user_id─┬─value──┐
│ 2022-08-02 00:00:00 │       1 │ ᴺᵁᴸᴸ   │
│ 2022-09-02 00:00:00 │       2 │ value2 │
│ 2022-09-02 00:00:01 │       3 │ ᴺᵁᴸᴸ   │
└─────────────────────┴─────────┴────────┘
```
            )"
        }},
        .category=FunctionDocumentation::Category::Encryption
    });
}

}

#endif
