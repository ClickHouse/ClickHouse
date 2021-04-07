#pragma once

#include <common/types.h>

#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace DB
{

class Secret;

/** Holds a value of a sceret.
 *
 *  Ensures that no copying is needed to access the actual value and that it doesn't go out of scope
 *  while SecretValue instance is held.
 *
 *  Assumes that Secret itself is never changed.
 */
class SecretValue
{
    std::shared_ptr<const Secret> secret;
    const String & value;

public:
    SecretValue(std::shared_ptr<const Secret> secret, std::string_view path);
    ~SecretValue();

    const String & str() const
    {
        return value;
    }
};

class ISecretsProvider
{
protected:
    ~ISecretsProvider() = default;

public:
    // Gets value from the secret vault, where path is a dot-separated path ("a.b.c") to a value,
    // e.g. "local.mysql.password" or "local.mysql.username".
    virtual SecretValue getSecretValue(std::string_view path_to_secret) const = 0;

    using ReplacamentFunc = std::function<String(std::string_view)>;
    // Removes all secret values from the text, replacing those with the result of replacement_func(secret_name).
    virtual String redactOutAllSecrets(const String & text_with_secrets, ReplacamentFunc replacement_func) const = 0;
};

}
