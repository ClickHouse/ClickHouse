#pragma once

#include <span>
#include <string_view>
#include <vector>
#include <sys/types.h>

#include <Interpreters/SecretArgumentsSpec.h>
#include <Parsers/SecretArguments.h>

namespace DB
{

/// Finds the secret arguments of one function call by interpreting its `SecretArgumentsSpec`. The helpers are
/// public: `SecretArgumentsSpec::custom` callbacks use them from the translation unit of each engine.
class FunctionSecretArgumentsFinder
{
public:
    using Result = SecretArgumentsResult;

    explicit FunctionSecretArgumentsFinder(const AbstractFunction & function_) : function(&function_) {}

    /// Applies the generic rules (unless disabled), then the declared slots and keys, then `custom`.
    void apply(const SecretArgumentsSpec & spec, bool generic_rules);

    const AbstractFunction * const function;
    Result result;

    void markSecretArgument(size_t index, bool argument_is_named = false);

    /// Hides every argument, for a shape whose valid slots cannot be established.
    void maskEveryArgument();

    /// Records the nested map `name(..)` (e.g. `headers(..)`), written at any position, so its values are
    /// hidden with the keys kept, except the values of `visible_keys`.
    void maskNestedSecretMap(std::string_view name, std::vector<std::string> visible_keys = {});

    /// The raw indexes of the arguments from `start` on that are not `key = value` pairs, in order. A
    /// positional argument after the first named one is hidden instead of listed: its slot is unknowable.
    std::vector<size_t> classifyPositionalArguments(size_t start = 0);

    /// Hides the value of every `key = value` argument from `start` on whose key is not a plain literal.
    void markNamedArgumentsWithUnreadableKeys(size_t start);

    /// Looks for an argument with a specified name. This function looks for arguments in format `key=value` where the key is specified.
    /// Returns -1 if no argument was found.
    ssize_t findNamedArgument(String * res, std::string_view key, size_t start = 0);

    /// Looks for secret arguments with a specified name in format `key=value` and marks them secret.
    /// Marks *every* occurrence, not just the first: a malformed query is formatted for logging before
    /// duplicate-key validation runs, so `session_token = 'a', session_token = 'b'` must hide both.
    bool findSecretNamedArgument(std::string_view key, size_t start = 0);

    /// The shape of the brokers (`NATS`, `RabbitMQ`): the only positional argument is the name of a named
    /// collection, the secrets are `secret_keys` overrides, and `address_key` is hidden when it carries an '@'.
    void findBrokerSecretArguments(std::span<const std::string_view> secret_keys, std::string_view address_key);

    bool tryGetStringFromArgument(size_t arg_idx, String * res, bool allow_identifier = true) const;
    static bool tryGetStringFromArgument(const AbstractFunction::Argument & argument, String * res, bool allow_identifier = true);

    /// `BackupInfo` keeps named overrides and a trailing map for every backup engine, including the
    /// ones that read neither, so an argument that is not a plain literal can carry a credential.
    static bool hasOnlyLiteralArguments(const AbstractFunction & function);

    /// Whether a specified argument can be the name of a named collection?
    bool isNamedCollectionName(size_t arg_idx) const;

private:
    void findPositionalAndNamedSecretArguments(const SecretArgumentsSpec & spec);
};

}
