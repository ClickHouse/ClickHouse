#pragma once

#include <base/types.h>

#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

class Field;
class FunctionSecretArgumentsFinder;

/// What is secret in the arguments of a table function, a table, database or backup engine, a function or a
/// dictionary source. Every factory registration declares one, next to the parser of the arguments it describes.
/// An engine without secrets declares `SecretArgumentsSpec{}` explicitly.
struct SecretArgumentsSpec
{
    /// Positional slots that carry a secret, counted over positional arguments only (named `key = value`
    /// arguments are excluded before counting, like the engine parsers do).
    std::vector<size_t> positional_secret_slots = {};
    /// Keys whose value is secret, both as `key = value` arguments (named-collection overrides included) and as
    /// keys of a dictionary `SOURCE(...)`.
    std::vector<String> secret_keys = {};
    /// Keys of a dictionary `SOURCE(...)` whose value is only partly secret, with the masker that rewrites the
    /// formatted value in place and returns whether anything was masked.
    std::unordered_map<String, std::function<bool(String &)>> partial = {};
    /// Settings of the engine's `SETTINGS` clause whose value is secret, with the SQL text that hides the value
    /// (`nullopt`: nothing to hide in it).
    std::unordered_map<String, std::function<std::optional<String>(const Field &)>> secret_settings = {};
    /// The engine also takes its settings as `key = value` arguments, overrides of a named collection
    /// (`NATS(collection, nats_password = '...')`): such an argument is masked like the same setting.
    bool settings_as_arguments = false;
    /// Shapes the fields cannot express (the signature depends on the arity or on the value of another argument).
    /// Runs after the fields, in the translation unit of the engine's argument parser.
    std::function<void(FunctionSecretArgumentsFinder &)> custom = {};
    /// `custom` reads interleaved named and positional arguments itself (`bigquery`), so the generic rules are
    /// not applied: a positional after a named argument and the value of a key that is not a literal are hidden.
    bool custom_reads_named_arguments = false;

    bool hasArgumentSecrets() const
    {
        return !positional_secret_slots.empty() || !secret_keys.empty() || (settings_as_arguments && !secret_settings.empty()) || custom;
    }
};

}
