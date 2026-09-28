#pragma once

#include <Core/SettingOrigin.h>
#include <Core/SettingsTierType.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

std::string_view toString(SettingOrigin origin);

/// One setting with everything known about it, as reported by whichever instance produced it.
///
/// Not table-specific: a default-constructed settings object describes an engine
/// (`system.engine_settings`, `system.merge_tree_settings`), the object a storage holds describes a
/// table (`system.table_settings`). Only in the second case can `origin` be `Definition` or
/// `SharedMetadata`.
///
/// `type`, `comment` and `aliases` are views, as in `BaseSettings::FieldInfo`: whoever fills them must point at storage
/// that lives as long as the program - a literal or a settings struct's metadata - since a `SettingDescription` outlives
/// whatever produced it. Owning them would copy every description into every row of the settings tables.
///
/// `type` and `comment` are empty, along with `default_value`, when a setting is known only from a table's
/// `SETTINGS` clause.
struct SettingDescription
{
    String name;
    String value;
    String default_value;
    std::string_view type;
    std::string_view comment;
    /// Other names this setting may be stated under. A definition may use any of them, so
    /// attribution has to match on all of them, not just `name`.
    std::vector<std::string_view> aliases;
    SettingOrigin origin = SettingOrigin::Other;
    SettingsTierType tier = SettingsTierType::PRODUCTION;
    /// From the current user's settings constraints. Only `MergeTreeSettings` can be constrained (a
    /// profile reaches them through the `merge_tree_` prefix), so these stay empty for other engines.
    std::optional<String> min_value;
    std::optional<String> max_value;
    std::vector<String> disallowed_values;
    /// Whether the setting cannot be changed: a settings constraint makes it read-only, or the engine
    /// refuses to change it on an existing table (`MergeTreeSettings::isReadonlySetting`).
    bool readonly = false;
    /// The value with its credential hidden, filled during enumeration while the raw `Field` - possibly an AST - is at
    /// hand. Empty where the value holds no credential.
    String masked_value;
    /// Which named collection supplied this value, where `origin` says one did, since a grant names one. Empty where
    /// the engine recorded none, which no reader may then see.
    String named_collection;
    /// Whether the value is the server configuration's rather than the query's: taken from an engine's server config
    /// section, or a secret with a macro expanded into it - a stated `nats_password = '{nats_pw}'`, say. Never
    /// shown: see `SettingRowWriter::masks`.
    bool from_server_configuration = false;
};

using SettingDescriptions = std::vector<SettingDescription>;

/// For `system.engine_settings`: the compiled defaults of `TSettings`, which is what an engine without a server-level
/// settings instance uses. Registered as `.enumerate_engine_settings_fn = enumerateCompiledDefaults<TSettings>`; the
/// engines that have such an instance - `MergeTree`, `Distributed` - register a function of their own.
///
/// `TSettings::enumerateSettings` describes one instance, for `system.table_settings` as well; every settings struct
/// declares it and defines it with `IMPLEMENT_SETTINGS_ENUMERATION`.
template <typename TSettings>
SettingDescriptions enumerateCompiledDefaults(ContextPtr)
{
    return TSettings{}.enumerateSettings();
}

}
