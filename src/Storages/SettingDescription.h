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
/// `type`, `comment` and `aliases` are views, as `BaseSettings::FieldInfo` holds the same three: whoever fills
/// them must point at storage that lives as long as the program - a string literal, or a settings struct's
/// macro-generated metadata - because a `SettingDescription` is copied and outlives whatever produced it, while
/// nothing here owns those bytes. Making them own would copy every setting's description on every row of
/// `system.engine_settings` and `system.table_settings`, which is most of what those tables carry.
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
    /// The value with its credential hidden, when the setting holds one and this value carries it. Filled during
    /// enumeration, which is the last place the raw `Field` is available - a value can be an AST rather than a
    /// literal, and no plain string form of it hides anything. Empty means nothing of this value has to be
    /// hidden - a reader who may not see it is then shown `[HIDDEN]` in full, as for a named collection's value.
    String masked_value;
    /// Which named collection supplied this value, where `origin` says one did. A reader sees it only where it
    /// may read that collection, and a grant names one, so the row has to say which. Empty where the engine
    /// did not record a name, which is then a value nothing can check and so nothing may see.
    String named_collection;
    /// Whether the value carries something the server configuration supplied rather than the query that stated it -
    /// a macro the engine expanded into a stated `nats_password = '{nats_pw}'`, say. A secret of the server's is
    /// never shown, whoever reads it, as a secret with `origin` `Config` is not: see `SettingRowWriter::masks`.
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
