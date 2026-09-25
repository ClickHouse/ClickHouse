#pragma once

#include <Core/SettingsTierType.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

/// The origin of a table setting's effective value - where it came from. Exposed as the `source` column of
/// `system.table_settings`, `system.engine_settings` and `system.merge_tree_settings`; "origin" in the code,
/// "source" where that column is meant.
///
/// When several of them wrote a setting, the one reported is whichever wrote it last, and that order is the
/// engine's. The values are declared in the order the engines apply them - each later source overriding the
/// earlier ones, as when the table is built - and the column is an `Enum8` of them, so `ORDER BY source` sorts
/// by that precedence: keep it when adding a value. `Other` is the exception - the catch-all, set wherever an
/// engine cannot tell the source, including before any other. Most engines apply the table's own `SETTINGS`
/// clause after a config section, `compatibility` and a named collection, so `Definition` outranks those.
/// An engine that adds a source has to decide where it belongs relative to the definition - `S3Queue` and
/// `AzureQueue` put `SharedMetadata` after it, which `docs/reference/system-tables/table_settings.mdx` explains.
enum class SettingOrigin : uint8_t
{
    /// The engine's compiled-in default. Also what `SettingsWithRecordedOrigin` stores for "nothing recorded",
    /// which is why it must stay the first value.
    Default,
    Config,           /// a server config section, e.g. <merge_tree> or <distributed>
    Compatibility,    /// rolled back to an older release's default by the `compatibility` setting
    /// A named collection the table was built from, for the settings it actually supplied - not those the
    /// engine arguments overrode. Reported by the engines whose settings object records them as it loads the
    /// collection: `Kafka`, `PostgreSQL`, `MySQL`, `NATS` and `RabbitMQ`.
    NamedCollection,
    Definition,       /// the table's own SETTINGS clause, whether from CREATE or a later ALTER
    SharedMetadata,   /// replicated table metadata, e.g. Keeper for S3Queue and AzureQueue
    /// The engine does not report an origin for this setting - including a value it adjusts while it runs and
    /// does not write back. A value for that belongs before this one, in the order above. Enumeration also sets
    /// this for every setting that is merely changed, before a storage's override refines it.
    ///
    /// Staying last is what lets `SettingsWithRecordedOrigin` check the whole enum by checking this one: it
    /// stores an origin in four bits, so there is room for sixteen values in all.
    Other,
};

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
