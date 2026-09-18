#pragma once

#include <Core/SettingsTierType.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

/// Where the effective value of a table setting came from. Exposed as `system.table_settings.source`.
///
/// When several sources wrote a setting, the one reported is whichever wrote it last, and that order is the
/// engine's. The values are declared in the order the engines apply them - each later source overriding the
/// earlier ones, as when the table is built - and the column is an `Enum8` of them, so `ORDER BY source` sorts
/// by that precedence: keep it when adding a value. Most engines apply the table's own `SETTINGS` clause after
/// a config section, `compatibility` and a named collection, so `Definition` outranks those. `S3Queue` and
/// `AzureQueue` apply `SharedMetadata` after the definition, deliberately: an `ALTER ... MODIFY SETTING` on
/// another replica has already changed the value this replica uses while its own `CREATE` query still states
/// the old one. An engine that adds a source has to decide where it belongs relative to the definition.
enum class SettingOrigin : uint8_t
{
    Default,          /// the engine's compiled-in default
    Config,           /// a server config section, e.g. <merge_tree> or <distributed>
    Compatibility,    /// rolled back to an older release's default by the `compatibility` setting
    /// A named collection the table was built from, for the settings it actually supplied - not those the
    /// engine arguments overrode. Reported by the engines that record them when the table is created: `Kafka`,
    /// `PostgreSQL`, `MySQL`, `NATS` and `RabbitMQ`.
    NamedCollection,
    Definition,       /// the table's own SETTINGS clause, whether from CREATE or a later ALTER
    SharedMetadata,   /// replicated table metadata, e.g. Keeper for S3Queue and AzureQueue
    /// Adjusted by the engine while it runs, and not written back to its settings - the case
    /// `SHOW CREATE TABLE` cannot serve. ⚠️ Reserved: nothing reports it yet. The intended first
    /// producer is https://github.com/ClickHouse/ClickHouse/pull/116522 (`StorageKafka` halving
    /// `kafka_max_block_size`); declared now so the values stay in the order they are applied in.
    Runtime,
    Other,            /// the engine does not report an origin for this setting
};

std::string_view toString(SettingOrigin origin);

/// One setting with everything known about it, as reported by whichever instance produced it.
///
/// Not table-specific: a default-constructed settings object describes an engine
/// (`system.engine_settings`, `system.merge_tree_settings`), the object a storage holds describes a
/// table (`system.table_settings`). Only in the second case can `origin` be `Definition` or
/// `SharedMetadata`.
///
/// `type` and `comment` are `string_view` because a settings struct owns them statically. They are
/// empty, along with `default_value`, when a setting is known only from a table's `SETTINGS` clause.
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
    /// The value with its credential hidden, when the setting holds one. Filled during enumeration,
    /// which is the last place the raw `Field` is available - a value can be an AST rather than a
    /// literal, and no plain string form of it hides anything. Empty when nothing is secret.
    String masked_value;
};

using SettingDescriptions = std::vector<SettingDescription>;

/// The two entry points a settings struct exposes to `system.engine_settings` and
/// `system.table_settings`. Goes in the struct's body, paired with
/// `IMPLEMENT_SETTINGS_ENUMERATION` in its .cpp, where the settings implementation is complete.
///
/// `enumerateEngineSettings` is defined here because it is the same for every engine that has no
/// server-level settings instance: the compiled defaults are what such an engine uses. The two
/// that do have one, `MergeTree` and `Distributed`, declare and define their own.
/// NOLINTBEGIN(bugprone-macro-parentheses): the argument is a type name, which cannot be parenthesized.
#define DECLARE_SETTINGS_ENUMERATION(TYPE) \
    static SettingDescriptions enumerateEngineSettings(ContextPtr) { return TYPE{}.enumerateSettings(); } \
    SettingDescriptions enumerateSettings() const;
/// NOLINTEND(bugprone-macro-parentheses)

}
