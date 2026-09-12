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
/// When more than one of these wrote a setting, the one reported is whichever wrote it last - and
/// that order is decided by the engine, not by the order of this list.
///
/// Most engines apply the table's own `SETTINGS` clause last, so `Definition` outranks `Config`,
/// `Compatibility` and `NamedCollection`: a setting named in the definition reports `Definition`
/// even when a config section or a named collection also names it.
///
/// `S3Queue` and `AzureQueue` are the exception, deliberately. They apply `SharedMetadata` *after*
/// the definition, because an `ALTER ... MODIFY SETTING` run on another replica has already changed
/// the value this replica uses while its own `CREATE` query still states what it was created with.
/// Reporting `Definition` there would name a source the engine does not consult.
///
/// So the order below is the usual one rather than a contract every engine keeps. An engine that
/// adds a source has to decide where it belongs relative to the definition, and say so.
enum class TableSettingOrigin : uint8_t
{
    Default,          /// the engine's compiled-in default
    Config,           /// a server config section, e.g. <merge_tree> or <distributed>
    Compatibility,    /// rolled back to an older release's default by the `compatibility` setting
    Definition,       /// the table's own SETTINGS clause, whether from CREATE or a later ALTER
    NamedCollection,  /// a named collection referenced in the engine arguments
    SharedMetadata,   /// replicated table metadata, e.g. Keeper for S3Queue and AzureQueue
    /// Adjusted by the engine while it runs, and not written back to its settings.
    ///
    /// ⚠️ Reserved: nothing reports this yet. It exists because a value that drifts at run time is
    /// the case `SHOW CREATE TABLE` fundamentally cannot serve - the AST is only rewritten by DDL -
    /// and so is a large part of why this surface exists at all. The first intended producer is
    /// https://github.com/ClickHouse/ClickHouse/pull/116522, which halves `StorageKafka`'s
    /// `kafka_max_block_size` after a memory limit error and computes the effective value at the use
    /// site rather than storing it. That pull request is not merged, so the value is kept unused
    /// rather than added later: appending it afterwards would have to take a number out of step with
    /// the order below, which is the order these are applied in.
    Runtime,
    Other,            /// the engine does not report an origin for this setting
};

std::string_view toString(TableSettingOrigin origin);

/// One setting of one table, as it is actually in effect.
///
/// `type` and `description` are `string_view` because a settings struct owns them statically. They
/// are empty, along with `default_value`, when a setting is known only from the table's `SETTINGS`
/// clause - an engine that keeps no settings struct has nothing else to report about it.
struct TableSetting
{
    String name;
    String value;
    String default_value;
    std::string_view type;
    std::string_view description;
    /// Other names this setting may be stated under. A definition may use any of them, so
    /// attribution has to match on all of them, not just `name`.
    std::vector<std::string_view> aliases;
    TableSettingOrigin origin = TableSettingOrigin::Other;
    SettingsTierType tier = SettingsTierType::PRODUCTION;
    /// From the current user's settings constraints. Only `MergeTreeSettings` can be constrained -
    /// a profile reaches those through the `merge_tree_` name prefix, and there is no equivalent for
    /// any other engine - so these stay empty elsewhere. That is not a gap peculiar to this table:
    /// in `system.settings` and `system.merge_tree_settings` they are empty for every row until a
    /// profile declares a constraint.
    std::optional<String> min_value;
    std::optional<String> max_value;
    std::vector<String> disallowed_values;
    /// Whether a constraint, or the engine itself, makes the setting read-only. As in the two tables
    /// above, `false` means only that nothing marks it read-only.
    bool readonly = false;
    /// The value with its credential hidden, when the setting holds one. Filled during enumeration,
    /// which is the last place the raw `Field` is available - a value can be an AST rather than a
    /// literal, and no plain string form of it hides anything. Empty when nothing is secret.
    String masked_value;
};

using TableSettings = std::vector<TableSetting>;

/// The two entry points a settings struct exposes to `system.engine_settings` and
/// `system.table_settings`. Goes in the struct's body, paired with
/// `IMPLEMENT_SETTINGS_ENUMERATION` in its .cpp, where the settings implementation is complete.
///
/// `enumerateEngineSettings` is defined here because it is the same for every engine that has no
/// server-level settings instance: the compiled defaults are what such an engine uses. The two
/// that do have one, `MergeTree` and `Distributed`, declare and define their own.
/// NOLINTBEGIN(bugprone-macro-parentheses): the argument is a type name, which cannot be parenthesized.
#define DECLARE_SETTINGS_ENUMERATION(TYPE) \
    static TableSettings enumerateEngineSettings(ContextPtr) { return TYPE{}.enumerateSettings(); } \
    TableSettings enumerateSettings() const;
/// NOLINTEND(bugprone-macro-parentheses)

}
