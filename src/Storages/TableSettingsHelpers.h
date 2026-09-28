#pragma once

#include <Common/SettingsChanges.h>
#include <Core/SettingIndex.h>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Core/Names.h>
#include <Storages/SettingDescription.h>

#include <optional>
#include <string_view>

namespace DB
{

struct StorageID;

/// A set of names a `std::string_view` can be looked up in without building a `String` for the lookup - which
/// the settings paths would otherwise do per alias, per setting, per table.
using NameSetWithViewLookup
    = std::unordered_set<String, StringHashForHeterogeneousLookup, StringHashForHeterogeneousLookup::transparent_key_equal>;

/// Helpers for `IStorage::getTableSettings` overrides. Most engines need none: their settings object records the
/// source of each value as it is assigned. The rest reconstruct it:
///   - no settings object - `Join`, `Set`, the `Log` family, the `IStorage` base: what the stored `CREATE` states,
///     through `describeSettingsStatedInDefinition` or `withOriginFromDefinition`;
///   - a loader or copy that marks every setting changed - `PostgreSQL`, `ObjectStorageQueue`, `Distributed`,
///     the `Log` family: `withOriginByValue`;
///   - a working value derived after loading - an expanded macro, a generated id, a value from a server config
///     section: `setEffectiveValue` and `setEffectiveValueWithConfigFallback`.

/// The `SETTINGS` clause of the table's stored `CREATE` query, copied out. Empty when there is none, or when
/// the catalog does not know the table, as for a table function's storage.
SettingsChanges getSettingsStatedInDefinition(const StorageID & table_id, ContextPtr context);

/// What a table's own `SETTINGS` clause states, described with the metadata the engine's registered settings give
/// each stated setting. The base `IStorage::getTableSettings` is this, for a storage that keeps no settings of its
/// own; an override that does keep them reports those instead.
SettingDescriptions describeSettingsStatedInDefinition(const StorageID & table_id, ContextPtr context);

/// Returns `settings` with every setting the table's own `SETTINGS` clause names marked `Definition`, matching
/// aliases too. The second form takes a clause already read, for an engine that needs its values as well as its
/// names, so that both come from one reading.
SettingDescriptions withOriginFromDefinition(SettingDescriptions settings, const StorageID & table_id, ContextPtr context);
SettingDescriptions withOriginFromDefinition(SettingDescriptions settings, const SettingsChanges & stated);

/// Marks `from_server_configuration` on every setting in `names` - by its declared name - that holds a secret: for an
/// engine that expands macros from the server configuration into its settings, and knows which values that changed,
/// whatever supplied the rest of the value - the table's clause, an engine argument or a named collection.
void markSecretsFromServerConfiguration(SettingDescriptions & settings, const NameSet & names);

/// Sets `origin` for every setting in `names`, matched by canonical name only, not by alias. Used where the
/// engine assigns settings outside its loaders, so the settings object cannot record the source.
void setOrigin(SettingDescriptions & settings, const NameSet & names, SettingOrigin origin);

/// Returns `settings` with `origin` recomputed from the value - `Default` where it equals the compiled-in default, else
/// `Other` - for an engine whose loader assigns every setting, marking them all changed. A recorded source is kept.
SettingDescriptions withOriginByValue(SettingDescriptions settings);

/// Replaces the reported value of setting `name` with the value the engine actually works with, masked as
/// enumeration masks it, and sets `origin` when given. For an engine that derives its working values after
/// loading its settings - by macro expansion, a generated default or a server config fallback. A value whose
/// `origin` is `Config` came from the engine's server config section, and is never shown.
void setEffectiveValue(
    SettingDescriptions & settings, std::string_view name, const String & value, std::optional<SettingOrigin> origin = {});

/// The same for a value the engine takes from a server config section when the table's own is empty: reported
/// as coming from the config when `stated` is empty and `value` is not.
void setEffectiveValueWithConfigFallback(
    SettingDescriptions & settings, std::string_view name, const String & stated, const String & value);

/// The same two by the setting's typed index, which is how an engine should name its own setting: a misspelled or
/// renamed one then fails to compile rather than matching no row.
template <typename Owner, typename FieldType>
void setEffectiveValue(
    SettingDescriptions & settings,
    SettingIndex<Owner, FieldType> setting,
    const String & value,
    std::optional<SettingOrigin> origin = {})
{
    setEffectiveValue(settings, Owner::nameAtOffset(setting.offset), value, origin);
}

/// `stated` is what `owner`, the table's settings object, holds for the setting.
template <typename Owner, typename FieldType>
void setEffectiveValueWithConfigFallback(
    SettingDescriptions & settings, const Owner & owner, SettingIndex<Owner, FieldType> setting, const String & value)
{
    setEffectiveValueWithConfigFallback(settings, Owner::nameAtOffset(setting.offset), owner[setting].value, value);
}

}
