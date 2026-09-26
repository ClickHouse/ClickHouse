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

/// Helpers for `IStorage::getTableSettings` overrides. Free functions, since none of them needs the storage
/// beyond its id.
///
/// There are two ways a table answers for its settings. Most engines *record*: the settings object marks each
/// value with the source that assigned it as it is assigned, so the override is the enumeration and needs none of
/// these helpers. The rest *reconstruct*, because their settings object cannot be believed, in one of four ways:
///
///   - the engine keeps no settings object at all, so nothing recorded anything - `Join`, `Set`, the `IStorage`
///     base: `getSettingsStatedInDefinition` and `withOriginFromDefinition` read the stored `CREATE` query;
///   - the object is rebuilt on every call, so every setting in it reads as assigned and the marks say nothing -
///     `ObjectStorageQueue`: `setOriginByValue` recovers the distinction from the value;
///   - the loader assigns every setting from the session before the table's own sources, with the same effect -
///     `PostgreSQL`: `setOriginByValue` again;
///   - the engine derives what it works with after loading - an expanded macro, a generated id, a value taken
///     from a server config section when the table gave none - so the object holds what it was given rather than
///     what it uses: `setEffectiveValue` and `setEffectiveValueWithConfigFallback` report the latter.
///
/// `set*` modify in place; `withOriginFromDefinition` returns.

/// The `SETTINGS` clause of the table's stored `CREATE` query, copied out. Empty when there is none, or when
/// the catalog does not know the table, as for a table function's storage.
SettingsChanges getSettingsStatedInDefinition(const StorageID & table_id, ContextPtr context);

/// The same, with the engine the query names: the name the engine is registered under, which `IStorage::getName`
/// need not be - an `AzureBlobStorage` table calls itself `Azure`. Both empty in the same cases.
struct EngineStatedInDefinition
{
    String engine;
    SettingsChanges settings;
};
EngineStatedInDefinition getEngineStatedInDefinition(const StorageID & table_id, ContextPtr context);

/// What a table's own `SETTINGS` clause states, described with the metadata the engine's registered settings give
/// each stated setting. The base `IStorage::getTableSettings` is this, for a storage that keeps no settings of its
/// own; an override that does keep them reports those instead.
SettingDescriptions describeSettingsStatedInDefinition(const StorageID & table_id, ContextPtr context);

/// Returns `settings` with every setting the table's own `SETTINGS` clause names marked `Definition`, matching
/// aliases too. The second form takes a clause already read, for an engine that needs its values as well as its
/// names, so that both come from one reading.
SettingDescriptions withOriginFromDefinition(SettingDescriptions settings, const StorageID & table_id, ContextPtr context);
SettingDescriptions withOriginFromDefinition(SettingDescriptions settings, const SettingsChanges & stated);

/// Sets `origin` for every setting in `names`, matched by canonical name only, not by alias. Used where the
/// engine assigns settings outside its loaders, so the settings object cannot record the source.
void setOrigin(SettingDescriptions & settings, const NameSet & names, SettingOrigin origin);

/// Recomputes `origin` from the value alone: `Default` where it equals the compiled-in default and
/// `Other` where it does not. For an engine whose loader assigns every setting - from the session, as
/// `PostgreSQLSettings::loadFromQueryContext` does, or by rebuilding the struct - which marks them all as
/// changed even where the value is the default, so the change alone says nothing about where it is from.
/// Only settings that enumeration left at `Default` or `Other`: a source the settings object recorded is
/// known whatever the value.
void setOriginByValue(SettingDescriptions & settings);

/// Replaces the reported value of setting `name` with the value the engine actually works with, masked as
/// enumeration masks it, and sets `origin` when given. For an engine that derives its working values after
/// loading its settings - by macro expansion, a generated default or a server config fallback.
void setEffectiveValue(
    SettingDescriptions & settings, std::string_view name, const String & value, std::optional<SettingOrigin> origin = {});

/// The same for a value the engine takes from a server config section when the table's own is empty: reported
/// as coming from the config when `stated` is empty and `value` is not.
void setEffectiveValueWithConfigFallback(
    SettingDescriptions & settings, std::string_view name, const String & stated, const String & value);

/// The same two, taking the setting's typed index rather than its name - which is how an engine should name
/// one of its own settings. A name that is misspelled, or that a later release renames, then fails to compile
/// rather than matching no row and leaving the value the engine does not use in the table. The name itself is
/// resolved from the offset, so the index stays one word wide: `nameAtOffset` reads it from the traits, which
/// only the settings class's own .cpp can see.
template <typename Owner, typename FieldType>
void setEffectiveValue(
    SettingDescriptions & settings,
    SettingIndex<Owner, FieldType> setting,
    const String & value,
    std::optional<SettingOrigin> origin = {})
{
    setEffectiveValue(settings, Owner::nameAtOffset(setting.offset), value, origin);
}

template <typename Owner, typename FieldType>
void setEffectiveValueWithConfigFallback(
    SettingDescriptions & settings, SettingIndex<Owner, FieldType> setting, const String & stated, const String & value)
{
    setEffectiveValueWithConfigFallback(settings, Owner::nameAtOffset(setting.offset), stated, value);
}

}
