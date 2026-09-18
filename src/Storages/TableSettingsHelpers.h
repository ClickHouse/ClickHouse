#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SettingDescription.h>

#include <optional>
#include <string_view>

namespace DB
{

class SettingsChanges;
struct StorageID;

/// Helpers for `IStorage::getTableSettings` overrides. Free functions, since none of them needs the storage
/// beyond its id.
///
/// An override starts from the enumeration of a settings struct - every setting `Default`, `Other` once
/// assigned, or the source a `SettingsWithRecordedOrigin` recorded as it loaded a config section,
/// `compatibility` or a named collection - and corrects each row to what the engine knows. Every engine applies
/// the corrections in one order: the definition, then whatever the engine assigns itself, so that each source
/// overrides the earlier ones as it does when the table is built. `set*` modify in place;
/// `withOriginFromDefinition` returns, since it is usually the last step and most overrides are the single line
/// `return withOriginFromDefinition(settings->enumerateSettings(), getStorageID(), context);`.

/// Maps a name as the definition spells it to the name the settings struct uses, or nullopt when
/// the two are the same. For an engine that accepts legacy spellings its loader rewrites -
/// `ObjectStorageQueue` takes `s3queue_processing_threads_num` for `processing_threads_num` -
/// without declaring them as aliases, so nothing else can know they refer to the same setting.
using SettingNameNormalizer = std::optional<std::string_view> (*)(std::string_view);

/// The `SETTINGS` clause of the table's stored `CREATE` query, copied out. Empty when there is none, or when
/// the catalog does not know the table, as for a table function's storage.
SettingsChanges getSettingsStatedInDefinition(const StorageID & table_id, ContextPtr context);

/// Returns `settings` with every setting the table's own `SETTINGS` clause names marked `Definition`,
/// matching aliases and names as `normalize` rewrites them. The second form takes a clause already read,
/// for an engine that needs its values as well as its names, so that both come from one reading.
SettingDescriptions withOriginFromDefinition(
    SettingDescriptions settings, const StorageID & table_id, ContextPtr context, SettingNameNormalizer normalize = nullptr);
SettingDescriptions withOriginFromDefinition(
    SettingDescriptions settings, const SettingsChanges & stated, SettingNameNormalizer normalize = nullptr);

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

}
