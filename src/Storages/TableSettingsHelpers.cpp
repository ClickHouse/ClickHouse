#include <Storages/TableSettingsHelpers.h>

#include <Common/SettingsChanges.h>
#include <Databases/IDatabase.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/StorageFactory.h>
#include <Common/FieldVisitorToString.h>
#include <Storages/maskEngineSettingValue.h>

#include <algorithm>


namespace DB
{

/// The stored `CREATE` query rather than `StorageInMemoryMetadata::settings_changes`: it is what
/// `SHOW CREATE TABLE` renders and the only source every engine keeps, while `settings_changes` is populated
/// by a few engines only. `ALTER ... MODIFY SETTING` writes back into the `CREATE` query, so this stays current.
EngineStatedInDefinition getEngineStatedInDefinition(const StorageID & table_id, ContextPtr context)
{
    if (table_id.database_name.empty())
        return {};

    const auto database = DatabaseCatalog::instance().tryGetDatabase(table_id.database_name);
    if (!database)
        return {};

    const auto create_query = database->tryGetCreateTableQuery(table_id.table_name, context);
    if (!create_query)
        return {};

    const auto & create = create_query->as<const ASTCreateQuery &>();
    if (!create.storage || !create.storage->settings)
        return {};

    return {
        .engine = create.storage->engine ? create.storage->engine->name : String{},
        .settings = create.storage->settings->as<const ASTSetQuery &>().changes,
    };
}

SettingsChanges getSettingsStatedInDefinition(const StorageID & table_id, ContextPtr context)
{
    return getEngineStatedInDefinition(table_id, context).settings;
}

namespace
{

/// An engine's settings as `system.engine_settings` describes them, less the values: name, default, type,
/// description, tier and aliases, with an index over the name and every alias.
struct EngineSettingsMetadata
{
    SettingDescriptions settings;
    std::unordered_map<String, size_t> by_name;
};

/// The same for every table of an engine, and built by enumerating the engine's whole settings struct - hundreds
/// of rows for `MergeTree` - so a scan of `system.table_settings` would otherwise rebuild it per table. Kept for
/// the life of the program, which costs one copy of each engine's metadata.
///
/// Only what is compiled in is kept - the name, default, type, description, tier and aliases. Two other kinds
/// of field are in a `SettingDescription` and neither belongs here: a value, which an engine with a server-level
/// instance reports from the server and a config reload changes, and the constraints, which
/// `MergeTreeSettings::enumerateEngineSettings` fills from the calling user's profile. This is shared by every
/// user and keyed by the engine name alone, so both are cleared rather than merely left unread - a later caller
/// must not be able to take one from here by mistake.
const EngineSettingsMetadata & engineSettingsMetadata(const String & engine_name, ContextPtr context)
{
    static std::mutex mutex;
    /// Never erased from, so a reference into it stays valid for the caller.
    static std::unordered_map<String, EngineSettingsMetadata> cache;

    std::lock_guard lock(mutex);
    if (const auto it = cache.find(engine_name); it != cache.end())
        return it->second;

    EngineSettingsMetadata metadata;
    const auto & engines = StorageFactory::instance().getAllStorages();
    if (const auto engine = engines.find(engine_name); engine != engines.end())
        if (const auto enumerate = engine->second.features.enumerate_engine_settings_fn)
            metadata.settings = enumerate(context);

    for (size_t i = 0; i < metadata.settings.size(); ++i)
    {
        auto & setting = metadata.settings[i];
        setting.value.clear();
        setting.masked_value.clear();
        setting.named_collection.clear();
        setting.origin = SettingOrigin::Default;
        /// Per user, not compiled in: `MergeTreeSettings::enumerateEngineSettings` fills these from the calling
        /// user's settings constraints, while this is shared by every user and keyed by the engine name alone.
        setting.min_value.reset();
        setting.max_value.reset();
        setting.disallowed_values.clear();
        setting.readonly = false;

        metadata.by_name.emplace(setting.name, i);
        for (const auto & alias : setting.aliases)
            metadata.by_name.emplace(String{alias}, i);
    }

    return cache.emplace(engine_name, std::move(metadata)).first->second;
}

}

/// What a table's own `SETTINGS` clause states, which is all a storage without settings of its own can say.
/// Values come from the AST, so unlike an override backed by a settings struct there is no accessor to give a
/// type-faithful rendering.
SettingDescriptions describeSettingsStatedInDefinition(const StorageID & table_id, ContextPtr context)
{
    const auto [engine_name, changes] = getEngineStatedInDefinition(table_id, context);
    if (changes.empty())
        return {};

    /// The default, type, description, tier and aliases of a setting are compiled in: where the engine lists its
    /// settings for `system.engine_settings`, take them from there, so that the two tables describe it alike.
    const auto & known = engineSettingsMetadata(engine_name, context);

    SettingDescriptions result;
    result.reserve(changes.size());
    for (const auto & change : changes)
    {
        /// A clause may state a setting under an alias; the row then carries the canonical name, as every other
        /// row does. Both are keys of the index, so an alias costs no more than the declared name.
        const auto it = known.by_name.find(change.name);

        SettingDescription described;
        if (it != known.by_name.end())
        {
            const auto & setting = known.settings[it->second];
            described.name = setting.name;
            described.default_value = setting.default_value;
            /// Views outliving the cache entry is what makes this safe: every engine's enumeration points them
            /// at its settings struct's metadata or at a literal, both of which live as long as the program.
            described.type = setting.type;
            described.comment = setting.comment;
            described.tier = setting.tier;
            described.aliases = setting.aliases;
        }
        else
        {
            described.name = change.name;
        }
        described.value = convertFieldToString(change.value);
        described.origin = SettingOrigin::Definition;

        /// Through the same helper the settings-struct path uses, and for the same reason: whether a
        /// value is redacted must not depend on which of the two built the row. A definition can
        /// state `url_base`, `s3_base` or `format_avro_schema_registry_url` with a credential in it,
        /// and `SHOW CREATE TABLE` hides those - so this has to as well.
        described.masked_value = maskEngineSettingValue(described.name, change.value, described.value);

        result.push_back(std::move(described));
    }
    return result;
}

SettingDescriptions withOriginFromDefinition(SettingDescriptions settings, const StorageID & table_id, ContextPtr context)
{
    return withOriginFromDefinition(std::move(settings), getSettingsStatedInDefinition(table_id, context));
}

SettingDescriptions withOriginFromDefinition(SettingDescriptions settings, const SettingsChanges & stated)
{
    NameSetWithViewLookup stated_in_definition;
    for (const auto & change : stated)
        stated_in_definition.insert(change.name);

    for (auto & setting : settings)
    {
        /// A definition may name a setting by any of its aliases - `monitor_batch_inserts` for
        /// `background_insert_batch`, say - so matching only the canonical name would miss it and
        /// report the value as coming from somewhere unknown.
        const bool is_stated = stated_in_definition.contains(setting.name)
            || std::any_of(setting.aliases.begin(), setting.aliases.end(),
                           [&](std::string_view alias) { return stated_in_definition.contains(alias); });
        if (is_stated)
            setting.origin = SettingOrigin::Definition;
    }
    return settings;
}

void setOrigin(SettingDescriptions & settings, const NameSet & names, SettingOrigin origin)
{
    for (auto & setting : settings)
        if (names.contains(setting.name))
            setting.origin = origin;
}

void setOriginByValue(SettingDescriptions & settings)
{
    for (auto & setting : settings)
        if (setting.origin == SettingOrigin::Default || setting.origin == SettingOrigin::Other)
            setting.origin = setting.value == setting.default_value ? SettingOrigin::Default : SettingOrigin::Other;
}

void setEffectiveValue(
    SettingDescriptions & settings, std::string_view name, const String & value, std::optional<SettingOrigin> origin)
{
    const auto it = std::find_if(settings.begin(), settings.end(), [&](const SettingDescription & setting) { return setting.name == name; });
    if (it == settings.end())
        return;

    it->value = value;
    it->masked_value = value != it->default_value ? maskEngineSettingValue(it->name, Field(value), value) : String{};
    if (origin)
        it->origin = *origin;
}

void setEffectiveValueWithConfigFallback(
    SettingDescriptions & settings, std::string_view name, const String & stated, const String & value)
{
    setEffectiveValue(settings, name, value, stated.empty() && !value.empty() ? std::optional(SettingOrigin::Config) : std::nullopt);
}

}
