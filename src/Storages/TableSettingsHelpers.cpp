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

namespace
{

/// The engine the query names - the name it is registered under, which `IStorage::getName` need not be: an
/// `AzureBlobStorage` table calls itself `Azure` - and the `SETTINGS` clause. Both empty in the same cases.
struct EngineStatedInDefinition
{
    String engine;
    SettingsChanges settings;
};

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

/// An engine's settings as `system.engine_settings` describes them, less the values: name, default, type,
/// description, tier and aliases, with an index over the name and every alias.
struct EngineSettingsMetadata
{
    SettingDescriptions settings;
    std::unordered_map<String, size_t> by_name;
};

/// Built by enumerating the engine's whole settings struct - hundreds of rows for `MergeTree` - so kept for the life
/// of the program rather than rebuilt per table. Only what is compiled in is kept: the value, which a config reload
/// changes, and the constraints, which come from the calling user's profile, are cleared in a cache every user shares.
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

SettingsChanges getSettingsStatedInDefinition(const StorageID & table_id, ContextPtr context)
{
    return getEngineStatedInDefinition(table_id, context).settings;
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
        /// A clause may state a setting under an alias; the row carries the canonical name, as every other row does.
        const auto it = known.by_name.find(change.name);

        SettingDescription described;
        if (it != known.by_name.end())
        {
            const auto & setting = known.settings[it->second];
            described.name = setting.name;
            described.default_value = setting.default_value;
            /// Views into storage that lives as long as the program: see `SettingDescription`.
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

        /// As the settings-struct path masks, so that whether a value is redacted does not depend on which of the
        /// two built the row: a definition can state `url_base` or `s3_base` with a credential in it.
        described.masked_value = maskEngineSettingValue(described, change.value);

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
        /// A definition may name a setting by an alias: `monitor_batch_inserts` for `background_insert_batch`.
        const bool is_stated = stated_in_definition.contains(setting.name)
            || std::any_of(setting.aliases.begin(), setting.aliases.end(),
                           [&](std::string_view alias) { return stated_in_definition.contains(alias); });
        if (is_stated)
            setting.origin = SettingOrigin::Definition;
    }
    return settings;
}

void markSecretsFromServerConfiguration(SettingDescriptions & settings, const NameSet & names)
{
    for (auto & setting : settings)
        /// Only a secret: a macro expanded into anything else - a topic, a path - is no more than the definition
        /// and `system.macros` say, and is reported as it is.
        if (!setting.masked_value.empty() && names.contains(setting.name))
            setting.from_server_configuration = true;
}

void setOrigin(SettingDescriptions & settings, const NameSet & names, SettingOrigin origin)
{
    for (auto & setting : settings)
        if (names.contains(setting.name))
            setting.origin = origin;
}

SettingDescriptions withOriginByValue(SettingDescriptions settings)
{
    for (auto & setting : settings)
        if (setting.origin == SettingOrigin::Default || setting.origin == SettingOrigin::Other)
            setting.origin = setting.value == setting.default_value ? SettingOrigin::Default : SettingOrigin::Other;
    return settings;
}

void setEffectiveValue(
    SettingDescriptions & settings, std::string_view name, const String & value, std::optional<SettingOrigin> origin)
{
    const auto it = std::find_if(settings.begin(), settings.end(), [&](const SettingDescription & setting) { return setting.name == name; });
    if (it == settings.end())
        return;

    it->value = value;
    it->masked_value = value != it->default_value ? maskEngineSettingValue(*it, Field(value)) : String{};
    if (origin)
        it->origin = *origin;
    /// A value an engine takes from its server config section is the server's, secret or not: the username of a
    /// `rabbitmq` section says as much about the server as its password, and `SHOW CREATE TABLE` shows neither.
    if (origin == SettingOrigin::Config)
        it->from_server_configuration = true;
}

void setEffectiveValueWithConfigFallback(
    SettingDescriptions & settings, std::string_view name, const String & stated, const String & value)
{
    setEffectiveValue(settings, name, value, stated.empty() && !value.empty() ? std::optional(SettingOrigin::Config) : std::nullopt);
}

}
