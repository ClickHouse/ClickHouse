#include <Storages/TableSettingsHelpers.h>

#include <Common/SettingsChanges.h>
#include <Databases/IDatabase.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/maskEngineSettingValue.h>

#include <algorithm>


namespace DB
{

/// The stored `CREATE` query rather than `StorageInMemoryMetadata::settings_changes`: it is what
/// `SHOW CREATE TABLE` renders and the only source every engine keeps, while `settings_changes` is populated
/// by a few engines only. `ALTER ... MODIFY SETTING` writes back into the `CREATE` query, so this stays current.
SettingsChanges getSettingsStatedInDefinition(const StorageID & table_id, ContextPtr context)
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

    return create.storage->settings->as<const ASTSetQuery &>().changes;
}

SettingDescriptions withOriginFromDefinition(
    SettingDescriptions settings, const StorageID & table_id, ContextPtr context, SettingNameNormalizer normalize)
{
    return withOriginFromDefinition(std::move(settings), getSettingsStatedInDefinition(table_id, context), normalize);
}

SettingDescriptions withOriginFromDefinition(
    SettingDescriptions settings, const SettingsChanges & stated, SettingNameNormalizer normalize)
{
    NameSet stated_in_definition;
    for (const auto & change : stated)
    {
        std::optional<std::string_view> canonical;
        if (normalize)
            canonical = normalize(change.name);
        stated_in_definition.insert(canonical ? String{*canonical} : change.name);
    }

    for (auto & setting : settings)
    {
        /// A definition may name a setting by any of its aliases - `monitor_batch_inserts` for
        /// `background_insert_batch`, say - so matching only the canonical name would miss it and
        /// report the value as coming from somewhere unknown.
        const bool is_stated = stated_in_definition.contains(setting.name)
            || std::any_of(setting.aliases.begin(), setting.aliases.end(),
                           [&](std::string_view alias) { return stated_in_definition.contains(String{alias}); });
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
