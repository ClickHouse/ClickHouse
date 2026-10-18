#include <Backups/resolveDefaultedSettings.h>

#include <Access/resolveSetting.h>
#include <Core/Settings.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <algorithm>


namespace DB
{

SettingsWithDefaultsResolved resolveDefaultedSettings(
    const ASTBackupQuery & query, std::span<const std::string_view> specific_names, CanonicalSettingNameFn canonical_name)
{
    SettingsWithDefaultsResolved res;

    if (!query.settings)
        return res;

    const auto & settings = query.settings->as<const ASTSetQuery &>();
    res.changes = settings.changes;

    if (settings.default_settings.empty())
        return res;

    auto is_specific = [&](std::string_view name)
    { return std::ranges::find(specific_names, canonical_name(name)) != specific_names.end(); };

    std::vector<std::string_view> defaulted_specific;
    for (const auto & name : settings.default_settings)
    {
        if (is_specific(name))
            defaulted_specific.push_back(canonical_name(name));
        else
            res.core_default_names.push_back(name);
    }

    /// Dropping the change leaves the field at its default. Erase every match: one name may appear several
    /// times.
    std::erase_if(
        res.changes,
        [&](const SettingChange & change)
        { return std::ranges::find(defaulted_specific, canonical_name(change.name)) != defaulted_specific.end(); });

    return res;
}

CoreSettingsFromQuery extractCoreSettings(
    const ASTBackupQuery & query, std::span<const std::string_view> specific_names, CanonicalSettingNameFn canonical_name)
{
    auto resolved = resolveDefaultedSettings(query, specific_names, canonical_name);

    CoreSettingsFromQuery res;
    res.default_names = std::move(resolved.core_default_names);

    for (const auto & setting : resolved.changes)
        if (std::ranges::find(specific_names, canonical_name(setting.name)) == specific_names.end())
            res.changes.emplace_back(setting);

    return res;
}

void eraseOverridesOfResetSettings(SettingsChanges & changes, const std::vector<String> & default_names)
{
    for (const auto & name : default_names)
    {
        /// A reset clears the setting under every name: an alias addresses its canonical field, a `merge_tree_` one is stored as written.
        const std::string_view canonical = Settings::resolveName(name);
        const Strings & equivalent_names = settingEquivalentNames(name);
        std::erase_if(
            changes,
            [&](const SettingChange & change)
            {
                return Settings::resolveName(change.name) == canonical
                    || std::ranges::find(equivalent_names, change.name) != equivalent_names.end();
            });
    }
}

}
