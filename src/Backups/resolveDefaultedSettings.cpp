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

    /// Dropping the change is what makes the setting take its default value: the fields of
    /// BackupSettings/RestoreSettings are default-initialized and only a `changes` entry overrides one.
    /// Erase every match, not just the first, since one name may appear repeatedly.
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

void appendCoreDefaultsAsChanges(SettingsChanges & changes, const std::vector<String> & default_names)
{
    if (default_names.empty())
        return;

    /// The value a reset produces: `Context::resetSettingsToDefaultValue` assigns the declared default, and
    /// `SettingsConstraints::checkResetToDefault` checks a reset as an assignment of that same value.
    const Settings declared_defaults;

    for (const auto & name : default_names)
    {
        /// A name that is not a built-in setting has no declared default to send, so dropping every
        /// override of it is what leaves the receiver where the reset leaves the initiator: with the
        /// setting absent. A `merge_tree_` setting is stored under the exact name that wrote it, so an
        /// override written through any of that setting's names is an override of it.
        if (!Settings::hasBuiltin(name))
        {
            const Strings & equivalent_names = settingEquivalentNames(name);
            std::erase_if(
                changes,
                [&](const SettingChange & change)
                {
                    return change.name == name
                        || std::ranges::find(equivalent_names, change.name) != equivalent_names.end();
                });
            continue;
        }

        Field default_value = declared_defaults.get(name);

        /// `operator Field` is not invertible for every setting type, and then the `Field` does not carry
        /// the reset. `SettingFieldMaxThreads::operator Field` drops `is_auto` and yields the resolved
        /// thread count, so shipping it would pin every receiving host to this host's number instead of
        /// letting each recompute its own auto value (`max_insert_threads`, `max_final_threads` and
        /// `max_parsing_threads` are the same field type). Reconstructing the field from that `Field` and
        /// comparing its text to the default's text detects exactly the types where this happens; there
        /// the default's own text is what resets the setting, because `parseFromString` restores the auto
        /// form (see `stringToMaxThreads`).
        const String default_string = declared_defaults.getDefaultValueString(name);
        if (Settings::valueToStringUtil(name, default_value) != default_string)
            default_value = default_string;

        changes.emplace_back(name, std::move(default_value));
    }
}

}
