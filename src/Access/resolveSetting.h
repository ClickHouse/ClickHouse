#pragma once

#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <map>

//
// Settings from different classes (Settings, MergeTreeSettings) can coexist in the same "namespace".
// This is, for example, required to define settings constraints inside user profiles.
// `resolveSetting(full_name)` is used to resolve setting name and choose which class is to be used.
// Templated lambda syntax should be used:
//
// return resolveSetting(name, [] <typename T> (std::string_view name, SettingsType<T>)
// {
//     return T::castValueUtil(name, value); // T will be deduced into `Settings`, `MergeTreeSettings`, ...
// });
//

namespace DB
{

constexpr std::string_view MERGE_TREE_SETTINGS_PREFIX = "merge_tree_";

template <typename T> struct SettingsType {};

// Resolve setting name and call function `f` back with short name and class
template <typename F>
auto resolveSetting(std::string_view full_name, F && f)
{
    if (full_name.starts_with(MERGE_TREE_SETTINGS_PREFIX))
    {
        std::string_view short_name = static_cast<std::string_view>(full_name).substr(MERGE_TREE_SETTINGS_PREFIX.size());
        if (MergeTreeSettings::hasBuiltin(short_name)) // Check is required because `Settings` also contain names starting with 'merge_tree_' prefix
            return f(short_name, SettingsType<MergeTreeSettings>());
    }
    // NOTE: other setting name resolution rules are to be added here

    // If no rule works - use global namespace
    return f(full_name, SettingsType<Settings>());
}

inline Field settingCastValueUtil(std::string_view full_name, const Field & value)
{
    return resolveSetting(full_name, [&] <typename T> (std::string_view short_name, SettingsType<T>)
    {
        return T::castValueUtil(short_name, value);
    });
}

inline Field settingStringToValueUtil(std::string_view full_name, const String & str)
{
    return resolveSetting(full_name, [&] <typename T> (std::string_view short_name, SettingsType<T>)
    {
        return T::stringToValueUtil(short_name, str);
    });
}

inline SettingsTierType settingGetTier(std::string_view full_name)
{
    return resolveSetting(full_name, [&] <typename T> (std::string_view short_name, SettingsType<T>)
    {
        /// A custom setting is not a feature of the server, so no tier applies to it.
        if (!T::hasBuiltin(short_name))
            return SettingsTierType::PRODUCTION;
        return T::getBuiltinTier(short_name);
    });
}

inline bool settingIsBuiltin(std::string_view full_name)
{
    return resolveSetting(full_name, [&] <typename T> (std::string_view short_name, SettingsType<T>)
    {
        return T::hasBuiltin(short_name);
    });
}


template <typename T>
inline String settingFullName(std::string_view short_name);

template <>
inline String settingFullName<Settings>(std::string_view short_name)
{
    return String(short_name);
}

template <>
inline String settingFullName<MergeTreeSettings>(std::string_view short_name)
{
    String full_name(MERGE_TREE_SETTINGS_PREFIX);
    full_name += short_name; // Just because you cannot concatenate `std::string_view` and `std::string` using operator+ in C++20 yet
    return full_name;
}

inline std::string resolveSettingName(std::string_view full_name)
{
    return resolveSetting(
        full_name,
        [&]<typename T>(std::string_view short_name, SettingsType<T>) { return settingFullName<T>(T::resolveName(short_name)); });
}

/// The other names of the same `MergeTreeSettings` setting, prefixed, given any one of them.
///
/// `merge_tree_enable_block_number_column` and `merge_tree_allow_experimental_block_number_column` are one
/// setting, so each of them returns the other here. `Settings` stores such a name as a custom setting under
/// the exact name that wrote it, so a value written under one name must be looked for under all of them.
inline const Strings & settingEquivalentNames(std::string_view full_name)
{
    static const std::map<String, Strings, std::less<>> equivalent_names = []
    {
        /// Group the names by the setting they mean, then point each name at the others in its group.
        static const MergeTreeSettings all_settings;
        std::map<String, Strings, std::less<>> groups;
        for (const auto & alias : all_settings.getAllAliasNames())
        {
            auto canonical = settingFullName<MergeTreeSettings>(MergeTreeSettings::resolveName(alias));
            groups[canonical].push_back(settingFullName<MergeTreeSettings>(alias));
        }

        std::map<String, Strings, std::less<>> result;
        for (const auto & [canonical, aliases] : groups)
        {
            result[canonical] = aliases;
            for (const auto & alias : aliases)
            {
                Strings & others = result[alias];
                others.push_back(canonical);
                for (const auto & other : aliases)
                {
                    if (other != alias)
                        others.push_back(other);
                }
            }
        }
        return result;
    }();

    static const Strings none;
    auto it = equivalent_names.find(full_name);
    return it == equivalent_names.end() ? none : it->second;
}

}
