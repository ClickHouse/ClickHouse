#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsFields.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Storages/ColumnsDescription.h>

namespace Poco { namespace Util { class AbstractConfiguration; } } // NOLINT(cppcoreguidelines-virtual-class-destructor)

namespace DB
{
struct FileCacheSettingsImpl;
class NamedCollection;
struct MutableColumnsAndConstraints;

#define FILE_CACHE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Double) \
    M(CLASS_NAME, FileCachePolicy) \
    M(CLASS_NAME, UInt64)

FILE_CACHE_SETTINGS_SUPPORTED_TYPES(FileCacheSettings, DECLARE_SETTING_TRAIT)

struct FileCacheSettings
{
    FileCacheSettings();
    FileCacheSettings(const FileCacheSettings & settings);
    FileCacheSettings(FileCacheSettings && settings) noexcept;
    FileCacheSettings & operator=(FileCacheSettings && settings) noexcept;
    bool operator==(const FileCacheSettings & settings) const noexcept;
    ~FileCacheSettings();

    static ColumnsDescription getColumnsDescription();

    FILE_CACHE_SETTINGS_SUPPORTED_TYPES(FileCacheSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const std::string & cache_path_prefix_if_relative,
        const std::string & default_cache_path = "");

    void loadFromCollection(
        const NamedCollection & collection,
        const std::string & cache_path_prefix_if_relative);

    void dumpToSystemSettingsColumns(
        MutableColumnsAndConstraints & params,
        const std::string & cache_name,
        const FileCachePtr & cache) const;

    void validate();

    bool isPathRelativeInConfig() const { return is_path_relative_in_config; }

private:
    std::unique_ptr<FileCacheSettingsImpl> impl;
    bool is_path_relative_in_config = false;
};

}
