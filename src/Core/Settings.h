#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/Field.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Core/SettingsWriteFormat.h>
#include <Core/ParallelReplicasMode.h>
#include <base/types.h>
#include <Common/SettingConstraintWritability.h>
#include <Common/SettingsChanges.h>

#include <string_view>
#include <unordered_map>
#include <vector>

namespace boost
{
namespace program_options
{
class options_description;
class variables_map;
}
}

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
class LayeredConfiguration;
}
}

using NameToNameMap = std::unordered_map<std::string, std::string>;

namespace DB
{
class IColumn;
struct MutableColumnsAndConstraints;
class ReadBuffer;
struct SettingsImpl;
class WriteBuffer;


COMMON_SETTINGS_SUPPORTED_TYPES(Settings, DECLARE_SETTING_TRAIT)
struct Settings
{
    Settings();
    Settings(const Settings & settings);
    Settings(Settings && settings) noexcept;
    ~Settings();

    Settings & operator=(const Settings & other);
    bool operator==(const Settings & other) const;

    COMMON_SETTINGS_SUPPORTED_TYPES(Settings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    /// General API as needed
    bool has(std::string_view name) const;
    bool isChanged(std::string_view name) const;

    bool tryGet(std::string_view name, Field & value) const;
    Field get(std::string_view name) const;

    void set(std::string_view name, const Field & value);
    void setDefaultValue(std::string_view name);

    std::vector<String> getHints(const String & name) const;
    String toString() const;

    SettingsChanges changes() const;
    void applyChanges(const SettingsChanges & changes);
    std::vector<std::string_view> getAllRegisteredNames() const;
    std::vector<std::string_view> getChangedAndObsoleteNames() const;
    std::vector<std::string_view> getUnchangedNames() const;

    void dumpToSystemSettingsColumns(MutableColumnsAndConstraints & params) const;
    void dumpToMapColumn(IColumn * column, bool changed_only = true) const;
    NameToNameMap toNameToNameMap() const;

    void write(WriteBuffer & out, SettingsWriteFormat format = SettingsWriteFormat::DEFAULT) const;
    void read(ReadBuffer & in, SettingsWriteFormat format = SettingsWriteFormat::DEFAULT);

    void addToProgramOptions(boost::program_options::options_description & options);
    void addToProgramOptions(std::string_view setting_name, boost::program_options::options_description & options);
    void addToProgramOptionsAsMultitokens(boost::program_options::options_description & options) const;
    void addToClientOptions(
        Poco::Util::LayeredConfiguration & config, const boost::program_options::variables_map & options, bool repeated_settings) const;

    static Field castValueUtil(std::string_view name, const Field & value);
    static String valueToStringUtil(std::string_view name, const Field & value);
    static Field stringToValueUtil(std::string_view name, const String & str);
    static bool hasBuiltin(std::string_view name);
    static std::string_view resolveName(std::string_view name);
    static void checkNoSettingNamesAtTopLevel(const Poco::Util::AbstractConfiguration & config, const String & config_path);

private:
    std::unique_ptr<SettingsImpl> impl;
};
}
