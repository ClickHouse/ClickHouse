#pragma once
#include <queue>
#include <set>
#include <Core/Field.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
class SettingsChanges;

namespace NamedCollectionConfiguration
{

ConfigurationPtr createEmptyConfiguration(const std::string & root_name);

bool hasConfigValue(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path);

template <typename T> T getConfigValue(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path);

template <typename T> T getConfigValueOrDefault(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const T * default_value = nullptr);

template <typename T>
void setConfigValue(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const T & value,
    bool update = false,
    std::optional<bool> is_overridable = {});

template <typename T> void copyConfigValue(
    const Poco::Util::AbstractConfiguration & from_config,
    const std::string & from_path,
    Poco::Util::AbstractConfiguration & to_config,
    const std::string & to_path);

void removeConfigValue(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path);

ConfigurationPtr createConfiguration(
    const std::string & root_name, const SettingsChanges & settings, const std::unordered_map<std::string, bool> & overridability);

/// Enumerate keys paths of the config recursively.
/// E.g. if `enumerate_paths` = {"root.key1"} and config like
/// <root>
///     <key0></key0>
///     <key1>
///         <key2></key2>
///         <key3>
///            <key4></key4>
///         </key3>
///     </key1>
/// </root>
/// the `result` will contain: "root.key0", "root.key1.key2" and "root.key1.key3.key4"
///
/// depth == -1 means to return all keys with full path: "root.key0", "root.key1.key2", "root.key1.key3.key4".
/// depth == 0 means: "root.key0" and "root.key1"
/// depth == 1 means: "root.key0", "root.key1.key2" and "root.key1.key3"
/// and so on.
void listKeys(
    const Poco::Util::AbstractConfiguration & config,
    std::queue<std::string> enumerate_paths,
    std::set<std::string, std::less<>> & result,
    ssize_t depth);

std::optional<bool> isOverridable(const Poco::Util::AbstractConfiguration & config, const std::string & path);
}

}
