#pragma once
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
class SettingsChanges;

namespace NamedCollectionConfiguration
{

ConfigurationPtr createEmptyConfiguration(const std::string & root_name);

template <typename T> T getConfigValue(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path);

template <typename T> T getConfigValueOrDefault(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const T * default_value = nullptr);

template<typename T> void setConfigValue(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const T & value,
    bool update = false);

template <typename T> void copyConfigValue(
    const Poco::Util::AbstractConfiguration & from_config,
    const std::string & from_path,
    Poco::Util::AbstractConfiguration & to_config,
    const std::string & to_path);

void removeConfigValue(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path);

ConfigurationPtr createConfiguration(const std::string & root_name, const SettingsChanges & settings);

}

}
