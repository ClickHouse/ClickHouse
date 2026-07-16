#include <IO/S3Settings.h>

#include <Core/Settings.h>
#include <IO/S3Common.h>
#include <Interpreters/Context.h>

#include <Common/ProxyConfigurationResolverProvider.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool s3_validate_request_settings;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsBool read_only;
    extern const S3RequestSettingsUInt64 min_bytes_for_seek;
    extern const S3RequestSettingsUInt64 list_object_keys_size;
    extern const S3RequestSettingsUInt64 objects_chunk_size_to_delete;
}


void S3Settings::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DB::Settings & settings)
{
    auth_settings = S3::S3AuthSettings(config, settings, config_prefix);
    request_settings = S3::S3RequestSettings(config, settings, config_prefix);
}

void S3Settings::loadFromConfigForObjectStorage(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DB::Settings & settings,
    const std::string & scheme,
    bool validate_settings)
{
    auth_settings = S3::S3AuthSettings(config, settings, config_prefix);
    request_settings = S3::S3RequestSettings(config, settings, config_prefix, "s3_", validate_settings);

    request_settings.proxy_resolver = DB::ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
        ProxyConfiguration::protocolFromString(scheme), config_prefix, config);

    /// We override these request settings from configuration, because they are related to disk configuration,
    /// which shouldn't be changed from user query
    request_settings[S3RequestSetting::read_only] = config.getBool(config_prefix + ".readonly", false);
    request_settings[S3RequestSetting::min_bytes_for_seek] = config.getUInt64(config_prefix + ".min_bytes_for_seek", S3::DEFAULT_MIN_BYTES_FOR_SEEK);
    request_settings[S3RequestSetting::list_object_keys_size] = config.getUInt64(config_prefix + ".list_object_keys_size", S3::DEFAULT_LIST_OBJECT_KEYS_SIZE);
    request_settings[S3RequestSetting::objects_chunk_size_to_delete] = config.getUInt(config_prefix + ".objects_chunk_size_to_delete", S3::DEFAULT_OBJECTS_CHUNK_SIZE_TO_DELETE);
}


void S3Settings::updateIfChanged(const S3Settings & settings)
{
    auth_settings.updateIfChanged(settings.auth_settings);
    request_settings.updateIfChanged(settings.request_settings);
}

void S3SettingsByEndpoint::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DB::Settings & settings)
{
    std::lock_guard lock(mutex);
    s3_settings.clear();
    if (!config.has(config_prefix))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);
    auto default_auth_settings = S3::S3AuthSettings(config, settings, config_prefix);
    auto default_request_settings = S3::S3RequestSettings(config, settings, config_prefix);

    for (const String & key : config_keys)
    {
        const auto key_path = config_prefix + "." + key;
        const auto endpoint_path = key_path + ".endpoint";
        if (config.has(endpoint_path))
        {
            auto auth_settings{default_auth_settings};
            auth_settings.updateIfChanged(S3::S3AuthSettings(config, settings, key_path));

            auto request_settings{default_request_settings};
            request_settings.updateIfChanged(S3::S3RequestSettings(config, settings, key_path, "", settings[Setting::s3_validate_request_settings]));

            s3_settings.emplace(
                config.getString(endpoint_path),
                S3Settings{std::move(auth_settings), std::move(request_settings)});
        }
    }
}

std::optional<S3Settings> S3SettingsByEndpoint::getSettings(
    const String & endpoint,
    const String & user,
    bool ignore_user) const
{
    std::lock_guard lock(mutex);
    auto next_prefix_setting = s3_settings.upper_bound(endpoint);

    /// Linear time algorithm may be replaced with logarithmic with prefix tree map.
    for (auto possible_prefix_setting = next_prefix_setting; possible_prefix_setting != s3_settings.begin();)
    {
        std::advance(possible_prefix_setting, -1);
        const auto & [endpoint_prefix, settings] = *possible_prefix_setting;
        if (endpoint.starts_with(endpoint_prefix) && (ignore_user || settings.auth_settings.canBeUsedByUser(user)))
            return possible_prefix_setting->second;
    }

    return {};
}

void S3Settings::serialize(WriteBuffer & os, ContextPtr context) const
{
    auth_settings.serialize(os, context);
    request_settings.serialize(os, context);
}

S3Settings S3Settings::deserialize(ReadBuffer & is, ContextPtr context)
{
    S3Settings result;
    result.auth_settings = S3::S3AuthSettings::deserialize(is, context);
    result.request_settings = S3::S3RequestSettings::deserialize(is, context);
    return result;
}

}
