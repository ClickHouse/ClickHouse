#include <Storages/StorageS3Settings.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

void StorageS3Settings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(mutex);
    settings.clear();
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    for (const String & key : config_keys)
    {
        auto endpoint = config.getString(config_elem + "." + key + ".endpoint");
        auto access_key_id = config.getString(config_elem + "." + key + ".access_key_id", "");
        auto secret_access_key = config.getString(config_elem + "." + key + ".secret_access_key", "");

        HeaderCollection headers;
        Poco::Util::AbstractConfiguration::Keys subconfig_keys;
        config.keys(config_elem + "." + key, subconfig_keys);
        for (const String & subkey : subconfig_keys)
        {
            if (subkey.starts_with("header"))
            {
                auto header_str = config.getString(config_elem + "." + key + "." + subkey);
                auto delimiter = header_str.find(':');
                if (delimiter == String::npos)
                    throw Exception("Malformed s3 header value", ErrorCodes::INVALID_CONFIG_PARAMETER);
                headers.emplace_back(HttpHeader{header_str.substr(0, delimiter), header_str.substr(delimiter + 1, String::npos)});
            }
        }

        settings.emplace(endpoint, S3AuthSettings{std::move(access_key_id), std::move(secret_access_key), std::move(headers)});
    }
}

S3AuthSettings StorageS3Settings::getSettings(const String & endpoint) const
{
    std::lock_guard lock(mutex);
    if (auto setting = settings.find(endpoint); setting != settings.end())
        return setting->second;
    return {};
}

}
