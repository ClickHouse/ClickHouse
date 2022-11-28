#include <Storages/StorageS3Settings.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>

#include <boost/algorithm/string/predicate.hpp>


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
        if (config.has(config_elem + "." + key + ".endpoint"))
        {
            auto endpoint = config.getString(config_elem + "." + key + ".endpoint");
            auto access_key_id = config.getString(config_elem + "." + key + ".access_key_id", "");
            auto secret_access_key = config.getString(config_elem + "." + key + ".secret_access_key", "");
            auto region = config.getString(config_elem + "." + key + ".region", "");
            auto server_side_encryption_customer_key_base64 = config.getString(config_elem + "." + key + ".server_side_encryption_customer_key_base64", "");
            std::optional<bool> use_environment_credentials;
            if (config.has(config_elem + "." + key + ".use_environment_credentials"))
            {
                use_environment_credentials = config.getBool(config_elem + "." + key + ".use_environment_credentials");
            }
            std::optional<bool> use_insecure_imds_request;
            if (config.has(config_elem + "." + key + ".use_insecure_imds_request"))
            {
                use_insecure_imds_request = config.getBool(config_elem + "." + key + ".use_insecure_imds_request");
            }

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

            settings.emplace(endpoint, S3AuthSettings{
                    std::move(access_key_id), std::move(secret_access_key),
                    std::move(region),
                    std::move(server_side_encryption_customer_key_base64),
                    std::move(headers),
                    use_environment_credentials,
                    use_insecure_imds_request
                });
        }
    }
}

S3AuthSettings StorageS3Settings::getSettings(const String & endpoint) const
{
    std::lock_guard lock(mutex);
    auto next_prefix_setting = settings.upper_bound(endpoint);

    /// Linear time algorithm may be replaced with logarithmic with prefix tree map.
    for (auto possible_prefix_setting = next_prefix_setting; possible_prefix_setting != settings.begin();)
    {
        std::advance(possible_prefix_setting, -1);
        if (boost::algorithm::starts_with(endpoint, possible_prefix_setting->first))
            return possible_prefix_setting->second;
    }

    return {};
}

}
