#include <Storages/StorageS3Settings.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>

#include <boost/algorithm/string/predicate.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

void StorageS3Settings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config, const Settings & settings)
{
    std::lock_guard lock(mutex);
    s3_settings.clear();
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    auto get_string_for_key = [&](const String & key, const String & elem, bool with_default = true, const String & default_value = "")
    {
        return with_default ? config.getString(config_elem + "." + key + "." + elem, default_value) : config.getString(config_elem + "." + key + "." + elem);
    };

    auto get_uint_for_key = [&](const String & key, const String & elem, bool with_default = true, UInt64 default_value = 0)
    {
        return with_default ? config.getUInt64(config_elem + "." + key + "." + elem, default_value) : config.getUInt64(config_elem + "." + key + "." + elem);
    };

    for (const String & key : config_keys)
    {
        if (config.has(config_elem + "." + key + ".endpoint"))
        {
            auto endpoint = get_string_for_key(key, "endpoint", false);
            auto access_key_id = get_string_for_key(key, "access_key_id");
            auto secret_access_key = get_string_for_key(key, "secret_access_key");
            auto region = get_string_for_key(key, "region");
            auto server_side_encryption_customer_key_base64 = get_string_for_key(key, "server_side_encryption_customer_key_base64");

            std::optional<bool> use_environment_credentials;
            if (config.has(config_elem + "." + key + ".use_environment_credentials"))
                use_environment_credentials = config.getBool(config_elem + "." + key + ".use_environment_credentials");

            std::optional<bool> use_insecure_imds_request;
            if (config.has(config_elem + "." + key + ".use_insecure_imds_request"))
                use_insecure_imds_request = config.getBool(config_elem + "." + key + ".use_insecure_imds_request");

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

            S3Settings::AuthSettings auth_settings{
                    std::move(access_key_id), std::move(secret_access_key),
                    std::move(region),
                    std::move(server_side_encryption_customer_key_base64),
                    std::move(headers),
                    use_environment_credentials,
                    use_insecure_imds_request};

            S3Settings::ReadWriteSettings rw_settings;
            rw_settings.max_single_read_retries = get_uint_for_key(key, "max_single_read_retries", true, settings.s3_max_single_read_retries);
            rw_settings.min_upload_part_size = get_uint_for_key(key, "min_upload_part_size", true, settings.s3_min_upload_part_size);
            rw_settings.upload_part_size_multiply_factor = get_uint_for_key(key, "upload_part_size_multiply_factor", true, settings.s3_upload_part_size_multiply_factor);
            rw_settings.upload_part_size_multiply_parts_count_threshold = get_uint_for_key(key, "upload_part_size_multiply_parts_count_threshold", true, settings.s3_upload_part_size_multiply_parts_count_threshold);
            rw_settings.max_single_part_upload_size = get_uint_for_key(key, "max_single_part_upload_size", true, settings.s3_max_single_part_upload_size);
            rw_settings.max_connections = get_uint_for_key(key, "max_connections", true, settings.s3_max_connections);

            s3_settings.emplace(endpoint, S3Settings{std::move(auth_settings), std::move(rw_settings)});
        }
    }
}

S3Settings StorageS3Settings::getSettings(const String & endpoint) const
{
    std::lock_guard lock(mutex);
    auto next_prefix_setting = s3_settings.upper_bound(endpoint);

    /// Linear time algorithm may be replaced with logarithmic with prefix tree map.
    for (auto possible_prefix_setting = next_prefix_setting; possible_prefix_setting != s3_settings.begin();)
    {
        std::advance(possible_prefix_setting, -1);
        if (boost::algorithm::starts_with(endpoint, possible_prefix_setting->first))
            return possible_prefix_setting->second;
    }

    return {};
}

S3Settings::ReadWriteSettings::ReadWriteSettings(const Settings & settings)
{
    max_single_read_retries = settings.s3_max_single_read_retries;
    min_upload_part_size = settings.s3_min_upload_part_size;
    upload_part_size_multiply_factor = settings.s3_upload_part_size_multiply_factor;
    upload_part_size_multiply_parts_count_threshold = settings.s3_upload_part_size_multiply_parts_count_threshold;
    max_single_part_upload_size = settings.s3_max_single_part_upload_size;
    max_connections = settings.s3_max_connections;
}

void S3Settings::ReadWriteSettings::updateFromSettingsIfEmpty(const Settings & settings)
{
    if (!max_single_read_retries)
        max_single_read_retries = settings.s3_max_single_read_retries;
    if (!min_upload_part_size)
        min_upload_part_size = settings.s3_min_upload_part_size;
    if (!upload_part_size_multiply_factor)
        upload_part_size_multiply_factor = settings.s3_upload_part_size_multiply_factor;
    if (!upload_part_size_multiply_parts_count_threshold)
        upload_part_size_multiply_parts_count_threshold = settings.s3_upload_part_size_multiply_parts_count_threshold;
    if (!max_single_part_upload_size)
        max_single_part_upload_size = settings.s3_max_single_part_upload_size;
    if (!max_connections)
        max_connections = settings.s3_max_connections;
}

}
