#include <string.h>
#include <Poco/RegularExpression.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Users.h>
#include <common/logger_useful.h>
#include <Poco/MD5Engine.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
    extern const int UNKNOWN_USER;
    extern const int BAD_ARGUMENTS;
}


User::User(const String & name_, const String & config_elem, const Poco::Util::AbstractConfiguration & config)
    : name(name_)
{
    bool has_password = config.has(config_elem + ".password");
    bool has_password_sha256_hex = config.has(config_elem + ".password_sha256_hex");
    bool has_password_double_sha1_hex = config.has(config_elem + ".password_double_sha1_hex");

    if (has_password + has_password_sha256_hex + has_password_double_sha1_hex > 1)
        throw Exception("More than one field of 'password', 'password_sha256_hex', 'password_double_sha1_hex' is used to specify password for user " + name + ". Must be only one of them.",
            ErrorCodes::BAD_ARGUMENTS);

    if (!has_password && !has_password_sha256_hex && !has_password_double_sha1_hex)
        throw Exception("Either 'password' or 'password_sha256_hex' or 'password_double_sha1_hex' must be specified for user " + name + ".", ErrorCodes::BAD_ARGUMENTS);

    if (has_password)
    {
        authentication = Authentication{Authentication::PLAINTEXT_PASSWORD};
        authentication.setPassword(config.getString(config_elem + ".password"));
    }
    else if (has_password_sha256_hex)
    {
        authentication = Authentication{Authentication::SHA256_PASSWORD};
        authentication.setPasswordHashHex(config.getString(config_elem + ".password_sha256_hex"));
    }
    else if (has_password_double_sha1_hex)
    {
        authentication = Authentication{Authentication::DOUBLE_SHA1_PASSWORD};
        authentication.setPasswordHashHex(config.getString(config_elem + ".password_double_sha1_hex"));
    }

    profile = config.getString(config_elem + ".profile");

    /// Fill list of allowed hosts.
    const auto config_networks = config_elem + ".networks";
    if (config.has(config_networks))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_networks, config_keys);
        for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
        {
            String value = config.getString(config_networks + "." + *it);
            if (startsWith(*it, "ip"))
                allowed_client_hosts.addSubnet(value);
            else if (startsWith(*it, "host_regexp"))
                allowed_client_hosts.addHostRegexp(value);
            else if (startsWith(*it, "host"))
                allowed_client_hosts.addHostName(value);
            else
                throw Exception("Unknown address pattern type: " + *it, ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE);
        }
    }

    /// Fill list of allowed databases.
    const auto config_sub_elem = config_elem + ".allow_databases";
    if (config.has(config_sub_elem))
    {
        databases = DatabaseSet();
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_sub_elem, config_keys);

        databases->reserve(config_keys.size());
        for (const auto & key : config_keys)
        {
            const auto database_name = config.getString(config_sub_elem + "." + key);
            databases->insert(database_name);
        }
    }

    /// Fill list of allowed dictionaries.
    const auto config_dictionary_sub_elem = config_elem + ".allow_dictionaries";
    if (config.has(config_dictionary_sub_elem))
    {
        dictionaries = DictionarySet();
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_dictionary_sub_elem, config_keys);

        dictionaries->reserve(config_keys.size());
        for (const auto & key : config_keys)
        {
            const auto dictionary_name = config.getString(config_dictionary_sub_elem + "." + key);
            dictionaries->insert(dictionary_name);
        }
    }

    if (config.has(config_elem + ".allow_quota_management"))
        is_quota_management_allowed = config.getBool(config_elem + ".allow_quota_management");
    if (config.has(config_elem + ".allow_row_policy_management"))
        is_row_policy_management_allowed = config.getBool(config_elem + ".allow_row_policy_management");
}

}
