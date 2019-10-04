#include <string.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/String.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/HexWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/Users.h>
#include <Common/config.h>
#include <boost/algorithm/hex.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
    extern const int UNKNOWN_USER;
    extern const int REQUIRED_PASSWORD;
    extern const int WRONG_PASSWORD;
    extern const int BAD_ARGUMENTS;
}


User::User(const String & name_, const String & config_elem, const Poco::Util::AbstractConfiguration & config)
    : name(name_)
{
    /// Read password.
    bool has_password = config.has(config_elem + ".password");
    bool has_password_sha256_hex = config.has(config_elem + ".password_sha256_hex");

    if (has_password && has_password_sha256_hex)
        throw Exception("Both fields 'password' and 'password_sha256_hex' are specified for user " + name + ". Must be only one of them.", ErrorCodes::BAD_ARGUMENTS);

    if (!has_password && !has_password_sha256_hex)
        throw Exception("Either 'password' or 'password_sha256_hex' must be specified for user " + name + ".", ErrorCodes::BAD_ARGUMENTS);

    if (has_password)
        password.setPassword(EncryptedPassword::PLAINTEXT, config.getString(config_elem + ".password"));

    if (has_password_sha256_hex)
        password.setHashHex(EncryptedPassword::SHA256, config.getString(config_elem + ".password_sha256_hex"));

    profile = config.getString(config_elem + ".profile");
    quota = config.getString(config_elem + ".quota");

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
            {
                const char * pos = strchr(value.c_str(), '/');
                if (pos)
                {
                    String prefix(value, 0, pos - value.c_str());
                    String mask(value, prefix.length() + 1, value.length() - prefix.length() - 1);
                    if (std::all_of(mask.begin(), mask.end(), isNumericASCII))
                        allowed_hosts.addIPSubnet(Poco::Net::IPAddress(prefix), parse<UInt8>(pos + 1));
                    else
                        allowed_hosts.addIPSubnet(Poco::Net::IPAddress(prefix), Poco::Net::IPAddress(mask));
                }
                else
                    allowed_hosts.addIPAddress(Poco::Net::IPAddress(value));
            }
            else if (startsWith(*it, "host_regexp"))
            {
                allowed_hosts.addHostRegexp(value);
            }
            else if (startsWith(*it, "host"))
            {
                allowed_hosts.addHostName(value);
            }
            else
                throw Exception("Unknown address pattern type: " + *it, ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE);
        }
    }


    /// Fill list of allowed databases.
    const auto config_sub_elem = config_elem + ".allow_databases";
    if (config.has(config_sub_elem))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_sub_elem, config_keys);

        databases.reserve(config_keys.size());
        for (const auto & key : config_keys)
        {
            const auto database_name = config.getString(config_sub_elem + "." + key);
            databases.insert(database_name);
        }
    }

    /// Read properties per "database.table"
    /// Only tables are expected to have properties, so that all the keys inside "database" are table names.
    const auto config_databases = config_elem + ".databases";
    if (config.has(config_databases))
    {
        Poco::Util::AbstractConfiguration::Keys database_names;
        config.keys(config_databases, database_names);

        /// Read tables within databases
        for (const auto & database : database_names)
        {
            const auto config_database = config_databases + "." + database;
            Poco::Util::AbstractConfiguration::Keys table_names;
            config.keys(config_database, table_names);

            /// Read table properties
            for (const auto & table : table_names)
            {
                const auto config_filter = config_database + "." + table + ".filter";
                if (config.has(config_filter))
                {
                    const auto filter_query = config.getString(config_filter);
                    table_props[database][table]["filter"] = filter_query;
                }
            }
        }
    }
}


}
