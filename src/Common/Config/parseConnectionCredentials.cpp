#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Config/parseConnectionCredentials.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

ConnectionsCredentials parseConnectionsCredentials(const Poco::Util::AbstractConfiguration & config, const std::string & host, const std::optional<std::string> & connection_name)
{
    ConnectionsCredentials res;

    std::string connection_or_host = connection_name.value_or(host);

    std::vector<std::string> keys;
    config.keys("connections_credentials", keys);
    bool connection_found = false;
    for (const auto & key : keys)
    {
        const std::string & prefix = "connections_credentials." + key;

        const std::string & name = config.getString(prefix + ".name", "");
        if (name != connection_or_host)
            continue;
        connection_found = true;

        if (config.has(prefix + ".hostname"))
            res.hostname.emplace(config.getString(prefix + ".hostname"));
        else
            res.hostname.emplace(name);

        if (config.has(prefix + ".port"))
            res.port.emplace(config.getInt(prefix + ".port"));
        if (config.has(prefix + ".secure"))
        {
            bool secure = config.getBool(prefix + ".secure");
            res.secure.emplace(secure);
        }
        if (config.has(prefix + ".user"))
            res.user.emplace(config.getString(prefix + ".user"));
        if (config.has(prefix + ".password"))
            res.password.emplace(config.getString(prefix + ".password"));
        if (config.has(prefix + ".database"))
            res.database.emplace(config.getString(prefix + ".database"));
        if (config.has(prefix + ".history_file"))
            res.history_file.emplace(config.getString(prefix + ".history_file"));
        if (config.has(prefix + ".history_max_entries"))
            res.history_max_entries.emplace(config.getUInt(prefix + ".history_max_entries"));
        if (config.has(prefix + ".accept-invalid-certificate"))
            res.accept_invalid_certificate.emplace(config.getBool(prefix + ".accept-invalid-certificate"));
        if (config.has(prefix + ".prompt"))
            res.prompt.emplace(config.getString(prefix + ".prompt"));
    }

    if (connection_name.has_value() && !connection_found)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No such connection '{}' in connections_credentials", connection_name.value());

    return res;
}

}
