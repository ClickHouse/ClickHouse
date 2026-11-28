#include <Access/OPA/OpaConfiguration.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Interpreters/DatabaseCatalog.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

const String CONFIG_SECTION = "open_policy_agent";

Poco::URI parseRequiredURI(const Poco::Util::AbstractConfiguration & config, const String & key)
{
    const String value = config.getString(key, "");
    if (value.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting {} is required in the {} section", backQuote(key), backQuote(CONFIG_SECTION));
    return Poco::URI{value};
}

std::optional<Poco::URI> parseOptionalURI(const Poco::Util::AbstractConfiguration & config, const String & key)
{
    const String value = config.getString(key, "");
    if (value.empty())
        return {};
    return Poco::URI{value};
}

/// Reads a list of `<user>name</user>` entries. An unexpected tag is rejected rather than ignored,
/// so that a typo in a security-relevant list cannot silently widen access.
std::unordered_set<String> parseUserList(const Poco::Util::AbstractConfiguration & config, const String & key)
{
    std::unordered_set<String> result;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(key, keys);

    for (const auto & entry : keys)
    {
        /// Repeated elements are reported as `user`, `user[1]`, `user[2]` and so on.
        std::string_view tag = entry;
        if (const auto bracket_pos = tag.find('['); bracket_pos != std::string_view::npos)
            tag = tag.substr(0, bracket_pos);

        if (tag != "user")
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected element {} in the {} section, only {} elements are allowed",
                backQuote(tag),
                backQuote(key),
                backQuote("user"));
        }

        const String user_name = config.getString(key + "." + entry);
        if (user_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "An empty user name in the {} section", backQuote(key));

        result.insert(user_name);
    }

    return result;
}

size_t parsePositive(const Poco::Util::AbstractConfiguration & config, const String & key, size_t default_value)
{
    const auto value = config.getUInt64(key, default_value);
    if (value == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting {} must be greater than zero", backQuote(key));
    return value;
}

}

bool OpaConfiguration::isConfigured(const Poco::Util::AbstractConfiguration & config)
{
    return config.has(CONFIG_SECTION);
}

OpaConfiguration OpaConfiguration::parse(const Poco::Util::AbstractConfiguration & config)
{
    OpaConfiguration result;

    result.uri = parseRequiredURI(config, CONFIG_SECTION + ".uri");
    result.batch_uri = parseOptionalURI(config, CONFIG_SECTION + ".batch_uri");
    result.row_filters_uri = parseOptionalURI(config, CONFIG_SECTION + ".row_filters_uri");
    result.column_masking_uri = parseOptionalURI(config, CONFIG_SECTION + ".column_masking_uri");

    result.token = config.getString(CONFIG_SECTION + ".token", "");

    result.authoritative = config.getBool(CONFIG_SECTION + ".authoritative", false);
    result.check_system_database = config.getBool(CONFIG_SECTION + ".check_system_database", false);
    result.log_requests = config.getBool(CONFIG_SECTION + ".log_requests", false);
    result.log_responses = config.getBool(CONFIG_SECTION + ".log_responses", false);

    const auto connection_timeout_ms = parsePositive(config, CONFIG_SECTION + ".connection_timeout_ms", 1000);
    const auto receive_timeout_ms = parsePositive(config, CONFIG_SECTION + ".receive_timeout_ms", 2000);
    const auto send_timeout_ms = parsePositive(config, CONFIG_SECTION + ".send_timeout_ms", 2000);
    result.timeouts = ConnectionTimeouts()
        .withConnectionTimeout(Poco::Timespan(connection_timeout_ms * 1000))
        .withReceiveTimeout(Poco::Timespan(receive_timeout_ms * 1000))
        .withSendTimeout(Poco::Timespan(send_timeout_ms * 1000));

    result.max_tries = parsePositive(config, CONFIG_SECTION + ".max_tries", 3);
    result.retry_initial_backoff_ms = parsePositive(config, CONFIG_SECTION + ".retry_initial_backoff_ms", 50);
    result.retry_max_backoff_ms = parsePositive(config, CONFIG_SECTION + ".retry_max_backoff_ms", 1000);
    result.max_batch_size = parsePositive(config, CONFIG_SECTION + ".max_batch_size", 1000);

    result.exempt_users = parseUserList(config, CONFIG_SECTION + ".exempt_users");

    return result;
}

bool OpaConfiguration::isDatabaseInScope(std::string_view database) const
{
    /// A name-less object cannot be described to a policy in the first place. On the initiator of a
    /// cross-replication query the database is not known, and the shards authorize the read again.
    if (database.empty())
        return false;

    /// Access to a temporary table is granted implicitly and is scoped to the session that created
    /// it, so there is nothing for a policy to decide.
    if (database == DatabaseCatalog::TEMPORARY_DATABASE)
        return false;

    if (database == DatabaseCatalog::INFORMATION_SCHEMA || database == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE)
        return false;

    if (database == DatabaseCatalog::SYSTEM_DATABASE)
        return check_system_database;

    return true;
}

bool OpaConfiguration::isUserExempt(const String & user_name) const
{
    return exempt_users.contains(user_name);
}

}
