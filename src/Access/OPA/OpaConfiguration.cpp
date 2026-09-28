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
    result.batch_column_masking_uri = parseOptionalURI(config, CONFIG_SECTION + ".batch_column_masking_uri");

    /// Both endpoints answer the same question, and honouring both would make the effective mask
    /// depend on which one the code happened to consult first.
    if (result.column_masking_uri && result.batch_column_masking_uri)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Settings {} and {} of the {} section are mutually exclusive, specify only one of them",
            backQuote("column_masking_uri"),
            backQuote("batch_column_masking_uri"),
            backQuote(CONFIG_SECTION));
    }

    result.token = config.getString(CONFIG_SECTION + ".token", "");

    result.default_catalog = config.getString(CONFIG_SECTION + ".default_catalog", "clickhouse");
    if (result.default_catalog.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting {} must not be empty", backQuote("default_catalog"));

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
    result.allowed_expression_identities = parseUserList(config, CONFIG_SECTION + ".allowed_expression_identities");

    const String mapping_key = CONFIG_SECTION + ".mapping";
    Poco::Util::AbstractConfiguration::Keys mapping_entries;
    config.keys(mapping_key, mapping_entries);

    for (const auto & entry : mapping_entries)
    {
        std::string_view tag = entry;
        if (const auto bracket_pos = tag.find('['); bracket_pos != std::string_view::npos)
            tag = tag.substr(0, bracket_pos);

        if (tag != "database")
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected element {} in the {} section, only {} elements are allowed",
                backQuote(tag),
                backQuote(mapping_key),
                backQuote("database"));
        }

        const String entry_key = mapping_key + "." + entry;
        const String database = config.getString(entry_key + "[@name]", "");
        if (database.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "An element {} in the {} section requires a non-empty {} attribute",
                backQuote("database"),
                backQuote(mapping_key),
                backQuote("name"));
        }

        DatabaseMapping mapping;
        mapping.catalog = config.getString(entry_key + ".catalog", result.default_catalog);
        if (mapping.catalog.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "An empty catalog for database {} in the {} section", backQuote(database), backQuote(mapping_key));

        mapping.split_dotted_table_name = config.getBool(entry_key + ".split_dotted_table_name", false);

        if (!result.database_mappings.emplace(database, std::move(mapping)).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate mapping for database {} in the {} section", backQuote(database), backQuote(mapping_key));
    }

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

bool OpaConfiguration::isIdentityAllowed(const String & user_name) const
{
    return allowed_expression_identities.empty() || allowed_expression_identities.contains(user_name);
}

OpaTableName OpaConfiguration::mapDatabase(const String & database) const
{
    OpaTableName result;
    result.catalog = default_catalog;
    result.schema = database;

    if (const auto it = database_mappings.find(database); it != database_mappings.end())
        result.catalog = it->second.catalog;

    return result;
}

OpaTableName OpaConfiguration::mapTable(const String & database, const String & table) const
{
    OpaTableName result = mapDatabase(database);
    result.table = table;

    const auto it = database_mappings.find(database);
    if (it == database_mappings.end() || !it->second.split_dotted_table_name)
        return result;

    /// A nested namespace contributes several components, so the table name is what follows the
    /// last separator and the namespace as a whole becomes the schema. A name without a separator
    /// describes a table that is not inside a namespace, and keeps the database as its schema.
    const auto separator_pos = table.rfind('.');
    if (separator_pos != String::npos)
    {
        result.schema = table.substr(0, separator_pos);
        result.table = table.substr(separator_pos + 1);
    }

    return result;
}

}
