#include <Storages/BigQuery/BigQueryConfiguration.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Poco/URI.h>
#include <Common/Exception.h>

#include <array>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

constexpr auto USAGE = "bigquery('project', 'dataset', 'table'[, 'access_token'][, key = value, ...]) "
                       "or bigquery(named_collection[, key = value, ...]). "
                       "Allowed keys: project, dataset, table, access_token, service_account_key, "
                       "client_id, client_secret, refresh_token, billing_project, base_url, token_url";

void checkIdentifierPart(const String & value, const char * name)
{
    if (value.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "BigQuery {} must not be empty", name);
    if (value.find_first_of("/?#%") != String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "BigQuery {} '{}' contains invalid characters", name, value);
}

void validate(BigQueryConfiguration & configuration)
{
    checkIdentifierPart(configuration.project, "project");
    checkIdentifierPart(configuration.dataset, "dataset");
    checkIdentifierPart(configuration.table, "table");

    bool has_adc_part = !configuration.client_id.empty() || !configuration.client_secret.empty() || !configuration.refresh_token.empty();
    bool has_adc = !configuration.client_id.empty() && !configuration.client_secret.empty() && !configuration.refresh_token.empty();
    if (has_adc_part && !has_adc)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "BigQuery credentials 'client_id', 'client_secret' and 'refresh_token' must be specified together");

    size_t credential_methods = !configuration.access_token.empty();
    credential_methods += !configuration.service_account_key.empty();
    credential_methods += has_adc;

    if (credential_methods == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "No credentials specified for BigQuery. Provide one of: 'access_token', 'service_account_key' "
            "(the content of a service account key file in JSON format), or 'client_id' + 'client_secret' + 'refresh_token'");
    if (credential_methods > 1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Multiple credential methods specified for BigQuery, provide exactly one of: 'access_token', "
            "'service_account_key', or 'client_id' + 'client_secret' + 'refresh_token'");

    if (!configuration.access_token.empty())
        configuration.credentials_kind = BigQueryConfiguration::CredentialsKind::AccessToken;
    else if (!configuration.service_account_key.empty())
        configuration.credentials_kind = BigQueryConfiguration::CredentialsKind::ServiceAccountKey;
    else
        configuration.credentials_kind = BigQueryConfiguration::CredentialsKind::RefreshToken;

    Poco::URI base_uri;
    try
    {
        base_uri = Poco::URI(configuration.base_url);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid BigQuery base_url '{}': {}", configuration.base_url, e.displayText());
    }
    if ((base_uri.getScheme() != "https" && base_uri.getScheme() != "http") || base_uri.getHost().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "BigQuery base_url '{}' must be an http(s) URL", configuration.base_url);
    /// Normalize: no trailing slash, no path/query.
    if ((!base_uri.getPath().empty() && base_uri.getPath() != "/") || !base_uri.getQuery().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "BigQuery base_url '{}' must not contain a path or query", configuration.base_url);
    base_uri.setPath("");
    configuration.base_url = base_uri.toString();
    if (configuration.base_url.ends_with('/'))
        configuration.base_url.pop_back();
}

using ConfigurationFields = std::unordered_map<std::string_view, String BigQueryConfiguration::*>;

const ConfigurationFields & configurationFields()
{
    static const ConfigurationFields fields =
    {
        {"project", &BigQueryConfiguration::project},
        {"dataset", &BigQueryConfiguration::dataset},
        {"table", &BigQueryConfiguration::table},
        {"access_token", &BigQueryConfiguration::access_token},
        {"service_account_key", &BigQueryConfiguration::service_account_key},
        {"client_id", &BigQueryConfiguration::client_id},
        {"client_secret", &BigQueryConfiguration::client_secret},
        {"refresh_token", &BigQueryConfiguration::refresh_token},
        {"billing_project", &BigQueryConfiguration::billing_project},
        {"base_url", &BigQueryConfiguration::base_url},
        {"token_url", &BigQueryConfiguration::token_url},
    };
    return fields;
}

BigQueryConfiguration fromNamedCollection(const NamedCollection & collection)
{
    BigQueryConfiguration configuration;
    validateNamedCollection(
        collection,
        {"project", "dataset", "table"},
        {"access_token", "service_account_key", "client_id", "client_secret", "refresh_token", "billing_project", "base_url", "token_url"});

    for (const auto & [key, member] : configurationFields())
    {
        String name{key};
        if (collection.has(name))
            configuration.*member = collection.get<String>(name);
    }
    return configuration;
}

}

BigQueryConfiguration BigQueryConfiguration::fromArguments(ASTs & args, ContextPtr context, const StorageID * table_id)
{
    BigQueryConfiguration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context, /*throw_unknown_collection=*/ true, nullptr, table_id))
    {
        configuration = fromNamedCollection(*named_collection);
        /// A collection resolved, so the first argument is the collection name identifier.
        configuration.named_collection_name = args.front()->as<ASTIdentifier &>().name();
    }
    else
    {
        if (args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "BigQuery requires arguments: {}", USAGE);

        const auto & fields = configurationFields();
        std::vector<String> positional;
        std::unordered_set<std::string_view> provided;
        for (auto & arg : args)
        {
            /// Any argument can be given in the key = value form; they can be interleaved
            /// with the positional ones.
            if (const auto * equals_function = arg->as<ASTFunction>(); equals_function && equals_function->name == "equals")
            {
                auto [key, value] = getKeyValueFromAST(arg, context);
                auto it = fields.find(key);
                if (it == fields.end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown BigQuery argument '{}'. {}", key, USAGE);
                if (value.getType() != Field::Types::String)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "BigQuery argument '{}' must be a string literal", key);
                if (!provided.emplace(it->first).second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "BigQuery argument '{}' is specified more than once", key);
                configuration.*(it->second) = value.safeGet<String>();
            }
            else
            {
                if (positional.size() >= 4)
                    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Too many positional arguments for BigQuery: {}", USAGE);
                arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);
                positional.emplace_back(checkAndGetLiteralArgument<String>(arg, "argument"));
            }
        }

        /// Positional arguments fill the 'project', 'dataset', 'table' and 'access_token'
        /// slots in this order; each slot can be filled at most once.
        static constexpr std::array<std::string_view, 4> positional_slots{"project", "dataset", "table", "access_token"};
        for (size_t i = 0; i < positional.size(); ++i)
        {
            if (!provided.emplace(positional_slots[i]).second)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "BigQuery argument '{}' is specified both positionally and in the key = value form", positional_slots[i]);
            configuration.*(fields.at(positional_slots[i])) = positional[i];
        }

        if (!provided.contains("project") || !provided.contains("dataset") || !provided.contains("table"))
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "BigQuery requires the 'project', 'dataset' and 'table' arguments: {}", USAGE);
    }

    validate(configuration);
    return configuration;
}

}
