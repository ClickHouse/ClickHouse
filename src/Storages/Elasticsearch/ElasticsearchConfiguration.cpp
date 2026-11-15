#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>

#include <Interpreters/Context.h>
#include <Interpreters/FunctionSecretArgumentsFinder.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/TableNameOrQuery.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <Common/HTTPHeaderFilter.h>
#include <Common/StringUtils.h>

#include <algorithm>
#include <array>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

using ConfigurationFields = std::unordered_map<std::string_view, String ElasticsearchConfiguration::*>;

namespace
{

constexpr auto USAGE = "Elasticsearch('url', 'index'[, 'user', 'password'][, query('...')][, key = value, ...]) "
                        "or Elasticsearch(named_collection[, query('...')][, key = value, ...]). "
                        "Allowed keys: url, index, user, password, bearer_token, api_key; a named collection also accepts query";

const ConfigurationFields & configurationFields()
{
    static const ConfigurationFields fields =
    {
        {"url", &ElasticsearchConfiguration::url},
        {"index", &ElasticsearchConfiguration::index},
        {"user", &ElasticsearchConfiguration::user},
        {"password", &ElasticsearchConfiguration::password},
        {"bearer_token", &ElasticsearchConfiguration::bearer_token},
        {"api_key", &ElasticsearchConfiguration::api_key},
    };
    return fields;
}

ElasticsearchConfiguration fromNamedCollection(const NamedCollection & collection)
{
    ElasticsearchConfiguration configuration;
    validateNamedCollection(
        collection,
        {"url", "index"},
        {"user", "password", "bearer_token", "api_key", "query"});

    for (const auto & [key, member] : configurationFields())
    {
        String name{key};
        if (collection.has(name))
            configuration.*member = collection.get<String>(name);
    }
    if (collection.has("query"))
        configuration.query = collection.get<String>("query");
    return configuration;
}

void validate(ElasticsearchConfiguration & configuration, const ContextPtr & context)
{
    Poco::URI uri;
    try
    {
        uri = Poco::URI(configuration.url);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Elasticsearch url '{}': {}", configuration.url, e.displayText());
    }
    if ((uri.getScheme() != "http" && uri.getScheme() != "https") || uri.getHost().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch url '{}' must be an http(s) URL with a host", configuration.url);
    if (!uri.getUserInfo().empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Elasticsearch url must not contain credentials, use the 'user' and 'password' arguments instead");
    if (!uri.getQuery().empty() || !uri.getFragment().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch url '{}' must not contain a query or a fragment", configuration.url);
    configuration.url = uri.toString();
    while (configuration.url.ends_with('/'))
        configuration.url.pop_back();

    const auto & index = configuration.index;
    if (index.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch index must not be empty");
    if (std::ranges::any_of(index, isWhitespaceASCII))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch index '{}' must not contain whitespace", index);
    if (index.starts_with('_') && index != "_all")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch index '{}' must not start with '_'", index);

    const auto & user = configuration.user;
    if (user.empty() && !configuration.password.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch argument 'password' requires 'user'");
    if (!user.empty())
    {
        if (user.contains(':'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch user '{}' must not contain ':'", user);
        if (isWhitespaceASCII(user.front()) || isWhitespaceASCII(user.back()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch user '{}' must not have leading or trailing whitespace", user);
    }

    size_t auth_methods = !user.empty();
    auth_methods += !configuration.bearer_token.empty();
    auth_methods += !configuration.api_key.empty();
    if (auth_methods > 1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Multiple authentication methods specified for Elasticsearch, provide at most one of: "
            "'user' + 'password', 'bearer_token', or 'api_key'");

    using AuthKind = ElasticsearchConfiguration::AuthKind;
    if (!user.empty())
        configuration.auth_kind = AuthKind::Basic;
    else if (!configuration.bearer_token.empty())
        configuration.auth_kind = AuthKind::Bearer;
    else if (!configuration.api_key.empty())
        configuration.auth_kind = AuthKind::ApiKey;
    else
        configuration.auth_kind = AuthKind::None;

    auto auth_headers = configuration.authorizationHeaders();
    if (!auth_headers.empty())
        context->getGlobalContext()->getHTTPHeaderFilter().checkHeaders(auth_headers);

    Poco::JSON::Object::Ptr parsed_query;
    try
    {
        parsed_query = Poco::JSON::Parser().parse(configuration.query).extract<Poco::JSON::Object::Ptr>();
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Elasticsearch query: {}", e.displayText());
    }
    if (!parsed_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch query must be a JSON object");
}

}

ElasticsearchConfiguration ElasticsearchConfiguration::fromArguments(ASTs & args, ContextPtr context, const StorageID * table_id)
{
    ElasticsearchConfiguration configuration;

    if (!args.empty() && args.front()->as<ASTIdentifier>())
    {
        std::unordered_set<String> override_keys;
        for (auto it = std::next(args.begin()); it != args.end(); ++it)
        {
            const auto * equals_function = (*it)->as<ASTFunction>();
            String key;
            if (equals_function && equals_function->name == "query")
            {
                key = "query";
            }
            else
            {
                if (!equals_function || equals_function->name != "equals" || !equals_function->arguments
                    || equals_function->arguments->children.size() != 2)
                    continue;
                auto literal_key = evaluateConstantExpressionOrIdentifierAsLiteral(equals_function->arguments->children[0], context);
                key = checkAndGetLiteralArgument<String>(literal_key, "key");
            }
            if (!override_keys.emplace(key).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch argument '{}' is specified more than once", key);
        }
    }

    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context, true, nullptr, table_id))
    {
        configuration = fromNamedCollection(*named_collection);
        configuration.named_collection_name = args.front()->as<ASTIdentifier &>().name();
        for (auto it = std::next(args.begin()); it != args.end(); ++it)
        {
            if (auto dsl_query = tryGetQueryFunctionArgument(*it, context))
            {
                *it = makeASTFunction("query", make_intrusive<ASTLiteral>(*dsl_query));
                configuration.query = std::move(*dsl_query);
            }
        }
    }
    else
    {
        if (args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Elasticsearch requires arguments: {}", USAGE);

        const auto & fields = configurationFields();
        std::vector<String> positional;
        std::unordered_set<std::string_view> provided;
        for (auto & arg : args)
        {
            if (auto dsl_query = tryGetQueryFunctionArgument(arg, context))
            {
                if (!provided.emplace("query").second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch argument 'query' is specified more than once");
                arg = makeASTFunction("query", make_intrusive<ASTLiteral>(*dsl_query));
                configuration.query = std::move(*dsl_query);
            }
            else if (const auto * equals_function = arg->as<ASTFunction>(); equals_function && equals_function->name == "equals")
            {
                auto [key, value] = getKeyValueFromAST(arg, context);
                auto it = fields.find(key);
                if (it == fields.end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Elasticsearch argument '{}'. {}", key, USAGE);
                if (value.getType() != Field::Types::String)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch argument '{}' must be a string literal", key);
                if (!provided.emplace(it->first).second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch argument '{}' is specified more than once", key);
                configuration.*(it->second) = value.safeGet<String>();
            }
            else
            {
                if (provided.contains("query"))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "Positional arguments for Elasticsearch must precede query('...'): {}", USAGE);
                if (positional.size() >= 4)
                    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Too many positional arguments for Elasticsearch: {}", USAGE);
                arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);
                positional.emplace_back(checkAndGetLiteralArgument<String>(arg, "argument"));
            }
        }

        static constexpr std::array<std::string_view, 4> positional_slots{"url", "index", "user", "password"};
        for (size_t i = 0; i < positional.size(); ++i)
        {
            if (!provided.emplace(positional_slots[i]).second)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Elasticsearch argument '{}' is specified both positionally and in the key = value form", positional_slots[i]);
            configuration.*(fields.at(positional_slots[i])) = positional[i];
        }

        if (!provided.contains("url") || !provided.contains("index"))
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Elasticsearch requires the 'url' and 'index' arguments: {}", USAGE);
    }

    validate(configuration, context);

    return configuration;
}

SecretArgumentsSpec elasticsearchSecretArguments()
{
    return {.positional_secret_slots = {3}, .secret_keys = {"password", "bearer_token", "api_key"}, .custom = [](FunctionSecretArgumentsFinder & finder)
    {
        /// `query(...)` is counted as a positional argument, so a positional argument after it is not in its slot.
        const auto positional = finder.classifyPositionalArguments();
        auto query_it = std::ranges::find_if(positional, [&finder](size_t index)
        {
            const auto function = finder.function->arguments->at(index)->getFunction();
            return function && function->name() == "query";
        });
        if (query_it == positional.end())
            return;
        for (auto it = std::next(query_it); it != positional.end(); ++it)
            finder.markSecretArgument(*it);
    }};
}

HTTPHeaderEntries ElasticsearchConfiguration::authorizationHeaders() const
{
    switch (auth_kind)
    {
        case AuthKind::Bearer:
            return {{"Authorization", "Bearer " + bearer_token}};
        case AuthKind::ApiKey:
            return {{"Authorization", "ApiKey " + api_key}};
        case AuthKind::Basic:
        case AuthKind::None:
            return {};
    }
}

}
