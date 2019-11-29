#include "HTTPQueryRequestHandlerMatcherAndCreator.h"

#include "../HTTPHandlerFactory.h"
#include "HTTPQueryRequestHandler.h"
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_PREDEFINED_QUERY;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int UNKNOWN_QUERY_PARAMETER;
    extern const int DUPLICATE_CAPTURE_QUERY_PARAM;
    extern const int ILLEGAL_HTTP_HANDLER_PARAM_NAME;
    extern const int TOO_MANY_INSERT_QUERY_WITH_PREDEFINED_QUERY;
}

ExtractorDynamicQueryParameters::ExtractorDynamicQueryParameters(
    Poco::Util::AbstractConfiguration & configuration, const String & key, const RegexRule & url_regex_, const HeadersRegexRule & headers_regex_)
    : url_regex(url_regex_), headers_regex(headers_regex_)
{
    dynamic_param_name = configuration.getString(key + ".query_param_name", "query");

    NameSet extracted_names;

    if (url_regex)
    {
        for (const auto & [capturing_name, capturing_index] : url_regex->NamedCapturingGroups())
        {
            if (startsWith(capturing_name, "param_"))
            {
                if (extracted_names.count(capturing_name))
                    throw Exception("Duplicate capture query parameter '" + capturing_name + "'", ErrorCodes::DUPLICATE_CAPTURE_QUERY_PARAM);

                extracted_names.emplace(capturing_name);
                extract_from_url[capturing_name] = capturing_index;
            }
        }
    }

    if (!headers_regex.empty())
    {
        for (const auto & [header_name, header_regex] : headers_regex)
        {
            for (const auto & [capturing_name, capturing_index] : header_regex->NamedCapturingGroups())
            {
                if (startsWith(capturing_name, "param_"))
                {
                    if (extracted_names.count(capturing_name))
                        throw Exception("Duplicate capture query parameter '" + capturing_name + "'", ErrorCodes::DUPLICATE_CAPTURE_QUERY_PARAM);

                    extracted_names.emplace(capturing_name);
                    extract_from_headers[header_name][capturing_name] = capturing_index;
                }
            }
        }
    }
}

template <bool remove_prefix_for_param = false>
void extractParamWithRegex(Context & context, const RegexRule & regex, const std::map<String, int> & extract_params, const String & value)
{
    if (value.empty())
        return;

    int num_captures = regex->NumberOfCapturingGroups() + 1;

    re2_st::StringPiece matches[num_captures];
    re2_st::StringPiece input(value.data(), value.size());

    if (regex->Match(input, 0, value.size(), re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
    {
        for (const auto & [capturing_name, capturing_index] : extract_params)
        {
            const auto & capturing_value = matches[capturing_index];

            if (capturing_value.data())
            {
                String param_name = capturing_name;
                if constexpr (remove_prefix_for_param)
                {
                    const static size_t prefix_size = strlen("param_");
                    param_name = capturing_name.substr(prefix_size);
                }

                context.setQueryParameter(param_name, String(capturing_value.data(), capturing_value.size()));
            }
        }
    }
}

ExtractRes ExtractorDynamicQueryParameters::extract(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params)
{
    if (!extract_from_url.empty())
        extractParamWithRegex<true>(context, url_regex, extract_from_url, Poco::URI{request.getURI()}.getPath());


    if (!extract_from_headers.empty())
        for (const auto & [header_name, extract_params] : extract_from_headers)
            extractParamWithRegex<true>(context, headers_regex.at(header_name), extract_params, request.get(header_name, ""));

    String extracted_query_from_params;
    const static size_t prefix_size = strlen("param_");

    for (const auto & [param_name, param_value] : params)
    {
        if (param_name == dynamic_param_name)
            extracted_query_from_params += param_value;
        else if (startsWith(param_name, "param_"))
            context.setQueryParameter(param_name.substr(prefix_size), param_value);
    }

    if (!extracted_query_from_params.empty())
        extracted_query_from_params += "\n";

    return {{extracted_query_from_params, true}};
}

ExtractorPredefinedQueryParameters::ExtractorPredefinedQueryParameters(
    Poco::Util::AbstractConfiguration & configuration, const String & key, const RegexRule & url_regex_, const HeadersRegexRule & headers_regex_)
    : url_regex(url_regex_), headers_regex(headers_regex_)
{
    Poco::Util::AbstractConfiguration::Keys queries_key;
    configuration.keys(key + ".queries", queries_key);

    if (queries_key.empty())
        throw Exception("There must be at least one predefined query in the predefined HTTPHandler.", ErrorCodes::EMPTY_PREDEFINED_QUERY);

    for (const auto & query_key : queries_key)
    {
        const auto & predefine_query = configuration.getString(key + ".queries." + query_key);

        const char * query_begin = predefine_query.data();
        const char * query_end = predefine_query.data() + predefine_query.size();

        ParserQuery parser(query_end, false);
        ASTPtr extract_query_ast = parseQuery(parser, query_begin, query_end, "", 0);
        QueryParameterVisitor{queries_names}.visit(extract_query_ast);

        bool is_insert_query = extract_query_ast->as<ASTInsertQuery>();

        if (has_insert_query && is_insert_query)
            throw Exception("Too many insert queries in predefined queries.", ErrorCodes::TOO_MANY_INSERT_QUERY_WITH_PREDEFINED_QUERY);

        has_insert_query |= is_insert_query;
        predefine_queries.push_back({predefine_query, is_insert_query});
    }

    const auto & reserved_params_name = ExtractorContextChange::getReservedParamNames();
    for (const auto & predefine_query_name : queries_names)
    {
        if (Settings::findIndex(predefine_query_name) != Settings::npos || reserved_params_name.count(predefine_query_name))
            throw Exception("Illegal http_handler param name '" + predefine_query_name +
                "', Because it's reserved name or Settings name", ErrorCodes::ILLEGAL_HTTP_HANDLER_PARAM_NAME);
    }

    NameSet extracted_names;

    if (url_regex)
    {
        for (const auto & [capturing_name, capturing_index] : url_regex->NamedCapturingGroups())
        {
            if (queries_names.count(capturing_name))
            {
                if (extracted_names.count(capturing_name))
                    throw Exception("Duplicate capture query parameter '" + capturing_name + "'", ErrorCodes::DUPLICATE_CAPTURE_QUERY_PARAM);

                extracted_names.emplace(capturing_name);
                extract_from_url[capturing_name] = capturing_index;
            }
        }
    }

    if (!headers_regex.empty())
    {
        for (const auto & [header_name, header_regex] : headers_regex)
        {
            for (const auto & [capturing_name, capturing_index] : header_regex->NamedCapturingGroups())
            {
                if (queries_names.count(capturing_name))
                {
                    if (extracted_names.count(capturing_name))
                        throw Exception("Duplicate capture query parameter '" + capturing_name + "'", ErrorCodes::DUPLICATE_CAPTURE_QUERY_PARAM);

                    extracted_names.emplace(capturing_name);
                    extract_from_headers[header_name][capturing_name] = capturing_index;
                }
            }
        }
    }
}

ExtractRes ExtractorPredefinedQueryParameters::extract(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params)
{
    if (!extract_from_url.empty())
        extractParamWithRegex<false>(context, url_regex, extract_from_url, Poco::URI{request.getURI()}.getPath());

    if (!extract_from_headers.empty())
        for (const auto & [header_name, extract_params] : extract_from_headers)
            extractParamWithRegex<false>(context, headers_regex.at(header_name), extract_params, request.get(header_name, ""));

    for (const auto & param : params)
        if (queries_names.count(param.first))
            context.setQueryParameter(param.first, param.second);

    return predefine_queries;
}

RegexRule HTTPQueryRequestHandlerMatcherAndCreator::createRegexRule(Poco::Util::AbstractConfiguration & configuration, const String & key)
{
    if (!configuration.has(key))
        return {};

    const auto & regex_str = configuration.getString(key);
    const auto & url_regex_rule = std::make_shared<re2_st::RE2>(regex_str);

    if (!url_regex_rule->ok())
        throw Exception("cannot compile re2: " + regex_str + " for HTTPHandler url, error: " + url_regex_rule->error() +
            ". Look at https://github.com/google/re2/wiki/Syntax for reference.", ErrorCodes::CANNOT_COMPILE_REGEXP);

    return url_regex_rule;
}

HeadersRegexRule HTTPQueryRequestHandlerMatcherAndCreator::createHeadersRegexRule(Poco::Util::AbstractConfiguration & configuration, const String & key)
{
    if (!configuration.has(key))
        return {};

    Poco::Util::AbstractConfiguration::Keys headers_names;
    configuration.keys(key, headers_names);

    HeadersRegexRule headers_regex_rule;
    for (const auto & header_name : headers_names)
    {
        if (headers_regex_rule.count(header_name))
            throw Exception("Duplicate header match declaration '" + header_name + "'", ErrorCodes::LOGICAL_ERROR);

        headers_regex_rule[header_name] = createRegexRule(configuration, key + "." + header_name);
    }

    return headers_regex_rule;
}

size_t findFirstMissingMatchPos(const re2_st::RE2 & regex_rule, const String & match_content)
{
    int num_captures = regex_rule.NumberOfCapturingGroups() + 1;

    re2_st::StringPiece matches[num_captures];
    re2_st::StringPiece input(match_content.data(), match_content.size());
    if (regex_rule.Match(input, 0, match_content.size(), re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
        return matches[0].size();

    return size_t(0);
}

HTTPHandlerMatcher HTTPQueryRequestHandlerMatcherAndCreator::createHandlerMatcher(
    const String & method, const RegexRule & url_rule, const HeadersRegexRule & headers_rule)
{
    return [method = Poco::toLower(method), url_rule = url_rule, headers_rule = headers_rule](const Poco::Net::HTTPServerRequest & request)
    {
        if (!method.empty() && Poco::toLower(request.getMethod()) != method)
            return false;

        if (url_rule)
        {
            Poco::URI uri(request.getURI());
            const auto & request_uri = uri.getPath();
            size_t first_missing_pos = findFirstMissingMatchPos(*url_rule, request_uri);

            const char * url_end = request_uri.data() + request_uri.size();
            const char * first_missing = request_uri.data() + first_missing_pos;

            if (first_missing != url_end && *first_missing == '/')
                ++first_missing;

            if (first_missing != url_end && *first_missing != '?')
                return false;   /// Not full matched
        }

        if (!headers_rule.empty())
        {
            for (const auto & [header_name, header_rule] : headers_rule)
            {
                if (!request.has(header_name))
                    return false;

                const String & header_value = request.get(header_name);
                if (header_value.size() != findFirstMissingMatchPos(*header_rule, header_value))
                    return false;
            }
        }

        return true;
    };
}

HTTPHandlerMatcher createDynamicQueryHandlerMatcher(IServer & server, const String & key)
{
    return HTTPQueryRequestHandlerMatcherAndCreator::invokeWithParsedRegexRule(server.config(), key,
        HTTPQueryRequestHandlerMatcherAndCreator::createHandlerMatcher);
}


HTTPHandlerMatcher createPredefinedQueryHandlerMatcher(IServer & server, const String & key)
{
    return HTTPQueryRequestHandlerMatcherAndCreator::invokeWithParsedRegexRule(server.config(), key,
        HTTPQueryRequestHandlerMatcherAndCreator::createHandlerMatcher);
}

HTTPHandlerCreator createDynamicQueryHandlerCreator(IServer & server, const String & key)
{
    return HTTPQueryRequestHandlerMatcherAndCreator::invokeWithParsedRegexRule(
        server.config(), key, [&](const String &, const RegexRule & url_rule, const HeadersRegexRule & headers_rule)
        {
            const auto & extract = std::make_shared<ExtractorDynamicQueryParameters>(server.config(), key, url_rule, headers_rule);

            return [&, query_extract = extract]()
            {
                return new HTTPQueryRequestHandler<ExtractorDynamicQueryParameters>(server, *query_extract);
            };
        });
}

HTTPHandlerCreator createPredefinedQueryHandlerCreator(IServer & server, const String & key)
{
    return HTTPQueryRequestHandlerMatcherAndCreator::invokeWithParsedRegexRule(
        server.config(), key, [&](const String &, const RegexRule & url_rule, const HeadersRegexRule & headers_rule)
        {
            const auto & extract = std::make_shared<ExtractorPredefinedQueryParameters>(server.config(), key, url_rule, headers_rule);

            return [&, query_extract = extract]()
            {
                return new HTTPQueryRequestHandler<ExtractorPredefinedQueryParameters>(server, *query_extract);
            };
        });
}
}
