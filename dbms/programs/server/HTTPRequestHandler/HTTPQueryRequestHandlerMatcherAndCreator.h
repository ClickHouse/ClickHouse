#pragma once

#include <Common/config.h>
#include <Common/HTMLForm.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryParameterVisitor.h>

#include <re2/re2.h>
#include <re2/stringpiece.h>

#include "ExtractorContextChange.h"
#include "../HTTPHandlerFactory.h"

#if USE_RE2_ST
#include <re2_st/re2.h>
#else
#define re2_st re2
#endif

namespace DB
{

using RegexRule = std::shared_ptr<re2_st::RE2>;
using HeadersRegexRule = std::map<String, RegexRule>;
using ExtractRes = std::vector<std::pair<String, bool>>;

class ExtractorDynamicQueryParameters
{
public:
    ExtractorDynamicQueryParameters(
        Poco::Util::AbstractConfiguration & configuration, const String & key,
        const RegexRule & url_regex_, const HeadersRegexRule & headers_regex_
    );

    bool loadSettingsFromPost() const { return false; }

    ExtractRes extract(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params);

private:
    const RegexRule url_regex;
    const HeadersRegexRule headers_regex;

    String dynamic_param_name;
    std::map<String, int> extract_from_url;
    std::map<String, std::map<String, int>> extract_from_headers;
};

class ExtractorPredefineQueryParameters
{
public:
    ExtractorPredefineQueryParameters(
        Poco::Util::AbstractConfiguration & configuration, const String & key,
        const RegexRule & url_regex_, const HeadersRegexRule & headers_regex_
    );

    bool loadSettingsFromPost() const { return !has_insert_query; }

    ExtractRes extract(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params);

private:
    const RegexRule url_regex;
    const HeadersRegexRule headers_regex;

    NameSet queries_names;
    bool has_insert_query{false};
    ExtractRes predefine_queries;
    std::map<String, int> extract_from_url;
    std::map<String, std::map<String, int>> extract_from_headers;
};

class HTTPQueryRequestHandlerMatcherAndCreator
{
public:
    template <typename NestedFunction>
    static auto invokeWithParsedRegexRule(Poco::Util::AbstractConfiguration & configuration, const String & key, const NestedFunction & fun)
    {
        return fun(configuration.getString(key + ".method", ""), createRegexRule(configuration, key + ".url"),
            createHeadersRegexRule(configuration, key + ".headers"));
    }

    static HTTPHandlerMatcher createHandlerMatcher(const String & method, const RegexRule & url_rule, const HeadersRegexRule & headers_rule);

private:
    static RegexRule createRegexRule(Poco::Util::AbstractConfiguration & configuration, const String & key);

    static HeadersRegexRule createHeadersRegexRule(Poco::Util::AbstractConfiguration & configuration, const String & key);
};

}
