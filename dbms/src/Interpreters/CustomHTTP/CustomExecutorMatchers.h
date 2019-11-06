#pragma once

#include <Core/Types.h>
#include <Common/config.h>
#include <Common/HTMLForm.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomHTTP/CustomQueryExecutors.h>
#include <Poco/Net/HTTPServerRequest.h>


#if USE_RE2_ST
#    include <re2_st/re2.h>
#else
#    define re2_st re2
#endif

namespace DB
{

class CustomExecutorMatcher
{
public:
    virtual ~CustomExecutorMatcher() = default;

    virtual bool checkQueryExecutor(const std::vector<CustomQueryExecutorPtr> & check_executors) const = 0;

    virtual bool match(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params) const = 0;
};

using CustomExecutorMatcherPtr = std::shared_ptr<CustomExecutorMatcher>;


class AlwaysMatchedCustomExecutorMatcher : public CustomExecutorMatcher
{
public:
    bool checkQueryExecutor(const std::vector<CustomQueryExecutorPtr> & /*check_executors*/) const override { return true; }

    bool match(Context & /*context*/, Poco::Net::HTTPServerRequest & /*request*/, HTMLForm & /*params*/) const override { return true; }
};

class HTTPURLCustomExecutorMatcher : public CustomExecutorMatcher
{
public:
    HTTPURLCustomExecutorMatcher(const Poco::Util::AbstractConfiguration & configuration, const String & url_config_key)
        : url_match_searcher(analyzeURLPatten(configuration.getString(url_config_key, ""), params_name_extract_from_url))
    {
    }

    bool checkQueryExecutor(const std::vector<CustomQueryExecutorPtr> & custom_query_executors) const override
    {
        for (const auto & param_name_from_url : params_name_extract_from_url)
        {
            bool found_param_name = false;
            for (const auto & custom_query_executor : custom_query_executors)
            {
                if (custom_query_executor->isQueryParam(param_name_from_url))
                {
                    found_param_name = true;
                    break;
                }
            }

            if (!found_param_name)
                throw Exception("The param name '" + param_name_from_url + "' is uselessed.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        }

        return true;
    }

    bool match(Context & /*context*/, Poco::Net::HTTPServerRequest & request, HTMLForm & /*params*/) const override
    {
        const String request_uri = request.getURI();
        re2_st::StringPiece query_params_matches[params_name_extract_from_url.size()];

//        re2_st::StringPiece input;
//        if (url_match_searcher.Match(input, start_pos, input.length(), re2_st::RE2::Anchor::UNANCHORED, query_params_matches, num_captures))
//        {
//
//        }
        return false;
    }

private:
    re2_st::RE2 url_match_searcher;
    std::vector<String> params_name_extract_from_url;

    String analyzeURLPatten(const String & /*url_patten*/, std::vector<String> & /*matches*/)
    {
        return ".+";
        /// TODO: first we replace all capture group
        /// TODO: second we replace all ${identifier}
    }
};


}
