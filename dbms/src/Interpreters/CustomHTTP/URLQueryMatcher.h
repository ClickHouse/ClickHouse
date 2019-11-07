#pragma once

#include <Core/Types.h>
#include <Interpreters/CustomHTTP/QueryExecutorAndMatcher.h>

#include <re2/re2.h>
#include <re2/stringpiece.h>

#if USE_RE2_ST
#    include <re2_st/re2.h>
#else
#    define re2_st re2
#endif


namespace DB
{

class URLQueryMatcher : public QueryMatcher
{
public:
    URLQueryMatcher(const Poco::Util::AbstractConfiguration & configuration, const String & config_key);

    bool match(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm &) const override;

    bool checkQueryExecutors(const std::vector<QueryExecutorPtr> & custom_query_executors) const override;

private:
    std::unique_ptr<re2_st::RE2> regex_matcher;
};

}
