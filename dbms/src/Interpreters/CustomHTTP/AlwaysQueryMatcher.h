#pragma once

#include <Interpreters/CustomHTTP/QueryExecutorAndMatcher.h>

namespace DB
{

class AlwaysQueryMatcher : public QueryMatcher
{
public:
    bool checkQueryExecutors(const std::vector<QueryExecutorPtr> & /*check_executors*/) const override { return true; }

    bool match(Context & /*context*/, Poco::Net::HTTPServerRequest & /*request*/, HTMLForm & /*params*/) const override { return true; }
};

}
