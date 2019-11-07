#pragma once

#include <Core/Types.h>
#include "QueryExecutorAndMatcher.h"

namespace DB
{

class MethodQueryMatcher : public QueryMatcher
{
public:
    using Config = Poco::Util::AbstractConfiguration;

    MethodQueryMatcher(const Config & configuration, const String & config_key)
        : method(Poco::toLower(configuration.getString(config_key)))
    {
    }

    bool match(Context & /*context*/, Poco::Net::HTTPServerRequest & request, HTMLForm & /*params*/) const override
    {
        return Poco::toLower(request.getMethod()) == method;
    }

    bool checkQueryExecutors(const std::vector<QueryExecutorPtr> & /*check_executors*/) const override { return true; }

private:
    String method;
};

}
